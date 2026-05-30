import { setTimeout as sleep } from "node:timers/promises"

const FNV_OFFSET = 0xcbf29ce484222325n
const FNV_PRIME = 0x100000001b3n
const MASK_64 = (1n << 64n) - 1n
function fnv1aHash64(str: string): string {
  let h = FNV_OFFSET
  for (let i = 0; i < str.length; i++) {
    h ^= BigInt(str.charCodeAt(i))
    h = (h * FNV_PRIME) & MASK_64
  }
  return h.toString(36)
}
import { Type, Inject } from "@nestjs/common"
import type { NatsConnection, Subscription } from "@nats-io/nats-core"
import { createSignalRouterDecorator, SignalRouterOptions } from "../../signal-router.utils"
import {
  Codec,
  getCodec,
  getDefaultCodec,
  maybeCompress,
  maybeDecompress,
  maybeDecompressAsync,
  shouldDecompressAsync,
  enforcePayloadLimit,
  DEFAULT_MAX_PAYLOAD_BYTES,
  resolveCompressionOptions,
  getDefaultLogger,
  DlqRouter,
  LruIdempotencyCache
} from "../../common"
import { getNatsModule } from "../optional-deps"
import { NevoNatsClient } from "./nevo-nats.client"

export interface NatsSignalRouterOptions extends SignalRouterOptions {
  servers?: string[]
  reuseClient?: boolean
  clientToken?: string | symbol
  codec?: Codec | string
  compression?: { enabled?: boolean; algorithm?: "gzip" | "deflate" | "zstd"; threshold?: number; level?: number; async?: boolean }
  responseCache?: { enabled?: boolean; maxEntries?: number; ttlMs?: number }
  reconnect?: {
    enabled?: boolean
    maxAttempts?: number
    timeWaitMs?: number
    jitterMs?: number
    jitterTlsMs?: number
    waitOnFirstConnect?: boolean
    lazyConnect?: boolean
  }
}

const DEFAULT_NATS_CLIENT_TOKEN = "NEVO_NATS_CLIENT"

export function NatsSignalRouter(serviceType: Type<any> | Type<any>[], options?: NatsSignalRouterOptions) {
  const codec: Codec = typeof options?.codec === "string" ? getCodec(options.codec) : (options?.codec as Codec) || getDefaultCodec()
  const compression = resolveCompressionOptions(options?.compression)
  const maxPayloadBytes = options?.security?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
  const logger = options?.logger || getDefaultLogger().child({ component: "nats-router" })
  const dlq = options?.dlq instanceof DlqRouter ? options.dlq : new DlqRouter({ enabled: (options?.dlq as any)?.enabled === true })

  const responseCacheEnabled = options?.responseCache?.enabled === true && compression.enabled
  const responseCache = responseCacheEnabled
    ? new LruIdempotencyCache<{ data: Uint8Array; encoding: string }>({
        enabled: true,
        maxEntries: options?.responseCache?.maxEntries ?? 1024,
        ttlMs: options?.responseCache?.ttlMs ?? 60_000
      })
    : null

  function hashResult(value: unknown): string {
    const json = typeof value === "string" ? value : JSON.stringify(value, (_k, v) => typeof v === "bigint" ? v.toString() + "n" : v)
    return fnv1aHash64(json)
  }

  return createSignalRouterDecorator(
    serviceType,
    { ...options, dlq, logger },
    (data) => {
      const messageData: any = data
      return {
        method: messageData.method,
        params: messageData.params,
        uuid: messageData.uuid,
        meta: messageData.meta
      }
    },
    (target, eventPattern, handlerName) => {
      target.prototype.natsConnection = null
      target.prototype.natsSubscription = null
      target.prototype.__natsRouterOwnsConnection = false

      const reuseClient = options?.reuseClient !== false
      const clientToken: any = options?.clientToken || DEFAULT_NATS_CLIENT_TOKEN

      if (reuseClient) {
        const existing = (Reflect.getMetadata("self:paramtypes", target) as any[]) || []
        const idx = (Reflect.getMetadata("design:paramtypes", target) as any[])?.length || 0
        existing.push({ index: idx, param: clientToken })
        Reflect.defineMetadata("self:paramtypes", existing, target)

        const originalCtor = target as any
        const wrapped: any = function (...args: any[]) {
          const inst = new originalCtor(...args.slice(0, args.length - 1))
          inst.__nevoNatsUniversalClient = args[args.length - 1]
          return inst
        }
        wrapped.prototype = originalCtor.prototype
        Reflect.defineMetadata("design:paramtypes", [
          ...((Reflect.getMetadata("design:paramtypes", originalCtor) as any[]) || []),
          NevoNatsClient
        ], originalCtor)
        Inject(clientToken)(originalCtor, undefined as any, idx)
      }

      const originalOnModuleInit = target.prototype.onModuleInit || function () {}
      target.prototype.onModuleInit = async function () {
        await originalOnModuleInit.call(this)

        let nc: NatsConnection | null = null
        let ownsConnection = false

        const universal: NevoNatsClient | undefined = (this as any).__nevoNatsUniversalClient
        if (universal && reuseClient) {
          nc = await universal.ensureConnection()
        }

        if (!nc) {
          const { connect } = getNatsModule()
          const servers = options?.servers && options.servers.length > 0 ? options.servers : ["nats://127.0.0.1:4222"]
          const reconnectEnabled = options?.reconnect?.enabled !== false
          const maxAttempts = options?.reconnect?.maxAttempts ?? -1
          const timeWaitMs = options?.reconnect?.timeWaitMs ?? 5000
          const lazyConnect = options?.reconnect?.lazyConnect === true
          const waitOnFirstConnect = options?.reconnect?.waitOnFirstConnect ?? !lazyConnect
          nc = await connect({
            servers,
            maxReconnectAttempts: reconnectEnabled ? maxAttempts : 0,
            reconnectTimeWait: timeWaitMs,
            reconnectJitter: options?.reconnect?.jitterMs,
            reconnectJitterTLS: options?.reconnect?.jitterTlsMs,
            waitOnFirstConnect
          })
          ownsConnection = true
        }

        this.natsConnection = nc
        this.__natsRouterOwnsConnection = ownsConnection

        const ctx = this
        const subscribeAndPump = async () => {
          const sub: Subscription = nc!.subscribe(eventPattern)
          ctx.natsSubscription = sub
          for await (const msg of sub) {
            try {
              const encoding = msg.headers?.get?.("content-encoding")
              const raw = shouldDecompressAsync(msg.data.byteLength, encoding)
                ? await maybeDecompressAsync(msg.data, encoding, maxPayloadBytes)
                : maybeDecompress(msg.data, encoding, maxPayloadBytes)
              enforcePayloadLimit(raw, maxPayloadBytes)
              const payload = codec.decode(raw)
              const result = await ctx[handlerName](payload)
              if (msg.reply && result) {
                let outData: Uint8Array
                let outEncoding: string
                if (responseCache) {
                  const cacheKey = hashResult((result as any)?.params?.result ?? result)
                  const cached = responseCache.get(cacheKey)
                  if (cached) {
                    outData = cached.data
                    outEncoding = cached.encoding
                  } else {
                    const outBuf = codec.encode(result)
                    const out = maybeCompress(outBuf, compression)
                    outData = out.data
                    outEncoding = out.encoding
                    responseCache.set(cacheKey, { data: outData, encoding: outEncoding })
                  }
                } else {
                  const outBuf = codec.encode(result)
                  const out = maybeCompress(outBuf, compression)
                  outData = out.data
                  outEncoding = out.encoding
                }
                const replyHeaders = ctx.__buildReplyHeaders ? ctx.__buildReplyHeaders(outEncoding) : undefined
                nc!.publish(msg.reply, outData, replyHeaders ? { headers: replyHeaders } : undefined)
              }
            } catch (err) {
              logger.error({ event: "nats.router.handler_error", err: (err as Error)?.message })
              await dlq.route({
                topic: eventPattern,
                reason: "handler-error",
                error: { message: (err as Error)?.message ?? String(err) },
                rawPayload: undefined,
                ts: Date.now()
              })
            }
          }
        }

        ;(async () => {
          while (nc && (nc as any).isClosed?.() !== true) {
            try {
              await subscribeAndPump()
              break
            } catch (err) {
              logger.warn({ event: "nats.router.subscription_loop_error", err: (err as Error)?.message }, "Re-subscribing after error")
              await sleep(1000)
            }
          }
        })()
      }

      const originalOnModuleDestroy = target.prototype.onModuleDestroy || function () {}
      target.prototype.onModuleDestroy = async function () {
        await originalOnModuleDestroy.call(this)
        if (this.natsSubscription) {
          try { this.natsSubscription.unsubscribe() } catch {}
        }
        if (this.natsConnection && this.__natsRouterOwnsConnection) {
          try { await this.natsConnection.close() } catch {}
        }
      }
    }
  )
}
