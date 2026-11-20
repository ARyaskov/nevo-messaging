import { setTimeout as sleep } from "node:timers/promises"

import { Type, Inject, Optional } from "@nestjs/common"
import type { Msg, NatsConnection, Subscription } from "@nats-io/nats-core"
import { createSignalRouterDecorator, SignalRouterOptions, getRouterRuntime } from "../../signal-router.utils"
import {
  Codec,
  getCodec,
  getDefaultCodec,
  maybeCompress,
  maybeCompressAsync,
  maybeDecompress,
  maybeDecompressAsync,
  shouldDecompressAsync,
  enforcePayloadLimit,
  resolveCompressionOptions
} from "../../common"
import { getNatsModule } from "../optional-deps"
import { NATS_SCATTER_SUFFIX, NevoNatsClient } from "./nevo-nats.client"

export interface NatsSignalRouterOptions extends SignalRouterOptions {
  servers?: string[]
  reuseClient?: boolean
  clientToken?: string | symbol
  /** Core NATS queue group for commands. Defaults to the request subject. */
  queueGroup?: string
  /** Maximum active handlers per subscription. Default 1 preserves delivery order. */
  concurrency?: number
  codec?: Codec | string
  compression?: { enabled?: boolean; algorithm?: "gzip" | "deflate" | "zstd"; threshold?: number; level?: number; async?: boolean }
  /** @deprecated Replies are encoded per request. This option has no effect. */
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
  const concurrency = options?.concurrency ?? 1
  if (!Number.isSafeInteger(concurrency) || concurrency < 1) throw new RangeError("NATS router concurrency must be a positive integer")

  return createSignalRouterDecorator(
    serviceType,
    options ?? {},
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
      target.prototype.__natsRouterOwnsConnection = false

      const reuseClient = options?.reuseClient !== false
      const clientToken: any = options?.clientToken || DEFAULT_NATS_CLIENT_TOKEN

      if (reuseClient) {
        Optional()(target.prototype, "__nevoNatsUniversalClient")
        Inject(clientToken)(target.prototype, "__nevoNatsUniversalClient")
      }

      target.prototype.__buildReplyHeaders = function (encoding: string) {
        if (!encoding || encoding === "identity") return undefined
        try {
          const { headers: createHeaders } = getNatsModule() as any
          if (typeof createHeaders === "function") {
            const h = createHeaders()
            h.set("content-encoding", encoding)
            return h
          }
        } catch {}
        return undefined
      }

      const originalOnModuleInit = target.prototype.onModuleInit || function () {}
      target.prototype.onModuleInit = async function () {
        await originalOnModuleInit.call(this)

        const runtime = getRouterRuntime(this.constructor)
        const logger = runtime.logger
        const dlq = runtime.dlq
        const maxPayloadBytes = runtime.maxPayloadBytes

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
        this.__natsRouterStopped = false
        const subscriptions = new Set<Subscription>()
        this.__natsRouterSubscriptions = subscriptions

        const handleMessage = async (msg: Msg) => {
          try {
            const encoding = msg.headers?.get?.("content-encoding")
            const raw = shouldDecompressAsync(msg.data.byteLength, encoding)
              ? await maybeDecompressAsync(msg.data, encoding, maxPayloadBytes)
              : maybeDecompress(msg.data, encoding, maxPayloadBytes)
            enforcePayloadLimit(raw, maxPayloadBytes)
            const payload = codec.decode(raw)
            const result = await this[handlerName](payload)
            if (msg.reply && result) {
              const outBuf = codec.encode(result)
              const out = compression.async ? await maybeCompressAsync(outBuf, compression) : maybeCompress(outBuf, compression)
              const replyHeaders = this.__buildReplyHeaders(out.encoding)
              nc!.publish(msg.reply, out.data, replyHeaders ? { headers: replyHeaders } : undefined)
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

        const subscribeAndPump = async (subject: string, queue?: string) => {
          const sub = nc!.subscribe(subject, queue === undefined ? undefined : { queue })
          subscriptions.add(sub)
          const active = new Set<Promise<void>>()
          try {
            for await (const msg of sub) {
              if (concurrency === 1) {
                await handleMessage(msg)
                continue
              }
              const work = handleMessage(msg)
                .catch((err) => logger.error({ event: "nats.router.worker_failed", err: (err as Error)?.message }))
                .finally(() => active.delete(work))
              active.add(work)
              if (active.size === concurrency) await Promise.race(active)
            }
          } finally {
            if (subscriptions.delete(sub)) sub.unsubscribe()
            await Promise.allSettled(active)
          }
        }

        const start = async (subject: string, queue?: string) => {
          while (!this.__natsRouterStopped && nc && nc.isClosed?.() !== true) {
            try {
              await subscribeAndPump(subject, queue)
              break
            } catch (err) {
              logger.warn({ event: "nats.router.subscription_loop_error", err: (err as Error)?.message }, "Re-subscribing after error")
              await sleep(1000)
            }
          }
        }
        const reportPumpError = (err: unknown) => logger.error({ event: "nats.router.pump_crashed", err: (err as Error)?.message })
        void start(eventPattern, options?.queueGroup ?? eventPattern).catch(reportPumpError)
        void start(`${eventPattern}${NATS_SCATTER_SUFFIX}`).catch(reportPumpError)
      }

      const originalOnModuleDestroy = target.prototype.onModuleDestroy || function () {}
      target.prototype.onModuleDestroy = async function () {
        this.__natsRouterStopped = true
        await originalOnModuleDestroy.call(this)
        for (const subscription of this.__natsRouterSubscriptions ?? []) {
          try {
            this.__natsRouterSubscriptions.delete(subscription)
            subscription.unsubscribe()
          } catch {}
        }
        if (this.natsConnection && this.__natsRouterOwnsConnection) {
          try {
            await this.natsConnection.close()
          } catch {}
        }
      }
    }
  )
}
