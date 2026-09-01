import { randomUUID } from "node:crypto"
import type { MessageMeta, MessageRequest, MessageType, TransportClientOptions } from "./types"
import { ErrorCode } from "./error-code"
import { MessagingError } from "./errors"
import { getDefaultLogger, NevoLogger } from "./logger"
import { Codec, getCodec, getDefaultCodec } from "./codec"
import { CircuitBreakerRegistry } from "./circuit-breaker"
import { resolveRetryOptions, withRetry, ResolvedRetryOptions } from "./retry"
import {
  resolveCompressionOptions,
  ResolvedCompressionOptions,
  maybeCompress,
  maybeCompressAsync,
  maybeDecompress,
  maybeDecompressAsync,
  shouldDecompressAsync,
  type CompressionEncoding
} from "./compression"
import { DEFAULT_MAX_PAYLOAD_BYTES, enforcePayloadLimit } from "./payload-limit"
import { getDefaultTracer, NevoTracer } from "./tracing"
import { getDefaultMetrics, NEVO_METRIC_NAMES, MetricsRegistry, methodLabel } from "./metrics"
import { GracefulShutdown } from "./graceful-shutdown"
import { LruIdempotencyCache, clientIdempotencyKey } from "./idempotency"
import { formatMethod, DEFAULT_METHOD_VERSION } from "./version"
import { getDevToolsBus, DevToolsBus, publishClientEvent } from "./devtools"
import { uuidv7 } from "./uuid"
import { applyResilience, InvocationBudget, type CompiledResilience } from "./resilience-runtime"
import { resolveOutboundChainId } from "./chain-context"

export interface ClientCallOptions {
  version?: string
  idempotencyKey?: string
  headers?: Record<string, string>
  tenantId?: string
  timeoutMs?: number
}

export interface EncodedRequest {
  envelope: MessageRequest
  payload: Uint8Array
  uuid: string
  method: string
  meta: MessageMeta
  encoding: CompressionEncoding
}

/** The breaker wraps the whole retried operation, so one logical call records one outcome. */
export async function runClientPipeline<T>(
  circuitBreaker: CircuitBreakerRegistry,
  retryOptions: ResolvedRetryOptions,
  key: string,
  attempt: (attempt: number) => Promise<T>,
  resilience?: CompiledResilience
): Promise<T> {
  circuitBreaker.before(key)
  const budget = new InvocationBudget()
  try {
    const result = await withRetry(
      (retryAttempt) =>
        resilience
          ? applyResilience<T>({
              config: resilience,
              ctx: { key },
              invoke: () => attempt(retryAttempt),
              budget
            })
          : budget.run(() => attempt(retryAttempt)),
      retryOptions
    )
    circuitBreaker.onSuccess(key)
    return result
  } catch (err) {
    circuitBreaker.onFailure(key, err)
    throw err
  }
}

export interface ClientRuntimeOptions {
  transport: string
  component?: string
  fallbackCodec?: () => Codec
  /** False when the wire format carries no content-encoding marker; compression is then forced off. */
  compressionCapable?: boolean
}

export type EnvelopeSender = (request: EncodedRequest, attempt: number) => Promise<unknown>

export type EnvelopeDispatcher = (request: EncodedRequest, attempt: number) => Promise<void>

export interface ClientQuerySpec {
  serviceName: string
  method: string
  params: unknown
  opts?: ClientCallOptions
  send: EnvelopeSender
  mapError?: (err: unknown) => unknown
}

export interface ClientEmitSpec {
  serviceName: string
  method: string
  params: unknown
  opts?: ClientCallOptions
  type?: MessageType
  send: EnvelopeDispatcher
  mapError?: (err: unknown) => unknown
}

/** The transport-agnostic half of a client; transports own only the wire call. */
export class ClientRuntime {
  readonly transport: string
  readonly serviceName?: string
  readonly instanceId: string
  readonly authToken?: string
  readonly logger: NevoLogger
  readonly codec: Codec
  readonly circuitBreaker: CircuitBreakerRegistry
  readonly retryOptions: ResolvedRetryOptions
  readonly compression: ResolvedCompressionOptions
  readonly tracer: NevoTracer
  readonly metrics: MetricsRegistry
  readonly shutdown = new GracefulShutdown()
  readonly maxPayloadBytes: number
  readonly idempotencyCache: LruIdempotencyCache<unknown>
  readonly devtoolsBus: DevToolsBus | null
  readonly timeoutMs: number
  readonly debug: boolean
  readonly defaultVersion: string

  private readonly metaStaticPart: Pick<MessageMeta, "service" | "instanceId" | "auth" | "codec">

  constructor(options: TransportClientOptions | undefined, runtime: ClientRuntimeOptions) {
    const opts = options ?? {}
    this.transport = runtime.transport
    this.timeoutMs = (opts["timeoutMs"] as number | undefined) ?? opts.timeout ?? 20000
    this.debug = opts.debug === true
    this.serviceName = opts.serviceName ?? opts.clientId
    this.instanceId = opts.instanceId || randomUUID()
    this.authToken = opts.authToken
    this.logger =
      (opts.logger as NevoLogger) ||
      getDefaultLogger().child({ component: runtime.component ?? `${runtime.transport}-client`, service: this.serviceName })
    this.codec =
      typeof opts.codec === "string"
        ? getCodec(opts.codec)
        : ((opts.codec as Codec | undefined) ?? (runtime.fallbackCodec ? runtime.fallbackCodec() : getDefaultCodec()))
    this.circuitBreaker = new CircuitBreakerRegistry(opts.circuitBreaker)
    this.retryOptions = resolveRetryOptions(opts.retry)
    const compression = resolveCompressionOptions(opts.compression)
    if (compression.enabled && runtime.compressionCapable === false) {
      this.logger.warn(
        { event: "client.compression_unsupported", transport: runtime.transport },
        `The ${runtime.transport} wire format cannot carry a content-encoding marker; compression is disabled for this client.`
      )
      compression.enabled = false
    }

    this.compression = compression
    this.tracer = getDefaultTracer()
    this.metrics = getDefaultMetrics()
    this.maxPayloadBytes = opts.security?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
    this.idempotencyCache = new LruIdempotencyCache<unknown>(opts.idempotency)
    this.devtoolsBus = opts.devtools === false ? null : opts.devtools instanceof Object ? (opts.devtools as DevToolsBus) : getDevToolsBus()
    this.defaultVersion = (opts.defaultVersion as string | undefined) || DEFAULT_METHOD_VERSION
    this.metaStaticPart = Object.freeze({
      service: this.serviceName,
      instanceId: this.instanceId,
      auth: this.authToken ? { token: this.authToken } : undefined,
      codec: this.codec.name
    })
  }

  buildMeta(type: MessageType, opts?: ClientCallOptions): MessageMeta {
    return this.tracer.inject({
      ...this.metaStaticPart,
      type,
      ts: Date.now(),
      version: opts?.version || this.defaultVersion,
      idempotencyKey: opts?.idempotencyKey,
      tenantId: opts?.tenantId,
      headers: opts?.headers,
      nevoChainId: resolveOutboundChainId()
    })
  }

  private versionedMethod(method: string, opts?: ClientCallOptions): string {
    return method.includes("@") ? method : formatMethod(method, opts?.version || this.defaultVersion)
  }

  private assertOutboundSize(raw: Uint8Array): void {
    if (raw.byteLength > this.maxPayloadBytes) {
      throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, {
        message: `Payload size ${raw.byteLength}B exceeds ${this.maxPayloadBytes}B`,
        size: raw.byteLength,
        limit: this.maxPayloadBytes
      })
    }
  }

  private observeOutbound(byteLength: number): void {
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "out", service: this.serviceName ?? "unknown" }, byteLength)
  }

  encodeSync(method: string, params: unknown, type: MessageType, opts?: ClientCallOptions & { uuid?: string }): EncodedRequest {
    const uuid = opts?.uuid ?? uuidv7()
    const meta = this.buildMeta(type, opts)
    const versioned = this.versionedMethod(method, opts)
    const envelope: MessageRequest = { uuid, method: versioned, params, meta }
    const raw = this.codec.encode(envelope)
    this.assertOutboundSize(raw)
    const compressed = maybeCompress(raw, this.compression)
    meta.contentEncoding = compressed.encoding
    this.observeOutbound(compressed.data.byteLength)
    return { envelope, payload: compressed.data, uuid, method: versioned, meta, encoding: compressed.encoding }
  }

  async encodeAsync(method: string, params: unknown, type: MessageType, opts?: ClientCallOptions & { uuid?: string }): Promise<EncodedRequest> {
    const uuid = opts?.uuid ?? uuidv7()
    const meta = this.buildMeta(type, opts)
    const versioned = this.versionedMethod(method, opts)
    const envelope: MessageRequest = { uuid, method: versioned, params, meta }
    const raw = this.codec.encode(envelope)
    this.assertOutboundSize(raw)
    const compressed = await maybeCompressAsync(raw, this.compression)
    meta.contentEncoding = compressed.encoding
    this.observeOutbound(compressed.data.byteLength)
    return { envelope, payload: compressed.data, uuid, method: versioned, meta, encoding: compressed.encoding }
  }

  encode(method: string, params: unknown, type: MessageType, opts?: ClientCallOptions & { uuid?: string }): EncodedRequest | Promise<EncodedRequest> {
    if (this.compression.async && this.compression.enabled) return this.encodeAsync(method, params, type, opts)
    return this.encodeSync(method, params, type, opts)
  }

  decode<T = unknown>(data: Uint8Array, encoding?: string): T | Promise<T> {
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "in", service: this.serviceName ?? "unknown" }, data.byteLength)
    if (!shouldDecompressAsync(data.byteLength, encoding)) {
      const decompressed = maybeDecompress(data, encoding, this.maxPayloadBytes)
      enforcePayloadLimit(decompressed, this.maxPayloadBytes)
      return this.codec.decode<T>(decompressed)
    }
    return this.decodeAsync<T>(data, encoding)
  }

  async decodeAsync<T = unknown>(data: Uint8Array, encoding?: string): Promise<T> {
    const decompressed = await maybeDecompressAsync(data, encoding, this.maxPayloadBytes)
    enforcePayloadLimit(decompressed, this.maxPayloadBytes)
    return this.codec.decode<T>(decompressed)
  }

  unwrap<T>(envelope: unknown, serviceName: string): T {
    const params = (envelope as { params?: { result?: unknown; error?: any } } | undefined)?.params
    if (params?.result === "error" && params.error) {
      const err = params.error
      throw new MessagingError(err.code, { message: err.message, ...(err.details ?? {}) }, err.service || serviceName)
    }
    return params?.result as T
  }

  breakerKey(serviceName: string, method: string): string {
    return `${serviceName}:${method}`
  }

  private metricLabels(serviceName: string, method: string): Record<string, string> {
    return { transport: this.transport, service: serviceName, method: methodLabel(method), role: "client" }
  }

  async query<T>(spec: ClientQuerySpec): Promise<T> {
    const { serviceName, method, params, opts, send, mapError } = spec
    const cacheKey = opts?.idempotencyKey ? clientIdempotencyKey(serviceName, method, opts.idempotencyKey) : undefined
    if (cacheKey && this.idempotencyCache.isEnabled() && this.idempotencyCache.has(cacheKey)) {
      return this.idempotencyCache.get(cacheKey) as T
    }

    const retryIdemKey = opts?.idempotencyKey ?? uuidv7()

    const result = await this.shutdown.trackInflight(
      runClientPipeline<T>(this.circuitBreaker, this.retryOptions, this.breakerKey(serviceName, method), async (attempt) => {
        const startMs = Date.now()
        let request: EncodedRequest | undefined
        const span = this.tracer.startSpan(`nevo.client.query ${serviceName}.${method}`, {
          "nevo.method": method,
          "nevo.service": serviceName,
          "nevo.codec": this.codec.name,
          "nevo.attempt": attempt
        })
        try {
          request = await this.encode(method, params, "query", {
            ...opts,
            idempotencyKey: retryIdemKey,
            headers: { ...(opts?.headers ?? {}), "nevo-attempt": String(attempt) }
          })
          const envelope = await send(request, attempt)
          const value = this.unwrap<T>(envelope, serviceName)
          span.setStatus({ code: 1 })
          publishClientEvent(this.devtoolsBus, {
            service: serviceName,
            method,
            uuid: request.uuid,
            chainId: request.meta.nevoChainId,
            durationMs: Date.now() - startMs,
            status: "ok",
            transport: this.transport,
            origin: this.serviceName
          })
          return value
        } catch (raw) {
          const err = mapError ? mapError(raw) : raw
          span.recordException(err)
          span.setStatus({ code: 2, message: (err as Error)?.message })
          publishClientEvent(this.devtoolsBus, {
            service: serviceName,
            method,
            uuid: request?.uuid,
            chainId: request?.meta.nevoChainId,
            durationMs: Date.now() - startMs,
            status: "error",
            transport: this.transport,
            origin: this.serviceName,
            error: {
              code: err instanceof MessagingError ? err.code : (err as { code?: number })?.code,
              message: (err as Error)?.message ?? String(err)
            }
          })
          throw err
        } finally {
          span.end()
          this.metrics.incCounter(NEVO_METRIC_NAMES.requestsTotal, this.metricLabels(serviceName, method))
          if (attempt > 1) {
            this.metrics.incCounter(NEVO_METRIC_NAMES.retries, {
              transport: this.transport,
              service: serviceName,
              method: methodLabel(method)
            })
          }
        }
      })
    )

    if (cacheKey && this.idempotencyCache.isEnabled()) this.idempotencyCache.set(cacheKey, result)
    return result
  }

  async emit(spec: ClientEmitSpec): Promise<void> {
    const { serviceName, method, params, opts, send, mapError } = spec
    const idemKey = opts?.idempotencyKey ?? uuidv7()
    return this.shutdown.trackInflight(
      runClientPipeline<void>(this.circuitBreaker, this.retryOptions, this.breakerKey(serviceName, method), async (attempt) => {
        try {
          const request = await this.encode(method, params, spec.type ?? "emit", { ...opts, idempotencyKey: idemKey })
          await send(request, attempt)
        } catch (raw) {
          throw mapError ? mapError(raw) : raw
        }
      })
    )
  }

  async close(timeoutMs = 30_000): Promise<void> {
    await this.shutdown.shutdown(timeoutMs)
  }
}
