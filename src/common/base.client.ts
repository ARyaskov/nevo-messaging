import { randomUUID } from "node:crypto"
import {
  MessageRequest,
  MicroserviceConfig,
  TransportClientOptions,
  MessageType,
  Subscription,
  SubscriptionOptions,
  SubscriptionContext,
  MessageMeta
} from "./types"
import { ErrorCode } from "./error-code"
import { MessagingError } from "./errors"
import { getDefaultLogger, NevoLogger } from "./logger"
import { Codec, getCodec, getDefaultCodec } from "./codec"
import { CircuitBreakerRegistry } from "./circuit-breaker"
import { resolveRetryOptions, withRetry, ResolvedRetryOptions } from "./retry"
import { resolveCompressionOptions, ResolvedCompressionOptions } from "./compression"
import { DEFAULT_MAX_PAYLOAD_BYTES } from "./payload-limit"
import { getDefaultTracer, NevoTracer } from "./tracing"
import { getDefaultMetrics, NEVO_METRIC_NAMES, MetricsRegistry, methodLabel } from "./metrics"
import { GracefulShutdown } from "./graceful-shutdown"
import { LruIdempotencyCache } from "./idempotency"
import { formatMethod, DEFAULT_METHOD_VERSION } from "./version"
import { RateLimiter, resolveRateLimiter } from "./rate-limit"
import { getDevToolsBus, DevToolsBus } from "./devtools"
import { uuidv7 } from "./uuid"
import { applyResilience, type CompiledResilience } from "./resilience-runtime"

export interface PreparedRequest {
  uuid: string
  method: string
  meta: MessageMeta
  request: MessageRequest
}

/**
 * Shared client resilience pipeline — the single source of truth for the
 * ordering "circuit breaker wraps the **entire** retried operation (plus any
 * optional decorator resilience)". Recording exactly one breaker outcome per
 * logical call (instead of one per retry attempt) is what stops N retries from
 * tripping the breaker N× too early.
 *
 * Transport clients that do not (yet) extend {@link BaseMessagingClient} — e.g.
 * `NevoNatsClient` — call this directly, so there is exactly one implementation
 * of the ordering across the codebase rather than divergent per-transport
 * copies. See the note on {@link BaseMessagingClient}.
 */
export async function runClientPipeline<T>(
  circuitBreaker: CircuitBreakerRegistry,
  retryOptions: ResolvedRetryOptions,
  key: string,
  attempt: (attempt: number) => Promise<T>,
  resilience?: CompiledResilience
): Promise<T> {
  circuitBreaker.before(key)
  try {
    const inner = (): Promise<T> => withRetry(attempt, retryOptions)
    const result = resilience
      ? await applyResilience<T>({ config: resilience, ctx: { key }, invoke: () => inner() })
      : await inner()
    circuitBreaker.onSuccess(key)
    return result
  } catch (err) {
    circuitBreaker.onFailure(key, err)
    throw err
  }
}

/**
 * Reference base for transport clients. Its constructor wires the shared
 * primitives (codec, circuit breaker, retry, metrics, idempotency, …) and
 * {@link withClientPipeline} is the canonical request path.
 *
 */
export abstract class BaseMessagingClient {
  protected readonly options: TransportClientOptions
  protected readonly microservices: Map<string, string> = new Map()
  protected readonly serviceName: string
  protected readonly instanceId: string
  protected readonly authToken?: string
  private _logger: NevoLogger | null = null
  private _loggerOverride: NevoLogger | null = null
  protected get logger(): NevoLogger {
    if (this._logger) return this._logger
    this._logger = this._loggerOverride ?? getDefaultLogger().child({ component: "client", service: this.serviceName })
    return this._logger
  }
  protected readonly codec: Codec
  protected readonly circuitBreaker: CircuitBreakerRegistry
  protected readonly retryOptions: ResolvedRetryOptions
  protected readonly compressionOptions: ResolvedCompressionOptions
  protected readonly tracer: NevoTracer
  protected readonly metrics: MetricsRegistry
  protected readonly shutdown = new GracefulShutdown()
  protected readonly idempotencyResults: LruIdempotencyCache<unknown>
  protected readonly timeoutMs: number
  protected readonly debug: boolean
  protected readonly maxPayloadBytes: number
  protected readonly defaultVersion: string
  protected readonly rateLimiter: RateLimiter
  protected readonly devtoolsBus: DevToolsBus | null

  protected constructor(options?: TransportClientOptions) {
    this.options = { timeout: 20000, debug: false, ...options }
    this.serviceName = this.options.serviceName || this.options.clientId || "nevo"
    this.instanceId = this.options.instanceId || randomUUID()
    this.authToken = this.options.authToken
    this._loggerOverride = (this.options.logger as NevoLogger) ?? null
    this.codec = typeof this.options.codec === "string" ? getCodec(this.options.codec) : (this.options.codec as Codec | undefined) || getDefaultCodec()
    this.circuitBreaker = new CircuitBreakerRegistry(this.options.circuitBreaker)
    this.retryOptions = resolveRetryOptions(this.options.retry)
    this.compressionOptions = resolveCompressionOptions(this.options.compression)
    this.tracer = getDefaultTracer()
    this.metrics = getDefaultMetrics()
    this.idempotencyResults = new LruIdempotencyCache<unknown>({ enabled: this.options.idempotency?.enabled === true, maxEntries: this.options.idempotency?.maxEntries, ttlMs: this.options.idempotency?.ttlMs })
    this.timeoutMs = this.options.timeout ?? 20000
    this.debug = this.options.debug ?? false
    this.maxPayloadBytes = this.options.security?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
    this.defaultVersion = (this.options.defaultVersion as string) || DEFAULT_METHOD_VERSION
    this.rateLimiter = this.options.rateLimit !== undefined ? resolveRateLimiter(this.options.rateLimit) : new RateLimiter()
    this.devtoolsBus = this.options.devtools === false ? null : (this.options.devtools instanceof Object ? (this.options.devtools as DevToolsBus) : getDevToolsBus())
  }

  getServiceName(): string { return this.serviceName }
  getInstanceId(): string { return this.instanceId }

  protected registerMicroservices(configs: MicroserviceConfig[]): void {
    for (const config of configs) {
      this.microservices.set(config.serviceName, config.clientName)
    }
  }

  protected formatVersionedMethod(method: string, explicitVersion?: string): string {
    if (method.includes("@")) return method
    if (!explicitVersion && this.defaultVersion === DEFAULT_METHOD_VERSION) return method
    return formatMethod(method, explicitVersion || this.defaultVersion)
  }

  protected buildRequest(method: string, params: unknown, type: MessageType, opts?: { idempotencyKey?: string; version?: string; headers?: Record<string, string>; tenantId?: string }): PreparedRequest {
    const uuid = uuidv7()
    const versioned = this.formatVersionedMethod(method, opts?.version)
    const baseMeta: MessageMeta = {
      type,
      service: this.serviceName,
      instanceId: this.instanceId,
      ts: Date.now(),
      version: opts?.version || this.defaultVersion,
      idempotencyKey: opts?.idempotencyKey,
      tenantId: opts?.tenantId,
      headers: opts?.headers,
      auth: this.authToken ? { token: this.authToken } : undefined
    }
    const meta = this.tracer.inject(baseMeta)
    const request: MessageRequest = { uuid, method: versioned, params, meta }
    return { uuid, method: versioned, meta, request }
  }

  protected encodeMessage(value: unknown): Uint8Array {
    const buf = this.codec.encode(value)
    if (buf.byteLength > this.maxPayloadBytes) {
      throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Payload size ${buf.byteLength}B exceeds ${this.maxPayloadBytes}B`, size: buf.byteLength, limit: this.maxPayloadBytes })
    }
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "out", service: this.serviceName }, buf.byteLength)
    return buf
  }

  protected decodeMessage<T = unknown>(data: Uint8Array | string): T {
    if (typeof data !== "string") {
      this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "in", service: this.serviceName }, data.byteLength)
    }
    return this.codec.decode<T>(data)
  }

  protected async withClientPipeline<T>(
    serviceName: string,
    method: string,
    fn: () => Promise<T>,
    resilience?: CompiledResilience
  ): Promise<T> {
    const key = `${serviceName}:${method}`
    // Version-stripped label so `foo@v1`/`foo@v2` don't split into separate series.
    const methodName = methodLabel(method)
    this.metrics.setGauge(NEVO_METRIC_NAMES.inflight, { service: serviceName, method: methodName }, 1)
    try {
      return await runClientPipeline<T>(
        this.circuitBreaker,
        this.retryOptions,
        key,
        async (attempt) => {
          if (attempt > 1) this.metrics.incCounter(NEVO_METRIC_NAMES.retries, { service: serviceName, method: methodName })
          return fn()
        },
        resilience
      )
    } finally {
      this.metrics.setGauge(NEVO_METRIC_NAMES.inflight, { service: serviceName, method: methodName }, 0)
    }
  }

  async close(): Promise<void> {
    await this.shutdown.shutdown()
  }

  protected publishClientDevToolsEvent(eventInput: {
    service: string
    method: string
    uuid?: string
    durationMs: number
    status: "ok" | "error"
    error?: { code?: number; message?: string }
  }): void {
    if (!this.devtoolsBus) return
    try {
      this.devtoolsBus.publish({
        ts: Date.now(),
        type: eventInput.status === "ok" ? "request" : "error",
        service: eventInput.service,
        method: eventInput.method,
        uuid: eventInput.uuid,
        durationMs: eventInput.durationMs,
        status: eventInput.status,
        error: eventInput.error,
        extra: { role: "client", origin: this.serviceName }
      })
    } catch {}
  }

  protected async query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string> }): Promise<T> {
    const clientName = this.microservices.get(serviceName)
    if (!clientName) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: `Microservice ${serviceName} is not registered`, serviceName })
    }
    return this.shutdown.trackInflight(this._queryMicroservice<T>(clientName, method, params, opts))
  }

  protected async emit(serviceName: string, method: string, params: unknown, opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string> }): Promise<void> {
    const clientName = this.microservices.get(serviceName)
    if (!clientName) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: `Microservice ${serviceName} is not registered`, serviceName })
    }
    return this.shutdown.trackInflight(this._emitToMicroservice(clientName, method, params, opts))
  }

  protected abstract _queryMicroservice<T>(clientName: string, method: string, params: unknown, opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string> }): Promise<T>
  protected abstract _emitToMicroservice(clientName: string, method: string, params: unknown, opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string> }): Promise<void>
  protected abstract _publishToMicroservice(clientName: string, method: string, params: unknown, opts?: { version?: string; headers?: Record<string, string> }): Promise<void>
  protected abstract _broadcast(method: string, params: unknown, opts?: { version?: string; headers?: Record<string, string> }): Promise<void>
  protected abstract _subscribeToMicroservice<T>(
    clientName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: (data: T, context: SubscriptionContext) => Promise<void> | void
  ): Promise<Subscription>
}
