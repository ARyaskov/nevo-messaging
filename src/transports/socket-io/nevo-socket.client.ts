import type { Socket } from "socket.io-client"
import { randomUUID } from "node:crypto"
import { uuidv7 } from "../../common/uuid"
import {
  DEFAULT_BROADCAST_TOPIC,
  DEFAULT_DISCOVERY_TOPIC,
  DEFAULT_SUBSCRIPTION_SUFFIX,
  DiscoveryRegistry,
  DiscoveryAnnouncement,
  MessageMeta,
  MessageType,
  MessagingError,
  TimeoutError,
  ErrorCode,
  Subscription,
  SubscriptionContext,
  SubscriptionOptions,
  Codec,
  getCodec,
  getDefaultCodec,
  NevoLogger,
  getDefaultLogger,
  CircuitBreakerRegistry,
  ResolvedRetryOptions,
  ResolvedCompressionOptions,
  resolveRetryOptions,
  withRetry,
  resolveCompressionOptions,
  DEFAULT_MAX_PAYLOAD_BYTES,
  getDefaultTracer,
  NevoTracer,
  getDefaultMetrics,
  NEVO_METRIC_NAMES,
  methodLabel,
  MetricsRegistry,
  GracefulShutdown,
  LruIdempotencyCache,
  TransportClientOptions,
  matchesFilter,
  DEFAULT_METHOD_VERSION,
  formatMethod,
  normalizeServiceName,
  resolveOutboundChainId
} from "../../common"
import { getSocketIoClientModule } from "../optional-deps"

export interface NevoSocketClientOptions extends TransportClientOptions {
  timeoutMs?: number
}

export class NevoSocketClient {
  private readonly serviceUrls: Map<string, string>
  private readonly timeoutMs: number
  private readonly debug: boolean
  private readonly serviceName?: string
  private readonly instanceId: string
  private readonly authToken?: string
  private readonly logger: NevoLogger
  private readonly codec: Codec
  private readonly circuitBreaker: CircuitBreakerRegistry
  private readonly retryOptions: ResolvedRetryOptions
  private readonly compression: ResolvedCompressionOptions
  private readonly tracer: NevoTracer
  private readonly metrics: MetricsRegistry
  private readonly shutdown = new GracefulShutdown()
  private readonly maxPayloadBytes: number
  private readonly idempotencyCache: LruIdempotencyCache<unknown>
  private readonly sockets = new Map<string, Socket>()
  private readonly discoveryRegistry = new DiscoveryRegistry()
  private readonly discoveryEnabled: boolean
  private readonly discoveryHeartbeatIntervalMs: number
  private readonly discoveryTtlMs: number
  private readonly metaStaticPart: Pick<MessageMeta, "service" | "instanceId" | "auth" | "codec">

  constructor(serviceUrls: Record<string, string>, options?: NevoSocketClientOptions) {
    this.serviceUrls = new Map(Object.entries(serviceUrls).map(([k, v]) => [k.toLowerCase(), v]))
    this.timeoutMs = options?.timeoutMs ?? options?.timeout ?? 20000
    this.debug = options?.debug || false
    this.serviceName = options?.serviceName
    this.instanceId = options?.instanceId || randomUUID()
    this.authToken = options?.authToken
    this.logger = (options?.logger as NevoLogger) || getDefaultLogger().child({ component: "socket-client", service: this.serviceName })
    this.codec = typeof options?.codec === "string" ? getCodec(options.codec) : (options?.codec as Codec) || getDefaultCodec()
    this.circuitBreaker = new CircuitBreakerRegistry(options?.circuitBreaker)
    this.retryOptions = resolveRetryOptions(options?.retry)
    this.compression = resolveCompressionOptions(options?.compression)
    this.tracer = getDefaultTracer()
    this.metrics = getDefaultMetrics()
    this.maxPayloadBytes = options?.security?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
    this.idempotencyCache = new LruIdempotencyCache<unknown>(options?.idempotency)
    this.discoveryEnabled = options?.discovery?.enabled === true
    this.discoveryHeartbeatIntervalMs = options?.discovery?.heartbeatIntervalMs || 10000
    this.discoveryTtlMs = options?.discovery?.ttlMs || 30000
    if (this.discoveryEnabled) this.discoveryRegistry.startBackgroundPrune(this.discoveryTtlMs)
    this.metaStaticPart = Object.freeze({
      service: this.serviceName,
      instanceId: this.instanceId,
      auth: this.authToken ? { token: this.authToken } : undefined,
      codec: this.codec.name
    })
  }

  getInstanceId(): string { return this.instanceId }

  private buildMeta(type: MessageType, opts?: any): MessageMeta {
    const baseMeta: MessageMeta = {
      ...this.metaStaticPart,
      type,
      ts: Date.now(),
      version: opts?.version || DEFAULT_METHOD_VERSION,
      idempotencyKey: opts?.idempotencyKey,
      tenantId: opts?.tenantId,
      headers: opts?.headers,
      // Stamp chain id from ALS (or mint a new one at the entry of a chain).
      nevoChainId: resolveOutboundChainId()
    }
    return this.tracer.inject(baseMeta)
  }

  private buildEnvelope(method: string, params: unknown, type: MessageType, opts?: any) {
    const uuid = uuidv7()
    const meta = this.buildMeta(type, opts)
    const versioned = method.includes("@") ? method : formatMethod(method, opts?.version || DEFAULT_METHOD_VERSION)
    const env = { uuid, method: versioned, params, meta }
    const encoded = this.codec.encode(env)
    if (encoded.byteLength > this.maxPayloadBytes) {
      throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Payload size ${encoded.byteLength}B exceeds ${this.maxPayloadBytes}B` })
    }
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "out", service: this.serviceName ?? "unknown" }, encoded.byteLength)
    return { env, uuid, meta, encoded }
  }

  private getSocket(serviceName: string): Socket {
    const normalized = normalizeServiceName(serviceName)
    const url = this.serviceUrls.get(normalized)
    if (!url) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: `Service "${serviceName}" is not registered`, availableServices: this.serviceUrls.keys().toArray() })
    }
    let socket = this.sockets.get(normalized)
    if (!socket) {
      const { io } = getSocketIoClientModule()
      socket = io(url, { transports: ["websocket"] })
      this.sockets.set(normalized, socket)
      if (this.discoveryEnabled) {
        socket.on(DEFAULT_DISCOVERY_TOPIC, (raw: any) => {
          try {
            const payload = typeof raw === "string" ? JSON.parse(raw) : raw
            if (payload?.serviceName) this.discoveryRegistry.update(payload as DiscoveryAnnouncement)
          } catch (err) {
            this.logger.error({ event: "socket.discovery.parse_error", err: (err as Error)?.message })
          }
        })
      }
    }
    return socket
  }

  async query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string>; timeoutMs?: number; tenantId?: string }): Promise<T> {
    const cbKey = `${normalizeServiceName(serviceName)}:${method}`
    return this.shutdown.trackInflight((async () => {
      if (opts?.idempotencyKey && this.idempotencyCache.isEnabled() && this.idempotencyCache.has(opts.idempotencyKey)) {
        return this.idempotencyCache.get(opts.idempotencyKey) as T
      }
      const result = await withRetry(async (attempt) => {
        this.circuitBreaker.before(cbKey)
        try {
          const socket = this.getSocket(serviceName)
          const { env } = this.buildEnvelope(method, params, "query", { ...opts, headers: { ...(opts?.headers || {}), "nevo-attempt": String(attempt) } })
          const { promise, resolve, reject } = Promise.withResolvers<T>()
          const effectiveTimeout = opts?.timeoutMs ?? this.timeoutMs
          const timer = setTimeout(() => reject(new TimeoutError(serviceName, method, effectiveTimeout)), effectiveTimeout)
          socket.emit("nevo:query", env, (response: any) => {
            clearTimeout(timer)
            if (response?.params?.result === "error" && response?.params?.error) {
              const err = response.params.error
              reject(new MessagingError(err.code, err.details ?? { message: err.message }, err.service || serviceName))
              return
            }
            this.circuitBreaker.onSuccess(cbKey)
            this.metrics.incCounter(NEVO_METRIC_NAMES.requestsTotal, { transport: "socketio", service: serviceName, method: methodLabel(method), role: "client" })
            resolve(response?.params?.result as T)
          })
          return await promise
        } catch (err) {
          this.circuitBreaker.onFailure(cbKey, err)
          if (attempt > 1) this.metrics.incCounter(NEVO_METRIC_NAMES.retries, { transport: "socketio", service: serviceName, method: methodLabel(method) })
          throw err
        }
      }, this.retryOptions)
      if (opts?.idempotencyKey && this.idempotencyCache.isEnabled()) this.idempotencyCache.set(opts.idempotencyKey, result)
      return result
    })())
  }

  async emit(serviceName: string, method: string, params: unknown, opts?: any): Promise<void> {
    const socket = this.getSocket(serviceName)
    const { env } = this.buildEnvelope(method, params, "emit", opts)
    socket.emit("nevo:emit", env)
  }

  async publish(serviceName: string, method: string, params: unknown, opts?: any): Promise<void> {
    const socket = this.getSocket(serviceName)
    const { env } = this.buildEnvelope(method, params, "sub", opts)
    socket.emit("nevo:publish", env)
  }

  async broadcast(method: string, params: unknown, opts?: any): Promise<void> {
    const first = this.serviceUrls.keys().toArray()[0]
    if (!first) throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: "No base URL available for broadcast" })
    const socket = this.getSocket(first)
    const { env } = this.buildEnvelope(method, params, "broadcast", opts)
    socket.emit("nevo:broadcast", env)
  }

  async subscribe<T = unknown>(
    serviceName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: (data: T, context: SubscriptionContext) => Promise<void> | void
  ): Promise<Subscription> {
    const normalized = normalizeServiceName(serviceName)
    const isBroadcast = normalized === DEFAULT_BROADCAST_TOPIC
    const socket = this.getSocket(isBroadcast ? this.serviceUrls.keys().toArray()[0] : serviceName)
    const room = options?.room ?? (method ? `${normalized}:${method}` : `${normalized}`)

    if (!isBroadcast) socket.emit("nevo:subscribe", { serviceName, method, room })

    const onMessage = async (raw: any) => {
      const payload: any = typeof raw === "string" ? JSON.parse(raw) : raw
      if (method && payload.method !== method && payload.method?.split("@")[0] !== method) return
      if (!matchesFilter(options?.filter, payload.meta)) return

      const context: SubscriptionContext = {
        meta: payload.meta || {},
        ack: async () => {},
        nack: async () => {}
      }
      try {
        await handler(payload.params as T, context)
      } catch (err) {
        this.logger.error({ event: "socket.sub.handler_error", err: (err as Error)?.message })
      }
    }

    const event = isBroadcast ? "nevo:broadcast" : "nevo:sub"
    socket.on(event, onMessage)

    return {
      unsubscribe: async () => {
        socket.off(event, onMessage)
        if (!isBroadcast) socket.emit("nevo:unsubscribe", { serviceName, method, room })
      }
    }
  }

  getAvailableServices(): string[] { return this.serviceUrls.keys().toArray() }
  getDiscoveredServices() { this.discoveryRegistry.prune(this.discoveryTtlMs); return this.discoveryRegistry.list() }
  isServiceAvailable(serviceName: string): boolean { return this.discoveryRegistry.isAvailable(serviceName, this.discoveryTtlMs) }

  async close(timeoutMs = 30_000): Promise<void> {
    this.discoveryRegistry.stopBackgroundPrune()
    for (const s of this.sockets.values()) {
      try { s.close() } catch {}
    }
    this.sockets.clear()
    await this.shutdown.shutdown(timeoutMs)
  }
}
