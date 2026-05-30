import type { NatsConnection, Subscription as NatsSubscription, ConnectionOptions } from "@nats-io/nats-core"
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
  matchesFilter,
  Codec,
  getCodec,
  getDefaultCodec,
  NevoLogger,
  getDefaultLogger,
  CircuitBreakerRegistry,
  resolveRetryOptions,
  runClientPipeline,
  resolveCompressionOptions,
  maybeCompress,
  maybeCompressAsync,
  maybeDecompress,
  maybeDecompressAsync,
  shouldDecompressAsync,
  enforcePayloadLimit,
  DEFAULT_MAX_PAYLOAD_BYTES,
  getDefaultTracer,
  NevoTracer,
  getDefaultMetrics,
  NEVO_METRIC_NAMES,
  methodLabel,
  MetricsRegistry,
  GracefulShutdown,
  LruIdempotencyCache,
  ResolvedRetryOptions,
  ResolvedCompressionOptions,
  formatMethod,
  DEFAULT_METHOD_VERSION,
  TransportClientOptions,
  DevToolsBus,
  getDevToolsBus,
  publishClientEvent,
  normalizeServiceName
} from "../../common"
import { getNatsModule } from "../optional-deps"
import { resolveOutboundChainId } from "../../common/chain-context"

export interface NevoNatsClientOptions extends TransportClientOptions {
  servers?: string[]
  timeoutMs?: number
  reconnect?: {
    enabled?: boolean
    maxAttempts?: number
    timeWaitMs?: number
    jitterMs?: number
    jitterTlsMs?: number
    waitOnFirstConnect?: boolean
    lazyConnect?: boolean
  }
  jetstream?: {
    enabled?: boolean
  }
  subscribeMaxPending?: number
  subscribeOnSlow?: (info: { subject: string; pending: number }) => void
}

export class NevoNatsClient {
  private nc: NatsConnection | null = null
  private connectingPromise: Promise<NatsConnection> | null = null
  private readonly serviceNames: string[]
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
  private readonly servers: string[]
  private readonly connectionOpts: ConnectionOptions
  private readonly lazyConnect: boolean
  private readonly discoveryRegistry = new DiscoveryRegistry()
  private readonly discoveryEnabled: boolean
  private readonly discoveryHeartbeatIntervalMs: number
  private readonly discoveryTtlMs: number
  private discoveryTimer: NodeJS.Timeout | null = null
  private discoverySubscription: NatsSubscription | null = null
  private readonly subscriptions = new Set<NatsSubscription>()
  private readonly jetstreamEnabled: boolean
  private readonly maxPayloadBytes: number
  private readonly idempotencyCache: LruIdempotencyCache<unknown>
  private readonly defaultVersion: string
  private readonly capabilities?: string[]
  private readonly host?: string
  private readonly port?: number
  private readonly version?: string
  private readonly opts: NevoNatsClientOptions
  private readonly devtoolsBus: DevToolsBus | null
  private readonly metaStaticPart: Pick<MessageMeta, "service" | "instanceId" | "auth" | "codec">
  private readonly defaultContentEncoding: "gzip" | "deflate" | "zstd" | "identity"

  constructor(serviceNames: string[], options?: NevoNatsClientOptions, preConnected?: NatsConnection) {
    this.opts = options || {}
    this.serviceNames = serviceNames.map((n) => n.toLowerCase())
    this.timeoutMs = options?.timeoutMs ?? options?.timeout ?? 20000
    this.debug = options?.debug || false
    this.serviceName = options?.serviceName
    this.instanceId = options?.instanceId || randomUUID()
    this.authToken = options?.authToken
    this.logger = (options?.logger as NevoLogger) || getDefaultLogger().child({ component: "nats-client", service: this.serviceName })
    this.codec = typeof options?.codec === "string" ? getCodec(options.codec) : (options?.codec as Codec) || getDefaultCodec()
    this.circuitBreaker = new CircuitBreakerRegistry(options?.circuitBreaker)
    this.retryOptions = resolveRetryOptions(options?.retry)
    this.compression = resolveCompressionOptions(options?.compression)
    this.tracer = getDefaultTracer()
    this.metrics = getDefaultMetrics()
    this.servers = options?.servers && options.servers.length > 0 ? options.servers : ["nats://127.0.0.1:4222"]
    this.lazyConnect = options?.reconnect?.lazyConnect === true
    this.maxPayloadBytes = options?.security?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
    this.idempotencyCache = new LruIdempotencyCache<unknown>(options?.idempotency)
    this.defaultVersion = DEFAULT_METHOD_VERSION
    this.jetstreamEnabled = options?.jetstream?.enabled === true
    this.discoveryEnabled = options?.discovery?.enabled !== false
    this.discoveryHeartbeatIntervalMs = options?.discovery?.heartbeatIntervalMs || 10000
    this.discoveryTtlMs = options?.discovery?.ttlMs || 30000
    this.capabilities = options?.discovery?.capabilities
    this.host = options?.discovery?.host
    this.port = options?.discovery?.port
    this.version = options?.discovery?.version
    this.devtoolsBus = options?.devtools === false ? null : (options?.devtools instanceof Object ? (options.devtools as DevToolsBus) : getDevToolsBus())
    this.metaStaticPart = Object.freeze({
      service: this.serviceName,
      instanceId: this.instanceId,
      auth: this.authToken ? { token: this.authToken } : undefined,
      codec: this.codec.name
    })
    this.defaultContentEncoding = this.compression.enabled ? this.compression.algorithm : "identity"

    const reconnectEnabled = options?.reconnect?.enabled !== false
    const maxAttempts = options?.reconnect?.maxAttempts ?? -1
    const timeWaitMs = options?.reconnect?.timeWaitMs ?? 5000
    const waitOnFirstConnect = options?.reconnect?.waitOnFirstConnect ?? !this.lazyConnect

    this.connectionOpts = {
      servers: this.servers,
      maxReconnectAttempts: reconnectEnabled ? maxAttempts : 0,
      reconnectTimeWait: timeWaitMs,
      reconnectJitter: options?.reconnect?.jitterMs,
      reconnectJitterTLS: options?.reconnect?.jitterTlsMs,
      waitOnFirstConnect
    }

    if (preConnected) {
      this.nc = preConnected
      this.afterConnect(preConnected)
    }

    if (this.discoveryEnabled) {
      this.discoveryRegistry.startBackgroundPrune(this.discoveryTtlMs)
    }
  }

  static async create(serviceNames: string[], options?: NevoNatsClientOptions): Promise<NevoNatsClient> {
    const client = new NevoNatsClient(serviceNames, options)
    if (options?.reconnect?.lazyConnect !== true) {
      await client.ensureConnection()
    }
    return client
  }

  getInstanceId(): string { return this.instanceId }
  getNatsConnection(): NatsConnection | null { return this.nc }

  async ensureConnection(): Promise<NatsConnection> {
    if (this.nc) return this.nc
    if (this.connectingPromise) return this.connectingPromise
    const { connect } = getNatsModule()
    this.connectingPromise = connect(this.connectionOpts).then((nc) => {
      this.nc = nc
      this.afterConnect(nc)
      return nc
    }).finally(() => { this.connectingPromise = null })
    return this.connectingPromise
  }

  private afterConnect(nc: NatsConnection): void {
    this.watchStatus(nc)
    if (this.discoveryEnabled) {
      void this.initDiscovery(nc)
    }
  }

  private async watchStatus(nc: NatsConnection): Promise<void> {
    try {
      for await (const evt of nc.status()) {
        this.logger.debug({ event: "nats.status", type: evt.type, data: (evt as any).data })
      }
    } catch (err) {
      this.logger.warn({ event: "nats.status.error", err: (err as Error)?.message }, "status iterator ended")
    }
  }

  private buildMeta(type: MessageType, opts?: { idempotencyKey?: string; version?: string; headers?: Record<string, string>; tenantId?: string }): MessageMeta {
    const baseMeta: MessageMeta = {
      ...this.metaStaticPart,
      type,
      ts: Date.now(),
      version: opts?.version || this.defaultVersion,
      idempotencyKey: opts?.idempotencyKey,
      tenantId: opts?.tenantId,
      headers: opts?.headers,
      contentEncoding: this.defaultContentEncoding,
      // Stamp the chain id (inherited from the active ALS context if we're
      // inside a handler, otherwise a fresh one). DevTools groups by this.
      nevoChainId: resolveOutboundChainId()
    }
    return this.tracer.inject(baseMeta)
  }

  private encodeRequestSync(method: string, params: unknown, type: MessageType, opts?: { idempotencyKey?: string; version?: string; headers?: Record<string, string>; tenantId?: string }): { payload: Uint8Array; uuid: string; method: string; meta: MessageMeta } {
    const uuid = uuidv7()
    const meta = this.buildMeta(type, opts)
    const versionedMethod = method.includes("@") ? method : formatMethod(method, opts?.version || this.defaultVersion)
    const body = { uuid, method: versionedMethod, params, meta }
    const raw = this.codec.encode(body)
    if (raw.byteLength > this.maxPayloadBytes) {
      throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Payload size ${raw.byteLength}B exceeds ${this.maxPayloadBytes}B`, size: raw.byteLength, limit: this.maxPayloadBytes })
    }
    const compressed = maybeCompress(raw, this.compression)
    meta.contentEncoding = compressed.encoding
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "out", service: this.serviceName ?? "unknown" }, compressed.data.byteLength)
    return { payload: compressed.data, uuid, method: versionedMethod, meta }
  }

  private encodeRequest(method: string, params: unknown, type: MessageType, opts?: { idempotencyKey?: string; version?: string; headers?: Record<string, string>; tenantId?: string }): { payload: Uint8Array; uuid: string; method: string; meta: MessageMeta } | Promise<{ payload: Uint8Array; uuid: string; method: string; meta: MessageMeta }> {
    if (this.compression.async && this.compression.enabled) {
      return this.encodeRequestAsync(method, params, type, opts)
    }
    return this.encodeRequestSync(method, params, type, opts)
  }

  private async encodeRequestAsync(method: string, params: unknown, type: MessageType, opts?: { idempotencyKey?: string; version?: string; headers?: Record<string, string>; tenantId?: string }): Promise<{ payload: Uint8Array; uuid: string; method: string; meta: MessageMeta }> {
    const uuid = uuidv7()
    const meta = this.buildMeta(type, opts)
    const versionedMethod = method.includes("@") ? method : formatMethod(method, opts?.version || this.defaultVersion)
    const body = { uuid, method: versionedMethod, params, meta }
    const raw = this.codec.encode(body)
    if (raw.byteLength > this.maxPayloadBytes) {
      throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Payload size ${raw.byteLength}B exceeds ${this.maxPayloadBytes}B`, size: raw.byteLength, limit: this.maxPayloadBytes })
    }
    const compressed = await maybeCompressAsync(raw, this.compression)
    meta.contentEncoding = compressed.encoding
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "out", service: this.serviceName ?? "unknown" }, compressed.data.byteLength)
    return { payload: compressed.data, uuid, method: versionedMethod, meta }
  }

  private decodePayload<T = any>(data: Uint8Array, encoding?: string): T | Promise<T> {
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "in", service: this.serviceName ?? "unknown" }, data.byteLength)
    // Tiny / identity payloads inflate synchronously (no event-loop hop on the
    // common path); larger compressed buffers offload to the worker pool.
    if (!shouldDecompressAsync(data.byteLength, encoding)) {
      const decompressed = maybeDecompress(data, encoding, this.maxPayloadBytes)
      enforcePayloadLimit(decompressed, this.maxPayloadBytes)
      return this.codec.decode<T>(decompressed)
    }
    return this.decodePayloadAsync<T>(data, encoding)
  }

  private async decodePayloadAsync<T = any>(data: Uint8Array, encoding?: string): Promise<T> {
    const decompressed = await maybeDecompressAsync(data, encoding, this.maxPayloadBytes)
    enforcePayloadLimit(decompressed, this.maxPayloadBytes)
    return this.codec.decode<T>(decompressed)
  }

  private ensureServiceRegistered(serviceName: string): string {
    const normalized = normalizeServiceName(serviceName)
    if (!this.serviceNames.includes(normalized)) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: `Service "${serviceName}" is not registered in nevo nats client`, availableServices: this.serviceNames })
    }
    return normalized
  }

  async query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string>; tenantId?: string; timeoutMs?: number }): Promise<T> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const subject = `${normalized}-events`
    const cbKey = `${normalized}:${method}`
    // Version-stripped label so `foo@v1`/`foo@v2` don't split into separate series.
    const metricMethod = methodLabel(method)

    return this.shutdown.trackInflight((async () => {
      const idempotencyKey = opts?.idempotencyKey
      if (idempotencyKey && this.idempotencyCache.isEnabled() && this.idempotencyCache.has(idempotencyKey)) {
        return this.idempotencyCache.get(idempotencyKey) as T
      }

      // Breaker wraps the *entire* retried operation through the shared client
      // pipeline, so one logical call records exactly one breaker outcome — not
      // one per `withRetry` attempt (which previously tripped it ~maxAttempts×
      // too early). Per-attempt spans / DevTools events / metrics stay inside.
      const result = await runClientPipeline<T>(this.circuitBreaker, this.retryOptions, cbKey, async (attempt) => {
        const startMs = Date.now()
        let lastUuid: string | undefined
        let lastChainId: string | undefined
        try {
          const nc = await this.ensureConnection()
          const { payload, meta, uuid } = await this.encodeRequest(method, params, "query", { ...opts, headers: { ...(opts?.headers || {}), "nevo-attempt": String(attempt) } })
          lastUuid = uuid
          lastChainId = meta.nevoChainId
          const span = this.tracer.startSpan(`nevo.client.query ${normalized}.${method}`, {
            "nevo.method": method,
            "nevo.service": normalized,
            "nevo.codec": this.codec.name,
            "nevo.attempt": attempt
          })
          try {
            const msg = await nc.request(subject, payload, { timeout: opts?.timeoutMs ?? this.timeoutMs, headers: this.toNatsHeaders(meta.headers) })
            const response: any = await this.decodePayload(msg.data, (meta.headers as any)?.["content-encoding"] || meta.contentEncoding)
            if (response?.params?.result === "error" && response?.params?.error) {
              const errorData = response.params.error
              throw new MessagingError(errorData.code, errorData.details ?? { message: errorData.message }, errorData.service || normalized)
            }
            span.setStatus({ code: 1 })
            publishClientEvent(this.devtoolsBus, { service: normalized, method, uuid, chainId: meta.nevoChainId, durationMs: Date.now() - startMs, status: "ok", transport: "nats", origin: this.serviceName })
            return response?.params?.result as T
          } catch (err: any) {
            span.recordException(err)
            span.setStatus({ code: 2, message: err?.message })
            if (err && err.code === "TIMEOUT") throw new TimeoutError(serviceName, method, opts?.timeoutMs ?? this.timeoutMs)
            throw err
          } finally {
            span.end()
            this.metrics.incCounter(NEVO_METRIC_NAMES.requestsTotal, { transport: "nats", service: normalized, method: metricMethod, role: "client" })
            if (attempt > 1) this.metrics.incCounter(NEVO_METRIC_NAMES.retries, { transport: "nats", service: normalized, method: metricMethod })
          }
        } catch (err: any) {
          publishClientEvent(this.devtoolsBus, {
            service: normalized,
            method,
            uuid: lastUuid,
            chainId: lastChainId,
            durationMs: Date.now() - startMs,
            status: "error",
            transport: "nats",
            origin: this.serviceName,
            error: { code: err instanceof MessagingError ? err.code : err?.code, message: err?.message ?? String(err) }
          })
          throw err
        }
      })

      if (idempotencyKey && this.idempotencyCache.isEnabled()) this.idempotencyCache.set(idempotencyKey, result)
      return result
    })())
  }

  private toNatsHeaders(headers?: Record<string, string>): any {
    if (!headers) return undefined
    try {
      const { headers: createHeaders } = getNatsModule() as any
      if (typeof createHeaders === "function") {
        const h = createHeaders()
        for (const [k, v] of Object.entries(headers)) h.set(k, v)
        return h
      }
    } catch {}
    return undefined
  }

  async emit(serviceName: string, method: string, params: unknown, opts?: { version?: string; headers?: Record<string, string>; idempotencyKey?: string }): Promise<void> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const subject = `${normalized}-events`
    const nc = await this.ensureConnection()
    const { payload, meta } = await this.encodeRequest(method, params, "emit", opts)
    nc.publish(subject, payload, { headers: this.toNatsHeaders(meta.headers) })
  }

  async publish(serviceName: string, method: string, params: unknown, opts?: { version?: string; headers?: Record<string, string> }): Promise<void> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const subject = `${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    const nc = await this.ensureConnection()
    const { payload, meta } = await this.encodeRequest(method, params, "sub", opts)
    nc.publish(subject, payload, { headers: this.toNatsHeaders(meta.headers) })
  }

  async broadcast(method: string, params: unknown, opts?: { version?: string; headers?: Record<string, string> }): Promise<void> {
    const nc = await this.ensureConnection()
    const { payload, meta } = await this.encodeRequest(method, params, "broadcast", opts)
    nc.publish(DEFAULT_BROADCAST_TOPIC, payload, { headers: this.toNatsHeaders(meta.headers) })
  }

  async requestMany<T = unknown>(
    serviceName: string,
    method: string,
    params: unknown,
    opts?: { version?: string; headers?: Record<string, string>; tenantId?: string; timeoutMs?: number; maxMessages?: number; maxWait?: number }
  ): Promise<T[]> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const subject = `${normalized}-events`
    const nc = await this.ensureConnection()
    const { payload, meta } = await this.encodeRequest(method, params, "query", opts)
    const iter: AsyncIterable<any> = (nc as any).requestMany(subject, payload, {
      maxMessages: opts?.maxMessages ?? 10,
      maxWait: opts?.maxWait ?? opts?.timeoutMs ?? this.timeoutMs,
      headers: this.toNatsHeaders(meta.headers)
    })
    const results: T[] = []
    for await (const msg of iter) {
      try {
        const encoding = getNatsHeader(msg.headers, "content-encoding") || meta.contentEncoding
        const response: any = await this.decodePayload(msg.data, encoding)
        if (response?.params?.result === "error") continue
        results.push(response?.params?.result as T)
      } catch {
        // skip malformed
      }
    }
    return results
  }

  async subscribeWildcard<T = unknown>(
    pattern: string,
    handler: (data: T, context: SubscriptionContext & { subject: string; method: string }) => Promise<void> | void
  ): Promise<Subscription> {
    const nc = await this.ensureConnection()
    const sub = nc.subscribe(pattern)
    this.subscriptions.add(sub)
    ;(async () => {
      for await (const msg of sub) {
        try {
          const encoding = getNatsHeader(msg.headers, "content-encoding")
          const payload: any = await this.decodePayload(msg.data, encoding)
          const ctx = {
            meta: payload.meta || {},
            ack: async () => {},
            nack: async () => {},
            subject: msg.subject,
            method: payload.method
          }
          await handler(payload.params as T, ctx as any)
        } catch (err) {
          this.logger.error({ event: "nats.sub.wildcard_handler_error", err: (err as Error)?.message })
        }
      }
    })()
    return { unsubscribe: async () => { this.subscriptions.delete(sub); sub.unsubscribe() } }
  }

  async subscribeQuery<T = unknown>(
    serviceName: string,
    method: string,
    params: unknown,
    onChunk: (chunk: T) => Promise<void> | void,
    onEnd?: (summary: { count: number; durationMs: number }) => void,
    opts?: { version?: string; headers?: Record<string, string>; timeoutMs?: number }
  ): Promise<{ cancel: () => Promise<void> }> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const nc = await this.ensureConnection()
    const subject = `${normalized}-events`
    const replySubject = `_INBOX.${this.instanceId}.${uuidv7()}`
    const replySub = nc.subscribe(replySubject)
    this.subscriptions.add(replySub)
    const startMs = Date.now()
    let count = 0
    let cancelled = false

    const { payload, meta } = await this.encodeRequest(method, params, "query", {
      ...opts,
      headers: { ...(opts?.headers || {}), "nevo-stream": "1", "nevo-reply-to": replySubject }
    })
    nc.publish(subject, payload, { reply: replySubject, headers: this.toNatsHeaders(meta.headers) })

    const timeoutMs = opts?.timeoutMs ?? this.timeoutMs
    const idleTimer = setTimeout(() => { if (!cancelled) { void replySub.unsubscribe() } }, timeoutMs)

    ;(async () => {
      try {
        for await (const msg of replySub) {
          if (cancelled) break
          try {
            const encoding = getNatsHeader(msg.headers, "content-encoding")
            const response: any = await this.decodePayload(msg.data, encoding)
            if (response?.meta?.headers?.["nevo-stream-end"] === "1") break
            if (response?.params?.result === undefined) continue
            count++
            await onChunk(response.params.result as T)
          } catch {}
        }
      } finally {
        clearTimeout(idleTimer)
        this.subscriptions.delete(replySub)
        onEnd?.({ count, durationMs: Date.now() - startMs })
      }
    })()

    return {
      cancel: async () => {
        cancelled = true
        try { replySub.unsubscribe() } catch {}
      }
    }
  }

  async emitBatch(items: Array<{ serviceName: string; method: string; params: unknown; opts?: { version?: string; headers?: Record<string, string>; idempotencyKey?: string } }>): Promise<void> {
    if (items.length === 0) return
    const nc = await this.ensureConnection()
    if (this.compression.async && this.compression.enabled) {
      const encoded = await Promise.all(items.map(async (item) => {
        const normalized = this.ensureServiceRegistered(item.serviceName)
        const subject = `${normalized}-events`
        const { payload, meta } = await this.encodeRequestAsync(item.method, item.params, "emit", item.opts)
        return { subject, payload, headers: this.toNatsHeaders(meta.headers) }
      }))
      for (const e of encoded) nc.publish(e.subject, e.payload, e.headers ? { headers: e.headers } : undefined)
    } else {
      for (const item of items) {
        const normalized = this.ensureServiceRegistered(item.serviceName)
        const subject = `${normalized}-events`
        const { payload, meta } = this.encodeRequestSync(item.method, item.params, "emit", item.opts)
        const headers = this.toNatsHeaders(meta.headers)
        nc.publish(subject, payload, headers ? { headers } : undefined)
      }
    }
    await nc.flush()
  }

  async publishBatch(items: Array<{ serviceName: string; method: string; params: unknown; opts?: { version?: string; headers?: Record<string, string> } }>): Promise<void> {
    if (items.length === 0) return
    const nc = await this.ensureConnection()
    if (!(this.compression.async && this.compression.enabled)) {
      for (const item of items) {
        const normalized = this.ensureServiceRegistered(item.serviceName)
        const subject = `${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`
        const { payload, meta } = this.encodeRequestSync(item.method, item.params, "sub", item.opts)
        const headers = this.toNatsHeaders(meta.headers)
        nc.publish(subject, payload, headers ? { headers } : undefined)
      }
      await nc.flush()
      return
    }
    const encoded = await Promise.all(items.map(async (item) => {
      const normalized = this.ensureServiceRegistered(item.serviceName)
      const subject = `${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`
      const { payload, meta } = await this.encodeRequest(item.method, item.params, "sub", item.opts)
      return { subject, payload, headers: this.toNatsHeaders(meta.headers) }
    }))
    for (const e of encoded) {
      nc.publish(e.subject, e.payload, e.headers ? { headers: e.headers } : undefined)
    }
    await nc.flush()
  }

  async flush(): Promise<void> {
    if (!this.nc) return
    await this.nc.flush()
  }

  async subscribe<T = unknown>(
    serviceName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: (data: T, context: SubscriptionContext) => Promise<void> | void
  ): Promise<Subscription> {
    const normalized = normalizeServiceName(serviceName)
    const isBroadcast = normalized === DEFAULT_BROADCAST_TOPIC
    if (!isBroadcast) this.ensureServiceRegistered(serviceName)

    const subject = isBroadcast ? DEFAULT_BROADCAST_TOPIC : `${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    const nc = await this.ensureConnection()
    const sub = nc.subscribe(subject)
    this.subscriptions.add(sub)

    const maxPending = this.opts.subscribeMaxPending
    const onSlow = this.opts.subscribeOnSlow
    const run = async () => {
      for await (const msg of sub) {
        if (maxPending !== undefined && typeof (sub as any).getPending === "function") {
          const pending = (sub as any).getPending() as number
          if (pending > maxPending) {
            onSlow?.({ subject, pending })
            this.logger.warn({ event: "nats.sub.slow_consumer", subject, pending, threshold: maxPending })
          }
        }
        try {
          const encoding = getNatsHeader(msg.headers, "content-encoding")
          const payload: any = await this.decodePayload(msg.data, encoding)
          if (method && payload.method !== method && payload.method?.split("@")[0] !== method) continue
          if (!matchesFilter(options?.filter, payload.meta)) continue
          if (options?.room && payload.meta?.headers?.["room"] !== options.room) continue

          const context: SubscriptionContext = {
            meta: payload.meta || {},
            ack: async () => {},
            nack: async () => {}
          }
          await handler(payload.params as T, context)
        } catch (err) {
          this.logger.error({ event: "nats.sub.handler_error", err: (err as Error)?.message }, "subscription handler failed")
        }
      }
    }
    void run()

    return {
      unsubscribe: async () => {
        this.subscriptions.delete(sub)
        sub.unsubscribe()
      }
    }
  }

  getAvailableServices(): string[] { return [...this.serviceNames] }
  getDiscoveredServices() { this.discoveryRegistry.prune(this.discoveryTtlMs); return this.discoveryRegistry.list() }
  isServiceAvailable(serviceName: string): boolean { return this.discoveryRegistry.isAvailable(serviceName, this.discoveryTtlMs) }

  private async initDiscovery(nc: NatsConnection): Promise<void> {
    this.discoverySubscription = nc.subscribe(DEFAULT_DISCOVERY_TOPIC)
    ;(async () => {
      for await (const msg of this.discoverySubscription!) {
        try {
          const payload = this.codec.decode<DiscoveryAnnouncement>(msg.data)
          if (payload?.serviceName) this.discoveryRegistry.update(payload)
        } catch (err) {
          this.logger.error({ event: "discovery.parse_error", err: (err as Error)?.message }, "Failed to parse discovery message")
        }
      }
    })()

    this.discoveryTimer = setInterval(() => {
      const announcement: DiscoveryAnnouncement = {
        serviceName: this.serviceName || "unknown",
        instanceId: this.instanceId,
        clientId: this.serviceName,
        transport: "nats",
        ts: Date.now(),
        host: this.host,
        port: this.port,
        version: this.version,
        capabilities: this.capabilities
      }
      try {
        nc.publish(DEFAULT_DISCOVERY_TOPIC, this.codec.encode(announcement))
      } catch (err) {
        this.logger.error({ event: "discovery.publish_failed", err: (err as Error)?.message })
      }
    }, this.discoveryHeartbeatIntervalMs)
    if (typeof this.discoveryTimer.unref === "function") this.discoveryTimer.unref()
  }

  async close(timeoutMs = 30_000): Promise<void> {
    if (this.discoveryTimer) clearInterval(this.discoveryTimer)
    this.discoveryRegistry.stopBackgroundPrune()
    if (this.discoverySubscription) this.discoverySubscription.unsubscribe()
    for (const sub of this.subscriptions) sub.unsubscribe()
    this.subscriptions.clear()
    await this.shutdown.shutdown(timeoutMs)
    if (this.nc) {
      try { await this.nc.drain() } catch {}
      this.nc = null
    }
  }
}

function getNatsHeader(h: any, key: string): string | undefined {
  if (!h) return undefined
  try { return h.get(key) } catch { return undefined }
}
