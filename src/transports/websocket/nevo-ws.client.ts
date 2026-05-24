import {
  DEFAULT_BROADCAST_TOPIC,
  DiscoveryRegistry,
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
  MetricsRegistry,
  GracefulShutdown,
  LruIdempotencyCache,
  TransportClientOptions,
  matchesFilter,
  DEFAULT_METHOD_VERSION,
  formatMethod,
  DevToolsBus,
  getDevToolsBus,
  publishClientEvent,
  uuidv7,
  normalizeServiceName,
  resolveOutboundChainId
} from "../../common"

export interface NevoWsClientOptions extends TransportClientOptions {
  timeoutMs?: number
  reconnectIntervalMs?: number
  maxReconnectAttempts?: number
  protocols?: string | string[]
  headers?: Record<string, string>
}

interface PendingQuery {
  resolve: (value: unknown) => void
  reject: (err: unknown) => void
  timer: NodeJS.Timeout
}

interface ServiceSocket {
  url: string
  socket: any
  pending: Map<string, PendingQuery>
  subscriptions: Map<string, Set<(payload: any) => void>>
  reconnectAttempt: number
  closed: boolean
}

export class NevoWsClient {
  private readonly serviceUrls: Map<string, string>
  private readonly timeoutMs: number
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
  private readonly sockets = new Map<string, ServiceSocket>()
  private readonly devtoolsBus: DevToolsBus | null
  private readonly reconnectIntervalMs: number
  private readonly maxReconnectAttempts: number
  private readonly protocols?: string | string[]
  private readonly metaStaticPart: Pick<MessageMeta, "service" | "instanceId" | "auth" | "codec">
  private readonly discoveryRegistry = new DiscoveryRegistry()
  private readonly discoveryEnabled: boolean
  private readonly discoveryTtlMs: number

  constructor(serviceUrls: Record<string, string>, options?: NevoWsClientOptions) {
    this.serviceUrls = new Map(Object.entries(serviceUrls).map(([k, v]) => [k.toLowerCase(), v]))
    this.timeoutMs = options?.timeoutMs ?? options?.timeout ?? 20000
    this.serviceName = options?.serviceName
    this.instanceId = options?.instanceId || uuidv7()
    this.authToken = options?.authToken
    this.logger = (options?.logger as NevoLogger) || getDefaultLogger().child({ component: "ws-client", service: this.serviceName })
    this.codec = typeof options?.codec === "string" ? getCodec(options.codec) : (options?.codec as Codec) || getDefaultCodec()
    this.circuitBreaker = new CircuitBreakerRegistry(options?.circuitBreaker)
    this.retryOptions = resolveRetryOptions(options?.retry)
    this.compression = resolveCompressionOptions(options?.compression)
    this.tracer = getDefaultTracer()
    this.metrics = getDefaultMetrics()
    this.maxPayloadBytes = options?.security?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
    this.idempotencyCache = new LruIdempotencyCache<unknown>(options?.idempotency)
    this.devtoolsBus = options?.devtools === false ? null : (options?.devtools instanceof Object ? (options.devtools as DevToolsBus) : getDevToolsBus())
    this.reconnectIntervalMs = options?.reconnectIntervalMs ?? 1000
    this.maxReconnectAttempts = options?.maxReconnectAttempts ?? -1
    this.protocols = options?.protocols
    this.discoveryEnabled = options?.discovery?.enabled === true
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

  private buildEnvelope(method: string, params: any, type: MessageType, opts?: any) {
    const uuid = uuidv7()
    const meta = this.buildMeta(type, opts)
    const versioned = method.includes("@") ? method : formatMethod(method, opts?.version || DEFAULT_METHOD_VERSION)
    const envelope = { uuid, method: versioned, params, meta }
    const data = this.codec.encode(envelope)
    if (data.byteLength > this.maxPayloadBytes) {
      throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Payload size ${data.byteLength}B exceeds ${this.maxPayloadBytes}B` })
    }
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "out", service: this.serviceName ?? "unknown" }, data.byteLength)
    return { envelope, uuid, data, meta }
  }

  private async getSocket(serviceName: string): Promise<ServiceSocket> {
    const normalized = normalizeServiceName(serviceName)
    const url = this.serviceUrls.get(normalized)
    if (!url) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: `Service "${serviceName}" is not registered`, availableServices: this.serviceUrls.keys().toArray() })
    }
    let entry = this.sockets.get(normalized)
    if (entry && entry.socket.readyState === 1) return entry
    if (!entry) {
      entry = { url, socket: null, pending: new Map(), subscriptions: new Map(), reconnectAttempt: 0, closed: false }
      this.sockets.set(normalized, entry)
    }
    await this.openSocket(normalized, entry)
    return entry
  }

  private openSocket(serviceKey: string, entry: ServiceSocket): Promise<void> {
    const { promise, resolve, reject } = Promise.withResolvers<void>()
    const Ws = (globalThis as any).WebSocket
    if (!Ws) {
      reject(new MessagingError(ErrorCode.INTERNAL, { message: "Global WebSocket not available; requires Node 22+ or polyfill" }))
      return promise
    }
    const ws = new Ws(entry.url, this.protocols)
    ws.binaryType = "arraybuffer"
    entry.socket = ws
    ws.addEventListener("open", () => { entry.reconnectAttempt = 0; resolve() })
    ws.addEventListener("error", (ev: any) => {
      this.logger.warn({ event: "ws.error", err: ev?.message ?? "ws error", service: serviceKey })
    })
    ws.addEventListener("close", () => {
      for (const [, p] of entry.pending) {
        clearTimeout(p.timer)
        p.reject(new MessagingError(ErrorCode.CONNECTION_LOST, { message: "WebSocket closed before reply" }))
      }
      entry.pending.clear()
      if (entry.closed) return
      if (this.maxReconnectAttempts >= 0 && entry.reconnectAttempt >= this.maxReconnectAttempts) return
      entry.reconnectAttempt++
      setTimeout(() => { void this.openSocket(serviceKey, entry) }, this.reconnectIntervalMs)
    })
    ws.addEventListener("message", (ev: MessageEvent) => {
      const data = ev.data
      const buf = data instanceof ArrayBuffer ? new Uint8Array(data) : typeof data === "string" ? new TextEncoder().encode(data) : new Uint8Array(data)
      this.handleMessage(entry, buf)
    })
    return promise
  }

  private handleMessage(entry: ServiceSocket, buf: Uint8Array): void {
    let envelope: any
    try {
      envelope = this.codec.decode(buf)
    } catch (err) {
      this.logger.warn({ event: "ws.decode_error", err: (err as Error)?.message })
      return
    }
    const uuid = envelope?.uuid
    if (uuid && entry.pending.has(uuid)) {
      const pending = entry.pending.get(uuid)!
      entry.pending.delete(uuid)
      clearTimeout(pending.timer)
      if (envelope?.params?.result === "error" && envelope?.params?.error) {
        const err = envelope.params.error
        pending.reject(new MessagingError(err.code, err.details ?? { message: err.message }, err.service))
      } else {
        pending.resolve(envelope?.params?.result)
      }
      return
    }

    const method: string = envelope?.method
    if (!method) return
    for (const [, handlers] of entry.subscriptions) {
      if (!matchesFilter(undefined, envelope.meta)) continue
      for (const h of handlers) {
        try { h(envelope) } catch {}
      }
    }
  }

  async query<T = any>(serviceName: string, method: string, params: any, opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string>; timeoutMs?: number; tenantId?: string }): Promise<T> {
    const cbKey = `${normalizeServiceName(serviceName)}:${method}`
    return this.shutdown.trackInflight((async () => {
      if (opts?.idempotencyKey && this.idempotencyCache.isEnabled() && this.idempotencyCache.has(opts.idempotencyKey)) {
        return this.idempotencyCache.get(opts.idempotencyKey) as T
      }
      const result = await withRetry(async (attempt) => {
        this.circuitBreaker.before(cbKey)
        const startMs = Date.now()
        let lastUuid: string | undefined
        let lastChainId: string | undefined
        try {
          const entry = await this.getSocket(serviceName)
          const { uuid, data, meta } = this.buildEnvelope(method, params, "query", { ...opts, headers: { ...(opts?.headers || {}), "nevo-attempt": String(attempt) } })
          lastUuid = uuid
          lastChainId = meta.nevoChainId
          const { promise, resolve, reject } = Promise.withResolvers<T>()
          const effectiveTimeout = opts?.timeoutMs ?? this.timeoutMs
          const timer = setTimeout(() => {
            entry.pending.delete(uuid)
            reject(new TimeoutError(serviceName, method, effectiveTimeout))
          }, effectiveTimeout)
          entry.pending.set(uuid, { resolve: resolve as any, reject, timer })
          entry.socket.send(data)
          const value = await promise
          this.circuitBreaker.onSuccess(cbKey)
          publishClientEvent(this.devtoolsBus, { service: serviceName, method, uuid, chainId: lastChainId, durationMs: Date.now() - startMs, status: "ok", transport: "ws", origin: this.serviceName })
          return value
        } catch (err: any) {
          this.circuitBreaker.onFailure(cbKey, err)
          publishClientEvent(this.devtoolsBus, {
            service: serviceName, method, uuid: lastUuid, chainId: lastChainId,
            durationMs: Date.now() - startMs, status: "error", transport: "ws", origin: this.serviceName,
            error: { code: err instanceof MessagingError ? err.code : err?.code, message: err?.message ?? String(err) }
          })
          throw err
        } finally {
          this.metrics.incCounter(NEVO_METRIC_NAMES.requestsTotal, { transport: "ws", service: serviceName, method, role: "client" })
          if (attempt > 1) this.metrics.incCounter(NEVO_METRIC_NAMES.retries, { transport: "ws", service: serviceName, method })
        }
      }, this.retryOptions)
      if (opts?.idempotencyKey && this.idempotencyCache.isEnabled()) this.idempotencyCache.set(opts.idempotencyKey, result)
      return result
    })())
  }

  async emit(serviceName: string, method: string, params: any, opts?: any): Promise<void> {
    const entry = await this.getSocket(serviceName)
    const { data } = this.buildEnvelope(method, params, "emit", opts)
    entry.socket.send(data)
  }

  async publish(serviceName: string, method: string, params: any, opts?: any): Promise<void> {
    const entry = await this.getSocket(serviceName)
    const { data } = this.buildEnvelope(method, params, "sub", opts)
    entry.socket.send(data)
  }

  async broadcast(method: string, params: any, opts?: any): Promise<void> {
    const first = this.serviceUrls.keys().next().value
    if (!first) throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: "No service URL configured" })
    const entry = await this.getSocket(first)
    const { data } = this.buildEnvelope(method, params, "broadcast", opts)
    entry.socket.send(data)
  }

  async subscribe<T = any>(
    serviceName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: (data: T, context: SubscriptionContext) => Promise<void> | void
  ): Promise<Subscription> {
    const normalized = normalizeServiceName(serviceName)
    const isBroadcast = normalized === DEFAULT_BROADCAST_TOPIC
    const target = isBroadcast ? (this.serviceUrls.keys().next().value as string) : serviceName
    const entry = await this.getSocket(target)

    const key = `${normalized}:${method}`
    let bag = entry.subscriptions.get(key)
    if (!bag) { bag = new Set(); entry.subscriptions.set(key, bag) }
    const wrapped = (envelope: any) => {
      if (method && envelope.method !== method && envelope.method?.split("@")[0] !== method) return
      if (!matchesFilter(options?.filter, envelope.meta)) return
      const ctx: SubscriptionContext = {
        meta: envelope.meta || {},
        ack: async () => {},
        nack: async () => {}
      }
      void handler(envelope.params as T, ctx)
    }
    bag.add(wrapped)

    const subscribeReq = this.buildEnvelope("__subscribe", { serviceName, method }, "sub", {})
    entry.socket.send(subscribeReq.data)

    return {
      unsubscribe: async () => {
        bag!.delete(wrapped)
        if (bag!.size === 0) entry.subscriptions.delete(key)
      }
    }
  }

  getAvailableServices(): string[] { return this.serviceUrls.keys().toArray() }
  getDiscoveredServices() { this.discoveryRegistry.prune(this.discoveryTtlMs); return this.discoveryRegistry.list() }
  isServiceAvailable(name: string): boolean { return this.discoveryRegistry.isAvailable(name, this.discoveryTtlMs) }

  async close(timeoutMs = 30_000): Promise<void> {
    this.discoveryRegistry.stopBackgroundPrune()
    for (const [, entry] of this.sockets) {
      entry.closed = true
      try { entry.socket?.close?.() } catch {}
    }
    this.sockets.clear()
    await this.shutdown.shutdown(timeoutMs)
  }
}
