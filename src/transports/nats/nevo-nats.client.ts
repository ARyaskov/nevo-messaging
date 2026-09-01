import type { NatsConnection, Subscription as NatsSubscription, ConnectionOptions } from "@nats-io/nats-core"
import type { OnModuleDestroy } from "@nestjs/common"
import { uuidv7 } from "../../common/uuid"
import {
  DEFAULT_BROADCAST_TOPIC,
  DEFAULT_DISCOVERY_TOPIC,
  DEFAULT_SUBSCRIPTION_SUFFIX,
  DiscoveryRegistry,
  DiscoveryAnnouncement,
  MessageType,
  MessagingError,
  TimeoutError,
  ErrorCode,
  Subscription,
  SubscriptionContext,
  SubscriptionOptions,
  matchesFilter,
  Codec,
  NevoLogger,
  enforcePayloadLimit,
  ClientRuntime,
  type ClientCallOptions,
  type EncodedRequest,
  parseMethod,
  TransportClientOptions,
  normalizeServiceName,
  mapLimit
} from "../../common"
import { getNatsModule } from "../optional-deps"

// Cap on concurrent async encode+compress operations during a batch publish.
const BATCH_ENCODE_CONCURRENCY = 16

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

export class NevoNatsClient implements OnModuleDestroy {
  private nc: NatsConnection | null = null
  private connectingPromise: Promise<NatsConnection> | null = null
  private readonly runtime: ClientRuntime
  private readonly serviceNames: string[]
  private readonly timeoutMs: number
  private readonly serviceName?: string
  private readonly instanceId: string
  private readonly logger: NevoLogger
  private readonly codec: Codec
  private readonly maxPayloadBytes: number
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
  private readonly capabilities?: string[]
  private readonly host?: string
  private readonly port?: number
  private readonly version?: string
  private readonly opts: NevoNatsClientOptions

  constructor(serviceNames: string[], options?: NevoNatsClientOptions, preConnected?: NatsConnection) {
    this.opts = options || {}
    this.serviceNames = serviceNames.map((n) => n.toLowerCase())
    this.runtime = new ClientRuntime(options, { transport: "nats" })
    this.timeoutMs = this.runtime.timeoutMs
    this.serviceName = this.runtime.serviceName
    this.instanceId = this.runtime.instanceId
    this.logger = this.runtime.logger
    this.codec = this.runtime.codec
    this.maxPayloadBytes = this.runtime.maxPayloadBytes
    this.servers = options?.servers && options.servers.length > 0 ? options.servers : ["nats://127.0.0.1:4222"]
    this.lazyConnect = options?.reconnect?.lazyConnect === true
    this.jetstreamEnabled = options?.jetstream?.enabled === true
    // Opt-in: announcements go to a shared, unauthenticated subject.
    this.discoveryEnabled = options?.discovery?.enabled === true
    this.discoveryHeartbeatIntervalMs = options?.discovery?.heartbeatIntervalMs || 10000
    this.discoveryTtlMs = options?.discovery?.ttlMs || 30000
    this.capabilities = options?.discovery?.capabilities
    this.host = options?.discovery?.host
    this.port = options?.discovery?.port
    this.version = options?.discovery?.version

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

  getInstanceId(): string {
    return this.instanceId
  }
  getNatsConnection(): NatsConnection | null {
    return this.nc
  }

  async ensureConnection(): Promise<NatsConnection> {
    if (this.nc) return this.nc
    if (this.connectingPromise) return this.connectingPromise
    const { connect } = getNatsModule()
    this.connectingPromise = connect(this.connectionOpts)
      .then((nc) => {
        this.nc = nc
        this.afterConnect(nc)
        return nc
      })
      .finally(() => {
        this.connectingPromise = null
      })
    return this.connectingPromise
  }

  private afterConnect(nc: NatsConnection): void {
    this.watchStatus(nc).catch((err) => this.logger.error({ event: "nats.status.crashed", err: (err as Error)?.message }))
    if (this.discoveryEnabled) {
      this.initDiscovery(nc).catch((err) => this.logger.error({ event: "discovery.crashed", err: (err as Error)?.message }))
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

  private encodeRequestSync(method: string, params: unknown, type: MessageType, opts?: ClientCallOptions & { uuid?: string }): EncodedRequest {
    return this.runtime.encodeSync(method, params, type, opts)
  }

  private encodeRequest(
    method: string,
    params: unknown,
    type: MessageType,
    opts?: ClientCallOptions & { uuid?: string }
  ): EncodedRequest | Promise<EncodedRequest> {
    return this.runtime.encode(method, params, type, opts)
  }

  private decodePayload<T = any>(data: Uint8Array, encoding?: string): T | Promise<T> {
    return this.runtime.decode<T>(data, encoding)
  }

  private ensureServiceRegistered(serviceName: string): string {
    const normalized = normalizeServiceName(serviceName)
    if (!this.serviceNames.includes(normalized)) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, {
        message: `Service "${serviceName}" is not registered in nevo nats client`,
        availableServices: this.serviceNames
      })
    }
    return normalized
  }

  async query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<T> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const subject = `${normalized}-events`
    return this.runtime.query<T>({
      serviceName: normalized,
      method,
      params,
      opts,
      mapError: (err: any) => (err?.code === "TIMEOUT" ? new TimeoutError(serviceName, method, opts?.timeoutMs ?? this.timeoutMs) : err),
      send: async (request) => {
        const nc = await this.ensureConnection()
        const msg = await nc.request(subject, request.payload, {
          timeout: opts?.timeoutMs ?? this.timeoutMs,
          headers: this.toNatsHeaders(request.meta.headers, request.encoding)
        })
        return this.decodePayload(msg.data, getNatsHeader(msg.headers, "content-encoding"))
      }
    })
  }

  private toNatsHeaders(headers?: Record<string, string>, encoding?: string): any {
    const compressed = encoding !== undefined && encoding !== "identity"
    if (!headers && !compressed) return undefined
    try {
      const { headers: createHeaders } = getNatsModule() as any
      if (typeof createHeaders === "function") {
        const h = createHeaders()
        for (const [k, v] of Object.entries(headers || {})) h.set(k, v)
        if (compressed) h.set("content-encoding", encoding)
        return h
      }
    } catch {}
    return undefined
  }

  /** Flushes: `nc.publish` is fire-and-forget, so resolving early would lose events on a crash. */
  async emit(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const subject = `${normalized}-events`
    return this.runtime.emit({
      serviceName: normalized,
      method,
      params,
      opts,
      send: async (request) => {
        const nc = await this.ensureConnection()
        nc.publish(subject, request.payload, { headers: this.toNatsHeaders(request.meta.headers, request.encoding) })
        await nc.flush()
      }
    })
  }

  async publish(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const subject = methodSubject(`${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`, method)
    return this.runtime.emit({
      serviceName: normalized,
      method,
      params,
      opts,
      type: "sub",
      send: async (request) => {
        const nc = await this.ensureConnection()
        nc.publish(subject, request.payload, { headers: this.toNatsHeaders(request.meta.headers, request.encoding) })
        await nc.flush()
      }
    })
  }

  async broadcast(method: string, params: unknown, opts?: { version?: string; headers?: Record<string, string> }): Promise<void> {
    const nc = await this.ensureConnection()
    const { payload, meta, encoding } = await this.encodeRequest(method, params, "broadcast", opts)
    nc.publish(methodSubject(DEFAULT_BROADCAST_TOPIC, method), payload, { headers: this.toNatsHeaders(meta.headers, encoding) })
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
    const { payload, meta, encoding } = await this.encodeRequest(method, params, "query", opts)
    const iter: AsyncIterable<any> = (nc as any).requestMany(subject, payload, {
      maxMessages: opts?.maxMessages ?? 10,
      maxWait: opts?.maxWait ?? opts?.timeoutMs ?? this.timeoutMs,
      headers: this.toNatsHeaders(meta.headers, encoding)
    })
    const results: T[] = []
    for await (const msg of iter) {
      try {
        const responseEncoding = getNatsHeader(msg.headers, "content-encoding")
        const response: any = await this.decodePayload(msg.data, responseEncoding)
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
      try {
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
      } catch (err) {
        this.logger.error({ event: "nats.sub.wildcard_loop_error", err: (err as Error)?.message }, "wildcard subscription iterator ended")
      }
    })().catch((err) => this.logger.error({ event: "nats.sub.wildcard_crashed", err: (err as Error)?.message }))
    return {
      unsubscribe: async () => {
        this.subscriptions.delete(sub)
        sub.unsubscribe()
      }
    }
  }

  async subscribeQuery<T = unknown>(
    serviceName: string,
    method: string,
    params: unknown,
    onChunk: (chunk: T) => Promise<void> | void,
    onEnd?: (summary: { count: number; durationMs: number; error?: unknown }) => void,
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

    const { payload, meta, encoding } = await this.encodeRequest(method, params, "query", {
      ...opts,
      headers: { ...(opts?.headers || {}), "nevo-stream": "1", "nevo-reply-to": replySubject }
    })
    nc.publish(subject, payload, { reply: replySubject, headers: this.toNatsHeaders(meta.headers, encoding) })

    // Idle timeout: reset on every chunk so a long, steadily-producing stream
    // isn't cut off — only a gap longer than `timeoutMs` ends it.
    const timeoutMs = opts?.timeoutMs ?? this.timeoutMs
    let timedOut = false
    let idleTimer: NodeJS.Timeout | undefined
    const armIdle = () => {
      clearTimeout(idleTimer)
      idleTimer = setTimeout(() => {
        if (!cancelled) {
          timedOut = true
          void replySub.unsubscribe()
        }
      }, timeoutMs)
      if (typeof idleTimer.unref === "function") idleTimer.unref()
    }
    armIdle()
    ;(async () => {
      let streamError: unknown
      try {
        for await (const msg of replySub) {
          if (cancelled) break
          armIdle()
          try {
            const encoding = getNatsHeader(msg.headers, "content-encoding")
            const response: any = await this.decodePayload(msg.data, encoding)
            if (response?.meta?.headers?.["nevo-stream-end"] === "1") break
            if (response?.params?.result === undefined) continue
            count++
            await onChunk(response.params.result as T)
          } catch {}
        }
        if (timedOut && !cancelled) streamError = new TimeoutError(serviceName, method, timeoutMs)
      } finally {
        clearTimeout(idleTimer)
        this.subscriptions.delete(replySub)
        try {
          onEnd?.({ count, durationMs: Date.now() - startMs, error: streamError })
        } catch (err) {
          this.logger.error({ event: "nats.stream.on_end_error", err: (err as Error)?.message }, "subscribeQuery onEnd callback threw")
        }
      }
    })().catch((err) => this.logger.error({ event: "nats.stream.crashed", err: (err as Error)?.message }))

    return {
      cancel: async () => {
        cancelled = true
        try {
          replySub.unsubscribe()
        } catch {}
      }
    }
  }

  async emitBatch(
    items: Array<{
      serviceName: string
      method: string
      params: unknown
      opts?: { version?: string; headers?: Record<string, string>; idempotencyKey?: string }
    }>
  ): Promise<void> {
    if (items.length === 0) return
    const nc = await this.ensureConnection()
    if (this.runtime.compression.async && this.runtime.compression.enabled) {
      const encoded = await mapLimit(items, BATCH_ENCODE_CONCURRENCY, async (item) => {
        const normalized = this.ensureServiceRegistered(item.serviceName)
        const subject = `${normalized}-events`
        const { payload, meta, encoding } = await this.runtime.encodeAsync(item.method, item.params, "emit", item.opts)
        return { subject, payload, headers: this.toNatsHeaders(meta.headers, encoding) }
      })
      for (const e of encoded) nc.publish(e.subject, e.payload, e.headers ? { headers: e.headers } : undefined)
    } else {
      for (const item of items) {
        const normalized = this.ensureServiceRegistered(item.serviceName)
        const subject = `${normalized}-events`
        const { payload, meta, encoding } = this.encodeRequestSync(item.method, item.params, "emit", item.opts)
        const headers = this.toNatsHeaders(meta.headers, encoding)
        nc.publish(subject, payload, headers ? { headers } : undefined)
      }
    }
    await nc.flush()
  }

  async publishBatch(
    items: Array<{ serviceName: string; method: string; params: unknown; opts?: { version?: string; headers?: Record<string, string> } }>
  ): Promise<void> {
    if (items.length === 0) return
    const nc = await this.ensureConnection()
    if (!(this.runtime.compression.async && this.runtime.compression.enabled)) {
      for (const item of items) {
        const normalized = this.ensureServiceRegistered(item.serviceName)
        const subject = methodSubject(`${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`, item.method)
        const { payload, meta, encoding } = this.encodeRequestSync(item.method, item.params, "sub", item.opts)
        const headers = this.toNatsHeaders(meta.headers, encoding)
        nc.publish(subject, payload, headers ? { headers } : undefined)
      }
      await nc.flush()
      return
    }
    const encoded = await mapLimit(items, BATCH_ENCODE_CONCURRENCY, async (item) => {
      const normalized = this.ensureServiceRegistered(item.serviceName)
      const subject = methodSubject(`${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`, item.method)
      const { payload, meta, encoding } = await this.encodeRequest(item.method, item.params, "sub", item.opts)
      return { subject, payload, headers: this.toNatsHeaders(meta.headers, encoding) }
    })
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

    // Method-scoped subject: NATS filters server-side; the in-process method
    // check below only refines version-suffixed subscriptions.
    const subject = methodSubject(isBroadcast ? DEFAULT_BROADCAST_TOPIC : `${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`, method, "subscribe")
    const methodIsPattern = !!method && /[*>]/.test(method)
    const nc = await this.ensureConnection()
    const sub = nc.subscribe(subject, options?.groupId ? { queue: options.groupId } : undefined)
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
          // Wildcard patterns are already filtered by the NATS subject itself.
          if (method && !methodIsPattern && payload.method !== method && parseMethod(payload.method ?? "").name !== method) continue
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

  getAvailableServices(): string[] {
    return [...this.serviceNames]
  }
  getDiscoveredServices() {
    this.discoveryRegistry.prune(this.discoveryTtlMs)
    return this.discoveryRegistry.list()
  }
  isServiceAvailable(serviceName: string): boolean {
    return this.discoveryRegistry.isAvailable(serviceName, this.discoveryTtlMs)
  }

  private async initDiscovery(nc: NatsConnection): Promise<void> {
    this.discoverySubscription = nc.subscribe(DEFAULT_DISCOVERY_TOPIC)
    ;(async () => {
      try {
        for await (const msg of this.discoverySubscription!) {
          try {
            // Unauthenticated input: cap before the codec can pre-allocate.
            enforcePayloadLimit(msg.data, this.maxPayloadBytes)
            const payload = this.codec.decode<DiscoveryAnnouncement>(msg.data)
            if (payload?.serviceName) this.discoveryRegistry.update(payload)
          } catch (err) {
            this.logger.error({ event: "discovery.parse_error", err: (err as Error)?.message }, "Failed to parse discovery message")
          }
        }
      } catch (err) {
        this.logger.error({ event: "discovery.subscription_error", err: (err as Error)?.message }, "discovery subscription iterator ended")
      }
    })().catch((err) => this.logger.error({ event: "discovery.listener_crashed", err: (err as Error)?.message }))

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

  onModuleDestroy(): Promise<void> {
    return this.close()
  }

  async close(timeoutMs = 30_000): Promise<void> {
    if (this.discoveryTimer) clearInterval(this.discoveryTimer)
    this.discoveryRegistry.stopBackgroundPrune()
    if (this.discoverySubscription) this.discoverySubscription.unsubscribe()
    for (const sub of this.subscriptions) sub.unsubscribe()
    this.subscriptions.clear()
    await this.runtime.close(timeoutMs)
    if (this.nc) {
      try {
        await this.nc.drain()
      } catch {}
      this.nc = null
    }
  }
}

function getNatsHeader(h: any, key: string): string | undefined {
  if (!h) return undefined
  try {
    return h.get(key)
  } catch {
    return undefined
  }
}

/**
 * Pub/sub subjects are method-scoped (`<service>-events.sub.<method>`) so NATS
 * filters server-side instead of every subscriber receiving and discarding the
 * whole service stream. The version suffix is stripped: subscribers of a base
 * method receive every version and refine in-process. In subscribe mode the
 * NATS wildcards `*`/`>` pass through, so `user.*` patterns work natively.
 */
export function methodSubject(prefix: string, method: string | undefined | null, mode: "publish" | "subscribe" = "publish"): string {
  if (!method) return `${prefix}.>`
  const base = parseMethod(method).name
  const token = base
    .split(".")
    .map((t) => {
      if (mode === "subscribe" && (t === "*" || t === ">")) return t
      return t.replace(/[\s*>]/g, "_") || "_"
    })
    .join(".")
  return `${prefix}.${token}`
}
