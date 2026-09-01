import {
  DEFAULT_BROADCAST_TOPIC,
  DiscoveryRegistry,
  MessageType,
  MessagingError,
  TimeoutError,
  ErrorCode,
  Subscription,
  SubscriptionContext,
  SubscriptionOptions,
  TransportClientOptions,
  ClientRuntime,
  type ClientCallOptions,
  type EncodedRequest,
  matchesFilter,
  normalizeServiceName,
  parseMethod
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
  subscribeRequests: Map<string, { serviceName: string; method: string }>
  reconnectAttempt: number
  closed: boolean
  opening: Promise<void> | null
}

export class NevoWsClient {
  private readonly runtime: ClientRuntime
  private readonly serviceUrls: Map<string, string>
  private readonly sockets = new Map<string, ServiceSocket>()
  private readonly reconnectIntervalMs: number
  private readonly maxReconnectAttempts: number
  private readonly protocols?: string | string[]
  private readonly discoveryRegistry = new DiscoveryRegistry()
  private readonly discoveryEnabled: boolean
  private readonly discoveryTtlMs: number

  constructor(serviceUrls: Record<string, string>, options?: NevoWsClientOptions) {
    this.serviceUrls = new Map(Object.entries(serviceUrls).map(([k, v]) => [k.toLowerCase(), v]))
    // A raw ws frame has no header channel to carry a content-encoding marker.
    this.runtime = new ClientRuntime(options, { transport: "ws", compressionCapable: false })
    this.reconnectIntervalMs = options?.reconnectIntervalMs ?? 1000
    this.maxReconnectAttempts = options?.maxReconnectAttempts ?? -1
    this.protocols = options?.protocols
    this.discoveryEnabled = options?.discovery?.enabled === true
    this.discoveryTtlMs = options?.discovery?.ttlMs || 30000
    if (this.discoveryEnabled) this.discoveryRegistry.startBackgroundPrune(this.discoveryTtlMs)
  }

  getInstanceId(): string {
    return this.runtime.instanceId
  }

  private encode(method: string, params: unknown, type: MessageType, opts?: ClientCallOptions): EncodedRequest {
    return this.runtime.encodeSync(method, params, type, opts)
  }

  private async getSocket(serviceName: string): Promise<ServiceSocket> {
    const normalized = normalizeServiceName(serviceName)
    const url = this.serviceUrls.get(normalized)
    if (!url) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, {
        message: `Service "${serviceName}" is not registered`,
        availableServices: this.serviceUrls.keys().toArray()
      })
    }
    let entry = this.sockets.get(normalized)
    if (entry && entry.socket?.readyState === 1) return entry
    if (!entry) {
      entry = {
        url,
        socket: null,
        pending: new Map(),
        subscriptions: new Map(),
        subscribeRequests: new Map(),
        reconnectAttempt: 0,
        closed: false,
        opening: null
      }
      this.sockets.set(normalized, entry)
    }
    await this.openSocket(normalized, entry)
    return entry
  }

  private openSocket(serviceKey: string, entry: ServiceSocket): Promise<void> {
    if (entry.opening) return entry.opening
    const { promise, resolve, reject } = Promise.withResolvers<void>()
    const Ws = (globalThis as any).WebSocket
    if (!Ws) {
      reject(new MessagingError(ErrorCode.INTERNAL, { message: "Global WebSocket not available; requires Node 22+ or polyfill" }))
      return promise
    }
    entry.opening = promise
    let settled = false
    const settle = (err?: unknown) => {
      if (settled) return
      settled = true
      if (entry.opening === promise) entry.opening = null
      if (err) reject(err)
      else resolve()
    }
    const ws = new Ws(entry.url, this.protocols)
    ws.binaryType = "arraybuffer"
    entry.socket = ws
    const connectTimer = setTimeout(() => {
      settle(
        new MessagingError(ErrorCode.TIMEOUT, {
          message: `WebSocket connect to "${serviceKey}" timed out after ${this.runtime.timeoutMs}ms`,
          retryable: true
        })
      )
      try {
        ws.close()
      } catch {}
    }, this.runtime.timeoutMs)
    ws.addEventListener("open", () => {
      clearTimeout(connectTimer)
      entry.reconnectAttempt = 0
      for (const req of entry.subscribeRequests.values()) {
        try {
          ws.send(this.encode("__subscribe", req, "sub").payload)
        } catch {}
      }
      settle()
    })
    ws.addEventListener("error", (ev: any) => {
      this.runtime.logger.warn({ event: "ws.error", err: ev?.message ?? "ws error", service: serviceKey })
      clearTimeout(connectTimer)
      settle(new MessagingError(ErrorCode.CONNECTION_LOST, { message: ev?.message ?? "WebSocket connection failed", retryable: true }))
    })
    ws.addEventListener("close", () => {
      clearTimeout(connectTimer)
      settle(new MessagingError(ErrorCode.CONNECTION_LOST, { message: "WebSocket closed before open", retryable: true }))
      for (const [, p] of entry.pending) {
        clearTimeout(p.timer)
        p.reject(new MessagingError(ErrorCode.CONNECTION_LOST, { message: "WebSocket closed before reply", retryable: true }))
      }
      entry.pending.clear()
      if (entry.closed) return
      if (this.maxReconnectAttempts >= 0 && entry.reconnectAttempt >= this.maxReconnectAttempts) return
      entry.reconnectAttempt++
      setTimeout(() => {
        this.openSocket(serviceKey, entry).catch(() => {})
      }, this.reconnectIntervalMs)
    })
    ws.addEventListener("message", (ev: MessageEvent) => {
      const data = ev.data
      const buf =
        data instanceof ArrayBuffer ? new Uint8Array(data) : typeof data === "string" ? new TextEncoder().encode(data) : new Uint8Array(data)
      this.handleMessage(entry, buf)
    })
    return promise
  }

  private handleMessage(entry: ServiceSocket, buf: Uint8Array): void {
    let envelope: any
    try {
      envelope = this.runtime.decode(buf)
    } catch (err) {
      this.runtime.logger.warn({ event: "ws.decode_error", err: (err as Error)?.message })
      return
    }
    const uuid = envelope?.uuid
    if (uuid && entry.pending.has(uuid)) {
      const pending = entry.pending.get(uuid)!
      entry.pending.delete(uuid)
      clearTimeout(pending.timer)
      pending.resolve(envelope)
      return
    }

    if (!envelope?.method) return
    for (const [, handlers] of entry.subscriptions) {
      for (const h of handlers) {
        try {
          h(envelope)
        } catch {}
      }
    }
  }

  async query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<T> {
    const normalized = normalizeServiceName(serviceName)
    return this.runtime.query<T>({
      serviceName: normalized,
      method,
      params,
      opts,
      send: async (request) => {
        const entry = await this.getSocket(serviceName)
        const { promise, resolve, reject } = Promise.withResolvers<unknown>()
        const timeout = opts?.timeoutMs ?? this.runtime.timeoutMs
        const timer = setTimeout(() => {
          entry.pending.delete(request.uuid)
          reject(new TimeoutError(serviceName, method, timeout))
        }, timeout)
        entry.pending.set(request.uuid, { resolve, reject, timer })
        entry.socket.send(request.payload)
        return promise
      }
    })
  }

  async emit(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    const normalized = normalizeServiceName(serviceName)
    return this.runtime.emit({
      serviceName: normalized,
      method,
      params,
      opts,
      send: async (request) => {
        const entry = await this.getSocket(serviceName)
        entry.socket.send(request.payload)
      }
    })
  }

  async publish(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    const normalized = normalizeServiceName(serviceName)
    return this.runtime.emit({
      serviceName: normalized,
      method,
      params,
      opts,
      type: "sub",
      send: async (request) => {
        const entry = await this.getSocket(serviceName)
        entry.socket.send(request.payload)
      }
    })
  }

  async broadcast(method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    const first = this.serviceUrls.keys().next().value
    if (!first) throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: "No service URL configured" })
    return this.runtime.emit({
      serviceName: DEFAULT_BROADCAST_TOPIC,
      method,
      params,
      opts,
      type: "broadcast",
      send: async (request) => {
        const entry = await this.getSocket(first)
        entry.socket.send(request.payload)
      }
    })
  }

  async subscribe<T = unknown>(
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
    if (!bag) {
      bag = new Set()
      entry.subscriptions.set(key, bag)
    }
    const wrapped = (envelope: any) => {
      if (method && envelope.method !== method && parseMethod(envelope.method ?? "").name !== method) return
      if (!matchesFilter(options?.filter, envelope.meta)) return
      const ctx: SubscriptionContext = {
        meta: envelope.meta || {},
        ack: async () => {},
        nack: async () => {}
      }
      void handler(envelope.params as T, ctx)
    }
    bag.add(wrapped)

    entry.subscribeRequests.set(key, { serviceName, method })
    entry.socket.send(this.encode("__subscribe", { serviceName, method }, "sub").payload)

    return {
      unsubscribe: async () => {
        bag!.delete(wrapped)
        if (bag!.size === 0) {
          entry.subscriptions.delete(key)
          entry.subscribeRequests.delete(key)
        }
      }
    }
  }

  getAvailableServices(): string[] {
    return this.serviceUrls.keys().toArray()
  }
  getDiscoveredServices() {
    this.discoveryRegistry.prune(this.discoveryTtlMs)
    return this.discoveryRegistry.list()
  }
  isServiceAvailable(name: string): boolean {
    return this.discoveryRegistry.isAvailable(name, this.discoveryTtlMs)
  }

  async close(timeoutMs = 30_000): Promise<void> {
    this.discoveryRegistry.stopBackgroundPrune()
    for (const [, entry] of this.sockets) {
      entry.closed = true
      for (const [, p] of entry.pending) {
        clearTimeout(p.timer)
        p.reject(new MessagingError(ErrorCode.CONNECTION_LOST, { message: "Client closed before reply" }))
      }
      entry.pending.clear()
      try {
        entry.socket?.close?.()
      } catch {}
    }
    this.sockets.clear()
    await this.runtime.close(timeoutMs)
  }
}
