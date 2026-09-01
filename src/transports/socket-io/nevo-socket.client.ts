import type { Socket } from "socket.io-client"
import {
  DEFAULT_BROADCAST_TOPIC,
  DEFAULT_DISCOVERY_TOPIC,
  DiscoveryRegistry,
  DiscoveryAnnouncement,
  MessagingError,
  TimeoutError,
  ErrorCode,
  Subscription,
  SubscriptionContext,
  SubscriptionOptions,
  TransportClientOptions,
  ClientRuntime,
  type ClientCallOptions,
  matchesFilter,
  normalizeServiceName,
  parseMethod
} from "../../common"
import { getSocketIoClientModule } from "../optional-deps"

export interface NevoSocketClientOptions extends TransportClientOptions {
  timeoutMs?: number
}

export class NevoSocketClient {
  private readonly runtime: ClientRuntime
  private readonly serviceUrls: Map<string, string>
  private readonly sockets = new Map<string, Socket>()
  private readonly activeSubscriptions = new Map<
    string,
    Map<string, { payload: { serviceName: string; method: string; room?: string }; count: number }>
  >()
  private readonly discoveryRegistry = new DiscoveryRegistry()
  private readonly discoveryEnabled: boolean
  private readonly discoveryTtlMs: number

  constructor(serviceUrls: Record<string, string>, options?: NevoSocketClientOptions) {
    this.serviceUrls = new Map(Object.entries(serviceUrls).map(([k, v]) => [k.toLowerCase(), v]))
    // socket.io serializes the envelope itself; a compressed body is unrecognisable to the peer.
    this.runtime = new ClientRuntime(options, { transport: "socketio", component: "socket-client", compressionCapable: false })
    this.discoveryEnabled = options?.discovery?.enabled === true
    this.discoveryTtlMs = options?.discovery?.ttlMs || 30000
    if (this.discoveryEnabled) this.discoveryRegistry.startBackgroundPrune(this.discoveryTtlMs)
  }

  getInstanceId(): string {
    return this.runtime.instanceId
  }

  private getSocket(serviceName: string): Socket {
    const normalized = normalizeServiceName(serviceName)
    const url = this.serviceUrls.get(normalized)
    if (!url) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, {
        message: `Service "${serviceName}" is not registered`,
        availableServices: this.serviceUrls.keys().toArray()
      })
    }
    let socket = this.sockets.get(normalized)
    if (!socket) {
      const { io } = getSocketIoClientModule()
      const created: Socket = io(url, { transports: ["websocket"] })
      socket = created
      this.sockets.set(normalized, created)
      created.on("connect", () => {
        const active = this.activeSubscriptions.get(normalized)
        if (!active) return
        for (const { payload } of active.values()) created.emit("nevo:subscribe", payload)
      })
      if (this.discoveryEnabled) {
        created.on(DEFAULT_DISCOVERY_TOPIC, (raw: any) => {
          try {
            const payload = typeof raw === "string" ? JSON.parse(raw) : raw
            if (payload?.serviceName) this.discoveryRegistry.update(payload as DiscoveryAnnouncement)
          } catch (err) {
            this.runtime.logger.error({ event: "socket.discovery.parse_error", err: (err as Error)?.message })
          }
        })
      }
    }
    return socket
  }

  async query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<T> {
    const normalized = normalizeServiceName(serviceName)
    return this.runtime.query<T>({
      serviceName: normalized,
      method,
      params,
      opts,
      send: (request) => {
        const socket = this.getSocket(serviceName)
        const timeout = opts?.timeoutMs ?? this.runtime.timeoutMs
        const { promise, resolve, reject } = Promise.withResolvers<unknown>()
        let settled = false
        socket.timeout(timeout).emit("nevo:query", request.envelope, (timeoutErr: any, response: any) => {
          if (settled) return
          settled = true
          if (timeoutErr) {
            reject(new TimeoutError(serviceName, method, timeout))
            return
          }
          resolve(response)
        })
        return promise
      }
    })
  }

  async emit(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    return this.dispatch(serviceName, method, params, "emit", "nevo:emit", opts)
  }

  async publish(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    return this.dispatch(serviceName, method, params, "sub", "nevo:publish", opts)
  }

  async broadcast(method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    const first = this.serviceUrls.keys().toArray()[0]
    if (!first) throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: "No base URL available for broadcast" })
    return this.runtime.emit({
      serviceName: DEFAULT_BROADCAST_TOPIC,
      method,
      params,
      opts,
      type: "broadcast",
      send: async (request) => {
        this.getSocket(first).emit("nevo:broadcast", request.envelope)
      }
    })
  }

  private async dispatch(
    serviceName: string,
    method: string,
    params: unknown,
    type: "emit" | "sub",
    event: string,
    opts?: ClientCallOptions
  ): Promise<void> {
    const normalized = normalizeServiceName(serviceName)
    return this.runtime.emit({
      serviceName: normalized,
      method,
      params,
      opts,
      type,
      send: async (request) => {
        this.getSocket(serviceName).emit(event, request.envelope)
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
    const socketKey = isBroadcast ? normalizeServiceName(this.serviceUrls.keys().toArray()[0]) : normalized
    const socket = this.getSocket(isBroadcast ? this.serviceUrls.keys().toArray()[0] : serviceName)
    const room = options?.room
    const subKey = `${normalized}:${method}:${room ?? ""}`

    if (!isBroadcast) {
      let active = this.activeSubscriptions.get(socketKey)
      if (!active) {
        active = new Map()
        this.activeSubscriptions.set(socketKey, active)
      }
      const tracked = active.get(subKey)
      if (tracked) tracked.count++
      else active.set(subKey, { payload: { serviceName, method, room }, count: 1 })
      socket.emit("nevo:subscribe", { serviceName, method, room })
    }

    const onMessage = async (raw: any) => {
      const payload: any = typeof raw === "string" ? JSON.parse(raw) : raw
      if (method && payload.method !== method && parseMethod(payload.method ?? "").name !== method) return
      if (!matchesFilter(options?.filter, payload.meta)) return

      const context: SubscriptionContext = {
        meta: payload.meta || {},
        ack: async () => {},
        nack: async () => {}
      }
      try {
        await handler(payload.params as T, context)
      } catch (err) {
        this.runtime.logger.error({ event: "socket.sub.handler_error", err: (err as Error)?.message })
      }
    }

    const event = isBroadcast ? "nevo:broadcast" : "nevo:sub"
    socket.on(event, onMessage)

    return {
      unsubscribe: async () => {
        socket.off(event, onMessage)
        if (!isBroadcast) {
          const active = this.activeSubscriptions.get(socketKey)
          const tracked = active?.get(subKey)
          if (tracked) {
            tracked.count--
            if (tracked.count > 0) return
            active!.delete(subKey)
            if (active!.size === 0) this.activeSubscriptions.delete(socketKey)
          }
          socket.emit("nevo:unsubscribe", { serviceName, method, room })
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
  isServiceAvailable(serviceName: string): boolean {
    return this.discoveryRegistry.isAvailable(serviceName, this.discoveryTtlMs)
  }

  async close(timeoutMs = 30_000): Promise<void> {
    this.discoveryRegistry.stopBackgroundPrune()
    for (const s of this.sockets.values()) {
      try {
        s.close()
      } catch {}
    }
    this.sockets.clear()
    await this.runtime.close(timeoutMs)
  }
}
