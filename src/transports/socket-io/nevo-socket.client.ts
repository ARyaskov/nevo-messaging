import type { Socket } from "socket.io-client"
import {
  DEFAULT_BROADCAST_TOPIC,
  DEFAULT_DISCOVERY_TOPIC,
  DEFAULT_SUBSCRIPTION_SUFFIX,
  DiscoveryRegistry,
  DiscoveryAnnouncement,
  MessageMeta,
  MessageType,
  MessagingError,
  ErrorCode,
  parseWithBigInt,
  stringifyWithBigInt,
  Subscription,
  SubscriptionContext,
  SubscriptionOptions
} from "../../common"
import { randomUUID } from "node:crypto"
import { getSocketIoClientModule } from "../optional-deps"

export interface NevoSocketClientOptions {
  timeoutMs?: number
  debug?: boolean
  serviceName?: string
  authToken?: string
  backoff?: {
    enabled?: boolean
    baseMs?: number
    maxMs?: number
    maxAttempts?: number
    jitter?: boolean
  }
  discovery?: {
    enabled?: boolean
    heartbeatIntervalMs?: number
    ttlMs?: number
  }
}

export class NevoSocketClient {
  private readonly serviceUrls: Map<string, string>
  private readonly timeoutMs: number
  private readonly debug: boolean
  private readonly serviceName?: string
  private readonly authToken?: string
  private readonly backoffEnabled: boolean
  private readonly backoffBaseMs: number
  private readonly backoffMaxMs: number
  private readonly backoffMaxAttempts: number
  private readonly backoffJitter: boolean
  private readonly inFlight = new Set<string>()
  private readonly sockets = new Map<string, Socket>()
  private readonly discoveryRegistry = new DiscoveryRegistry()
  private readonly discoveryEnabled: boolean
  private readonly discoveryHeartbeatIntervalMs: number
  private readonly discoveryTtlMs: number
  private discoveryTimer?: NodeJS.Timeout

  constructor(serviceUrls: Record<string, string>, options?: NevoSocketClientOptions) {
    this.serviceUrls = new Map(Object.entries(serviceUrls).map(([k, v]) => [k.toLowerCase(), v]))
    this.timeoutMs = options?.timeoutMs || 20000
    this.debug = options?.debug || false
    this.serviceName = options?.serviceName
    this.authToken = options?.authToken
    this.backoffEnabled = options?.backoff?.enabled !== false
    this.backoffBaseMs = options?.backoff?.baseMs || 100
    this.backoffMaxMs = options?.backoff?.maxMs || 2000
    this.backoffMaxAttempts = options?.backoff?.maxAttempts || 0
    this.backoffJitter = options?.backoff?.jitter !== false
    this.discoveryEnabled = options?.discovery?.enabled === true
    this.discoveryHeartbeatIntervalMs = options?.discovery?.heartbeatIntervalMs || 5000
    this.discoveryTtlMs = options?.discovery?.ttlMs || 15000
  }

  private createMessagePayload(method: string, params: any, type: MessageType) {
    const uuid = randomUUID()
    const meta: MessageMeta = {
      type,
      service: this.serviceName,
      ts: Date.now(),
      auth: { token: this.authToken }
    }
    return { uuid, method, params, meta }
  }

  private async waitForInFlightSlot(key: string): Promise<void> {
    if (!this.backoffEnabled) {
      return
    }

    let attempt = 0
    let delay = this.backoffBaseMs

    while (this.inFlight.has(key)) {
      attempt++
      if (this.backoffMaxAttempts > 0 && attempt > this.backoffMaxAttempts) {
        throw new MessagingError(ErrorCode.UNKNOWN, {
          message: `Backoff exceeded for ${key}`
        })
      }

      const jitter = this.backoffJitter ? Math.floor(Math.random() * delay * 0.2) : 0
      await new Promise((resolve) => setTimeout(resolve, delay + jitter))
      delay = Math.min(this.backoffMaxMs, delay * 2)
    }
  }

  private getSocket(serviceName: string): Socket {
    const normalized = serviceName.toLowerCase()
    const url = this.serviceUrls.get(normalized)
    if (!url) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Service "${serviceName}" is not registered in nevo socket client`,
        availableServices: [...this.serviceUrls.keys()]
      })
    }

    let socket = this.sockets.get(normalized)
    if (!socket) {
      const { io } = getSocketIoClientModule()
      socket = io(url, { transports: ["websocket"] })
      this.sockets.set(normalized, socket)

      if (this.discoveryEnabled) {
        socket.on(DEFAULT_DISCOVERY_TOPIC, (raw: string) => {
          try {
            const payload = parseWithBigInt(raw) as DiscoveryAnnouncement
            if (payload?.serviceName) {
              this.discoveryRegistry.update(payload)
            }
          } catch (error) {
            console.error("[NevoSocketClient] Failed to parse discovery message", error)
          }
        })
      }
    }

    return socket
  }

  async query<T = any>(serviceName: string, method: string, params: any): Promise<T> {
    const socket = this.getSocket(serviceName)
    const payload = this.createMessagePayload(method, params, "query")
    const inFlightKey = `${serviceName.toLowerCase()}:${method}`

    if (this.debug) {
      console.log(`[NevoSocketClient] Sending query to ${serviceName}:`, { method, params })
    }

    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(
          new MessagingError(ErrorCode.UNKNOWN, {
            message: `Socket request timed out after ${this.timeoutMs}ms`
          })
        )
      }, this.timeoutMs)

      ;(async () => {
        let inFlightAcquired = false
        try {
          await this.waitForInFlightSlot(inFlightKey)
          this.inFlight.add(inFlightKey)
          inFlightAcquired = true

          socket.emit("nevo:query", payload, (response: any) => {
            clearTimeout(timeout)
            if (inFlightAcquired) {
              this.inFlight.delete(inFlightKey)
            }
            if (response?.params?.result === "error" && response?.params?.error) {
              const errorData = response.params.error
              const error = new MessagingError(errorData.code, errorData.details, errorData.service || serviceName)
              if (process.env["MODE"] !== "production" && errorData.stack) {
                error.stack = errorData.stack
              }
              reject(error)
              return
            }
            resolve(response?.params?.result as T)
          })
        } catch (error) {
          clearTimeout(timeout)
          if (inFlightAcquired) {
            this.inFlight.delete(inFlightKey)
          }
          reject(error)
        }
      })()
    })
  }

  async emit(serviceName: string, method: string, params: any): Promise<void> {
    const socket = this.getSocket(serviceName)
    const payload = this.createMessagePayload(method, params, "emit")
    if (this.debug) {
      console.log(`[NevoSocketClient] Emitting to ${serviceName}:`, { method, params })
    }
    socket.emit("nevo:emit", payload)
  }

  async publish(serviceName: string, method: string, params: any): Promise<void> {
    const socket = this.getSocket(serviceName)
    const payload = this.createMessagePayload(method, params, "sub")
    if (this.debug) {
      console.log(`[NevoSocketClient] Publishing to ${serviceName}:`, { method, params })
    }
    socket.emit("nevo:publish", payload)
  }

  async broadcast(method: string, params: any): Promise<void> {
    const first = [...this.serviceUrls.keys()][0]
    if (!first) {
      throw new MessagingError(ErrorCode.UNKNOWN, { message: "No base URL available for broadcast" })
    }
    const socket = this.getSocket(first)
    const payload = this.createMessagePayload(method, params, "broadcast")
    if (this.debug) {
      console.log(`[NevoSocketClient] Broadcasting:`, { method, params })
    }
    socket.emit("nevo:broadcast", payload)
  }

  async subscribe<T = any>(
    serviceName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: (data: T, context: SubscriptionContext) => Promise<void> | void
  ): Promise<Subscription> {
    const normalized = serviceName.toLowerCase()
    const isBroadcast = normalized === DEFAULT_BROADCAST_TOPIC
    const socket = this.getSocket(isBroadcast ? [...this.serviceUrls.keys()][0] : serviceName)
    const room = method ? `${normalized}:${method}` : `${normalized}`

    if (!isBroadcast) {
      socket.emit("nevo:subscribe", { serviceName, method, room })
    }

    const onMessage = async (raw: any) => {
      const payload = typeof raw === "string" ? parseWithBigInt(raw) : raw
      if (method && payload.method !== method) {
        return
      }

      const context: SubscriptionContext = {
        meta: payload.meta || {},
        ack: async () => {
          return
        },
        nack: async () => {
          return
        }
      }

      await handler(payload.params as T, context)
    }

    socket.on(isBroadcast ? "nevo:broadcast" : "nevo:sub", onMessage)

    return {
      unsubscribe: async () => {
        socket.off(isBroadcast ? "nevo:broadcast" : "nevo:sub", onMessage)
        if (!isBroadcast) {
          socket.emit("nevo:unsubscribe", { serviceName, method, room })
        }
      }
    }
  }

  getAvailableServices(): string[] {
    return [...this.serviceUrls.keys()]
  }

  getDiscoveredServices() {
    this.discoveryRegistry.prune(this.discoveryTtlMs)
    return this.discoveryRegistry.list()
  }

  isServiceAvailable(serviceName: string): boolean {
    return this.discoveryRegistry.isAvailable(serviceName, this.discoveryTtlMs)
  }
}
