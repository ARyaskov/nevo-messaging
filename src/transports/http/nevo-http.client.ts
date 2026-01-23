import {
  DEFAULT_BROADCAST_TOPIC,
  DEFAULT_DISCOVERY_TOPIC,
  DEFAULT_EVENTS_SUFFIX,
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

export interface NevoHttpClientOptions {
  timeoutMs?: number
  debug?: boolean
  serviceName?: string
  authToken?: string
  discoveryUrl?: string
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

export class NevoHttpClient {
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
  private readonly discoveryRegistry = new DiscoveryRegistry()
  private readonly discoveryEnabled: boolean
  private readonly discoveryHeartbeatIntervalMs: number
  private readonly discoveryTtlMs: number
  private readonly discoveryUrl?: string
  private discoveryTimer?: NodeJS.Timeout
  private discoveryAbort?: AbortController

  constructor(serviceUrls: Record<string, string>, options?: NevoHttpClientOptions) {
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
    this.discoveryEnabled = options?.discovery?.enabled === true && !!options?.discoveryUrl
    this.discoveryHeartbeatIntervalMs = options?.discovery?.heartbeatIntervalMs || 5000
    this.discoveryTtlMs = options?.discovery?.ttlMs || 15000
    this.discoveryUrl = options?.discoveryUrl

    if (this.discoveryEnabled) {
      void this.initDiscovery()
    }
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

  private getServiceUrl(serviceName: string): string {
    const normalized = serviceName.toLowerCase()
    const url = this.serviceUrls.get(normalized)
    if (!url) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Service "${serviceName}" is not registered in nevo http client`,
        availableServices: [...this.serviceUrls.keys()]
      })
    }
    return url.replace(/\/+$/, "")
  }

  private async postJson<T>(url: string, payload: any): Promise<T> {
    const fetchFn = (globalThis as any).fetch as any
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs)

    try {
      const response = await fetchFn(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: stringifyWithBigInt(payload),
        signal: controller.signal
      })

      const text = await response.text()
      if (!text) {
        return undefined as T
      }

      return parseWithBigInt(text) as T
    } catch (error: any) {
      if (error?.name === "AbortError") {
        throw new MessagingError(ErrorCode.UNKNOWN, {
          message: `HTTP request timed out after ${this.timeoutMs}ms`
        })
      }
      throw error
    } finally {
      clearTimeout(timeout)
    }
  }

  async query<T = any>(serviceName: string, method: string, params: any): Promise<T> {
    const url = this.getServiceUrl(serviceName)
    const endpoint = `${url}/${serviceName.toLowerCase()}${DEFAULT_EVENTS_SUFFIX}`
    const payload = this.createMessagePayload(method, params, "query")
    const inFlightKey = `${serviceName.toLowerCase()}:${method}`

    if (this.debug) {
      console.log(`[NevoHttpClient] Sending query to ${endpoint}:`, { method, params })
    }

    let inFlightAcquired = false
    await this.waitForInFlightSlot(inFlightKey)
    this.inFlight.add(inFlightKey)
    inFlightAcquired = true

    try {
      const response = await this.postJson<any>(endpoint, payload)
      if (response?.params?.result === "error" && response?.params?.error) {
        const errorData = response.params.error
        const error = new MessagingError(errorData.code, errorData.details, errorData.service || serviceName)
        if (process.env["MODE"] !== "production" && errorData.stack) {
          error.stack = errorData.stack
        }
        throw error
      }
      return response?.params?.result as T
    } finally {
      if (inFlightAcquired) {
        this.inFlight.delete(inFlightKey)
      }
    }
  }

  async emit(serviceName: string, method: string, params: any): Promise<void> {
    const url = this.getServiceUrl(serviceName)
    const endpoint = `${url}/${serviceName.toLowerCase()}${DEFAULT_EVENTS_SUFFIX}`
    const payload = this.createMessagePayload(method, params, "emit")

    if (this.debug) {
      console.log(`[NevoHttpClient] Emitting to ${endpoint}:`, { method, params })
    }

    await this.postJson(endpoint, payload)
  }

  async publish(serviceName: string, method: string, params: any): Promise<void> {
    const url = this.getServiceUrl(serviceName)
    const endpoint = `${url}/__nevo/publish`
    const payload = this.createMessagePayload(method, params, "sub")
    const body = {
      serviceName,
      ...payload
    }

    if (this.debug) {
      console.log(`[NevoHttpClient] Publishing to ${endpoint}:`, { method, params })
    }

    await this.postJson(endpoint, body)
  }

  async broadcast(method: string, params: any): Promise<void> {
    const url = this.discoveryUrl || [...this.serviceUrls.values()][0]
    if (!url) {
      throw new MessagingError(ErrorCode.UNKNOWN, { message: "No base URL available for broadcast" })
    }

    const endpoint = `${url.replace(/\/+$/, "")}/${DEFAULT_BROADCAST_TOPIC}`
    const payload = this.createMessagePayload(method, params, "broadcast")

    if (this.debug) {
      console.log(`[NevoHttpClient] Broadcasting to ${endpoint}:`, { method, params })
    }

    await this.postJson(endpoint, payload)
  }

  async subscribe<T = any>(
    serviceName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: (data: T, context: SubscriptionContext) => Promise<void> | void
  ): Promise<Subscription> {
    const normalized = serviceName.toLowerCase()
    const isBroadcast = normalized === DEFAULT_BROADCAST_TOPIC
    const baseUrl = isBroadcast ? this.discoveryUrl || [...this.serviceUrls.values()][0] : this.getServiceUrl(serviceName)
    if (!baseUrl) {
      throw new MessagingError(ErrorCode.UNKNOWN, { message: "No base URL available for subscription" })
    }
    const endpoint = isBroadcast
      ? `${baseUrl.replace(/\/+$/, "")}/${DEFAULT_BROADCAST_TOPIC}`
      : `${baseUrl.replace(/\/+$/, "")}/__nevo/subscribe?service=${encodeURIComponent(serviceName)}`
    const controller = new AbortController()

    const fetchFn = (globalThis as any).fetch as any
    const response = await fetchFn(endpoint, {
      headers: { Accept: "text/event-stream" },
      signal: controller.signal
    })

    if (!response.ok || !response.body) {
      throw new MessagingError(ErrorCode.UNKNOWN, { message: `Failed to subscribe via SSE: ${response.status}` })
    }

    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ""

    const readLoop = async () => {
      while (true) {
        const { value, done } = await reader.read()
        if (done) {
          break
        }
        buffer += decoder.decode(value)
        const parts = buffer.split("\n\n")
        buffer = parts.pop() || ""

        for (const part of parts) {
          const line = part.split("\n").find((l) => l.startsWith("data:"))
          if (!line) continue
          const raw = line.replace(/^data:\s*/, "")
          const payload = parseWithBigInt(raw)
          if (method && payload.method !== method) {
            continue
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
      }
    }

    void readLoop()

    return {
      unsubscribe: async () => {
        controller.abort()
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

  private async initDiscovery(): Promise<void> {
    if (!this.discoveryUrl) {
      return
    }

    this.discoveryAbort = new AbortController()

    const discoveryEndpoint = `${this.discoveryUrl.replace(/\/+$/, "")}/${DEFAULT_DISCOVERY_TOPIC}`
    const fetchFn = (globalThis as any).fetch as any
    const response = await fetchFn(discoveryEndpoint, {
      headers: { Accept: "text/event-stream" },
      signal: this.discoveryAbort.signal
    })

    if (response.ok && response.body) {
      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ""

      ;(async () => {
        while (true) {
          const { value, done } = await reader.read()
          if (done) {
            break
          }
          buffer += decoder.decode(value)
          const parts = buffer.split("\n\n")
          buffer = parts.pop() || ""

          for (const part of parts) {
            const line = part.split("\n").find((l) => l.startsWith("data:"))
            if (!line) continue
            const raw = line.replace(/^data:\s*/, "")
            const payload = parseWithBigInt(raw) as DiscoveryAnnouncement
            if (payload?.serviceName) {
              this.discoveryRegistry.update(payload)
            }
          }
        }
      })()
    }

    this.discoveryTimer = setInterval(() => {
      const announcement: DiscoveryAnnouncement = {
        serviceName: this.serviceName || "unknown",
        clientId: this.serviceName,
        transport: "http",
        ts: Date.now()
      }
      void this.postJson(discoveryEndpoint, announcement)
    }, this.discoveryHeartbeatIntervalMs)
  }
}
