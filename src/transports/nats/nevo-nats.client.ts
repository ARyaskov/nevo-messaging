import type { NatsConnection, Subscription as NatsSubscription } from "nats"
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
import { getNatsModule } from "../optional-deps"

export interface NevoNatsClientOptions {
  servers?: string[]
  timeoutMs?: number
  debug?: boolean
  serviceName?: string
  authToken?: string
  reconnect?: {
    enabled?: boolean
    maxAttempts?: number
    timeWaitMs?: number
    jitterMs?: number
    jitterTlsMs?: number
    waitOnFirstConnect?: boolean
    lazyConnect?: boolean
  }
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

export class NevoNatsClient {
  private readonly nc: NatsConnection
  private readonly codec: { encode: (input: string) => Uint8Array; decode: (input: Uint8Array) => string }
  private readonly serviceNames: string[]
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
  private discoveryTimer?: NodeJS.Timeout
  private discoverySubscription?: NatsSubscription

  constructor(nc: NatsConnection, serviceNames: string[], options?: NevoNatsClientOptions) {
    const { StringCodec } = getNatsModule()
    this.nc = nc
    this.serviceNames = serviceNames.map((name) => name.toLowerCase())
    this.timeoutMs = options?.timeoutMs || 20000
    this.debug = options?.debug || false
    this.serviceName = options?.serviceName
    this.authToken = options?.authToken
    this.codec = StringCodec()
    this.backoffEnabled = options?.backoff?.enabled !== false
    this.backoffBaseMs = options?.backoff?.baseMs || 100
    this.backoffMaxMs = options?.backoff?.maxMs || 2000
    this.backoffMaxAttempts = options?.backoff?.maxAttempts || 0
    this.backoffJitter = options?.backoff?.jitter !== false
    this.discoveryEnabled = options?.discovery?.enabled !== false
    this.discoveryHeartbeatIntervalMs = options?.discovery?.heartbeatIntervalMs || 5000
    this.discoveryTtlMs = options?.discovery?.ttlMs || 15000

    if (this.discoveryEnabled) {
      void this.initDiscovery()
    }
  }

  static async create(serviceNames: string[], options?: NevoNatsClientOptions): Promise<NevoNatsClient> {
    const { connect } = getNatsModule()
    const reconnectEnabled = options?.reconnect?.enabled !== false
    const maxAttempts = options?.reconnect?.maxAttempts ?? -1
    const timeWaitMs = options?.reconnect?.timeWaitMs ?? 5000
    const jitterMs = options?.reconnect?.jitterMs
    const jitterTlsMs = options?.reconnect?.jitterTlsMs
    const lazyConnect = options?.reconnect?.lazyConnect === true
    const waitOnFirstConnect = options?.reconnect?.waitOnFirstConnect ?? !lazyConnect
    const nc = await connect({
      servers: options?.servers && options.servers.length > 0 ? options.servers : ["nats://127.0.0.1:4222"],
      maxReconnectAttempts: reconnectEnabled ? maxAttempts : 0,
      reconnectTimeWait: timeWaitMs,
      reconnectJitter: jitterMs,
      reconnectJitterTLS: jitterTlsMs,
      waitOnFirstConnect
    })
    return new NevoNatsClient(nc, serviceNames, options)
  }

  private createMessagePayload(method: string, params: any, type: MessageType): string {
    const uuid = randomUUID()
    const meta: MessageMeta = {
      type,
      service: this.serviceName,
      ts: Date.now(),
      auth: { token: this.authToken }
    }
    return stringifyWithBigInt({ uuid, method, params, meta })
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

  async query<T = any>(serviceName: string, method: string, params: any): Promise<T> {
    const normalizedServiceName = serviceName.toLowerCase()

    if (!this.serviceNames.includes(normalizedServiceName)) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Service "${serviceName}" is not registered in nevo nats client`,
        availableServices: this.serviceNames
      })
    }

    const subject = `${normalizedServiceName}-events`
    const payload = this.createMessagePayload(method, params, "query")
    const inFlightKey = `${normalizedServiceName}:${method}`

    if (this.debug) {
      console.log(`[NevoNatsClient] Sending query to ${subject}:`, { method, params })
    }

    let inFlightAcquired = false
    await this.waitForInFlightSlot(inFlightKey)
    this.inFlight.add(inFlightKey)
    inFlightAcquired = true

    try {
      const msg = await this.nc.request(subject, this.codec.encode(payload), { timeout: this.timeoutMs })
      const response = parseWithBigInt(this.codec.decode(msg.data))

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
    const normalizedServiceName = serviceName.toLowerCase()

    if (!this.serviceNames.includes(normalizedServiceName)) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Service "${serviceName}" is not registered in nevo nats client`,
        availableServices: this.serviceNames
      })
    }

    const subject = `${normalizedServiceName}-events`
    const payload = this.createMessagePayload(method, params, "emit")

    if (this.debug) {
      console.log(`[NevoNatsClient] Emitting to ${subject}:`, { method, params })
    }

    this.nc.publish(subject, this.codec.encode(payload))
  }

  async publish(serviceName: string, method: string, params: any): Promise<void> {
    const normalizedServiceName = serviceName.toLowerCase()

    if (!this.serviceNames.includes(normalizedServiceName)) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Service "${serviceName}" is not registered in nevo nats client`,
        availableServices: this.serviceNames
      })
    }

    const subject = `${normalizedServiceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    const payload = this.createMessagePayload(method, params, "sub")

    if (this.debug) {
      console.log(`[NevoNatsClient] Publishing to ${subject}:`, { method, params })
    }

    this.nc.publish(subject, this.codec.encode(payload))
  }

  async broadcast(method: string, params: any): Promise<void> {
    const payload = this.createMessagePayload(method, params, "broadcast")
    if (this.debug) {
      console.log(`[NevoNatsClient] Broadcasting to ${DEFAULT_BROADCAST_TOPIC}:`, { method, params })
    }
    this.nc.publish(DEFAULT_BROADCAST_TOPIC, this.codec.encode(payload))
  }

  async subscribe<T = any>(
    serviceName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: (data: T, context: SubscriptionContext) => Promise<void> | void
  ): Promise<Subscription> {
    const normalizedServiceName = serviceName.toLowerCase()
    const isBroadcast = normalizedServiceName === DEFAULT_BROADCAST_TOPIC

    if (!isBroadcast && !this.serviceNames.includes(normalizedServiceName)) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Service "${serviceName}" is not registered in nevo nats client`,
        availableServices: this.serviceNames
      })
    }

    const subject = isBroadcast ? DEFAULT_BROADCAST_TOPIC : `${normalizedServiceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    const sub = this.nc.subscribe(subject)

    ;(async () => {
      for await (const msg of sub) {
        const raw = this.codec.decode(msg.data)
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
    })()

    return {
      unsubscribe: async () => {
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

  private async initDiscovery(): Promise<void> {
    this.discoverySubscription = this.nc.subscribe(DEFAULT_DISCOVERY_TOPIC)
    ;(async () => {
      for await (const msg of this.discoverySubscription!) {
        try {
          const payload = parseWithBigInt(this.codec.decode(msg.data)) as DiscoveryAnnouncement
          if (payload?.serviceName) {
            this.discoveryRegistry.update(payload)
          }
        } catch (error) {
          console.error("[NevoNatsClient] Failed to parse discovery message", error)
        }
      }
    })()

    this.discoveryTimer = setInterval(() => {
      const announcement: DiscoveryAnnouncement = {
        serviceName: this.serviceName || "unknown",
        clientId: this.serviceName,
        transport: "nats",
        ts: Date.now()
      }
      try {
        this.nc.publish(DEFAULT_DISCOVERY_TOPIC, this.codec.encode(stringifyWithBigInt(announcement)))
      } catch (error) {
        console.error("[NevoNatsClient] Discovery publish failed", error)
      }
    }, this.discoveryHeartbeatIntervalMs)
  }
}
