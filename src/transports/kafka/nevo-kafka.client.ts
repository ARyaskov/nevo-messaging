import { ClientKafka } from "@nestjs/microservices"
import { lastValueFrom, timeout, TimeoutError } from "rxjs"
import {
  MessagingError,
  ErrorCode,
  stringifyWithBigInt,
  parseWithBigInt,
  DEFAULT_BROADCAST_TOPIC,
  DEFAULT_DISCOVERY_TOPIC,
  DEFAULT_SUBSCRIPTION_SUFFIX,
  DiscoveryRegistry,
  MessageMeta,
  MessageType,
  Subscription,
  SubscriptionContext,
  SubscriptionOptions,
  DiscoveryAnnouncement
} from "../../common"
import { randomUUID } from "node:crypto"
import { Kafka, Consumer, Producer } from "kafkajs"

export interface NevoKafkaClientOptions {
  timeoutMs?: number
  debug?: boolean
  serviceName?: string
  authToken?: string
  brokers?: string[]
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

export class NevoKafkaClient {
  private readonly kafkaClient: ClientKafka
  private readonly serviceNames: string[]
  private readonly timeoutMs: number
  private readonly debug: boolean
  private readonly serviceName?: string
  private readonly authToken?: string
  private readonly brokers: string[]
  private readonly backoffEnabled: boolean
  private readonly backoffBaseMs: number
  private readonly backoffMaxMs: number
  private readonly backoffMaxAttempts: number
  private readonly backoffJitter: boolean
  private readonly inFlight = new Set<string>()
  private readonly discoveryTopic: string = DEFAULT_DISCOVERY_TOPIC
  private readonly broadcastTopic: string = DEFAULT_BROADCAST_TOPIC
  private readonly discoveryRegistry = new DiscoveryRegistry()
  private readonly discoveryEnabled: boolean
  private readonly discoveryHeartbeatIntervalMs: number
  private readonly discoveryTtlMs: number
  private discoveryProducer?: Producer
  private discoveryConsumer?: Consumer
  private discoveryTimer?: NodeJS.Timeout
  private readonly subscriptionConsumers = new Set<Consumer>()

  constructor(kafkaClient: ClientKafka, serviceNames: string[], options?: NevoKafkaClientOptions) {
    this.kafkaClient = kafkaClient
    this.serviceNames = serviceNames.map((name) => name.toLowerCase())
    this.timeoutMs = options?.timeoutMs || 20000
    this.debug = options?.debug || false
    this.serviceName = options?.serviceName
    this.authToken = options?.authToken
    this.brokers = options?.brokers && options.brokers.length > 0 ? options.brokers : ["127.0.0.1:9092"]
    this.backoffEnabled = options?.backoff?.enabled !== false
    this.backoffBaseMs = options?.backoff?.baseMs || 100
    this.backoffMaxMs = options?.backoff?.maxMs || 2000
    this.backoffMaxAttempts = options?.backoff?.maxAttempts || 0
    this.backoffJitter = options?.backoff?.jitter !== false
    this.discoveryEnabled = options?.discovery?.enabled !== false
    this.discoveryHeartbeatIntervalMs = options?.discovery?.heartbeatIntervalMs || 5000
    this.discoveryTtlMs = options?.discovery?.ttlMs || 15000

    this.serviceNames.forEach((serviceName) => {
      const topicName = `${serviceName}-events`
      const replyTopicName = `${topicName}.reply`

      this.kafkaClient.subscribeToResponseOf(topicName)
      this.kafkaClient.subscribeToResponseOf(replyTopicName)
    })

    if (this.discoveryEnabled) {
      void this.initDiscovery()
    }
  }

  private createMessagePayload(method: string, params: any, type: MessageType): { key: string; value: string } {
    const uuid = randomUUID()
    const meta: MessageMeta = {
      type,
      service: this.serviceName,
      ts: Date.now(),
      auth: { token: this.authToken }
    }
    return {
      key: uuid,
      value: stringifyWithBigInt({ uuid, method, params, meta })
    }
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
        message: `Service "${serviceName}" is not registered in nevo kafka client`,
        availableServices: this.serviceNames
      })
    }

    const topicName = `${normalizedServiceName}-events`
    const payload = this.createMessagePayload(method, params, "query")
    const inFlightKey = `${normalizedServiceName}:${method}`

    if (this.debug) {
      console.log(`[NevoKafkaClient] Sending query to ${topicName}:`, { method, params })
    }

    let inFlightAcquired = false
    try {
      await this.waitForInFlightSlot(inFlightKey)
      this.inFlight.add(inFlightKey)
      inFlightAcquired = true
      const response = await lastValueFrom(this.kafkaClient.send<any>(topicName, payload).pipe(timeout(this.timeoutMs)))

      if (response?.params?.result === "error" && response?.params?.error) {
        const errorData = response.params.error
        const error = new MessagingError(errorData.code, errorData.details, errorData.service || serviceName)

        if (process.env["MODE"] !== "production" && errorData.stack) {
          error.stack = errorData.stack
        }

        throw error
      }

      return response?.params?.result as T
    } catch (error) {
      if (error instanceof TimeoutError) {
        console.error(`Kafka request timed out after ${this.timeoutMs}ms`)
        throw new MessagingError(ErrorCode.UNKNOWN, {
          message: `Request to ${serviceName}.${method} timed out after ${this.timeoutMs}ms`
        })
      }
      throw error
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
        message: `Service "${serviceName}" is not registered in nevo kafka client`,
        availableServices: this.serviceNames
      })
    }

    const topicName = `${normalizedServiceName}-events`
    const payload = this.createMessagePayload(method, params, "emit")

    if (this.debug) {
      console.log(`[NevoKafkaClient] Emitting to ${topicName}:`, { method, params })
    }

    try {
      this.kafkaClient.emit(topicName, payload)
    } catch (error) {
      console.error(`Failed to emit event to ${serviceName}.${method}:`, error)
      throw error
    }
  }

  getAvailableServices(): string[] {
    return [...this.serviceNames]
  }

  async publish(serviceName: string, method: string, params: any): Promise<void> {
    const normalizedServiceName = serviceName.toLowerCase()

    if (!this.serviceNames.includes(normalizedServiceName)) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Service "${serviceName}" is not registered in nevo kafka client`,
        availableServices: this.serviceNames
      })
    }

    const topicName = `${normalizedServiceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    const payload = this.createMessagePayload(method, params, "sub")

    if (this.debug) {
      console.log(`[NevoKafkaClient] Publishing to ${topicName}:`, { method, params })
    }

    try {
      this.kafkaClient.emit(topicName, payload)
    } catch (error) {
      console.error(`Failed to publish event to ${serviceName}.${method}:`, error)
      throw error
    }
  }

  async broadcast(method: string, params: any): Promise<void> {
    const payload = this.createMessagePayload(method, params, "broadcast")

    if (this.debug) {
      console.log(`[NevoKafkaClient] Broadcasting to ${this.broadcastTopic}:`, { method, params })
    }

    try {
      this.kafkaClient.emit(this.broadcastTopic, payload)
    } catch (error) {
      console.error(`Failed to broadcast ${method}:`, error)
      throw error
    }
  }

  async subscribe<T = any>(
    serviceName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: (data: T, context: SubscriptionContext) => Promise<void> | void
  ): Promise<Subscription> {
    const normalizedServiceName = serviceName.toLowerCase()
    const isBroadcast = normalizedServiceName === this.broadcastTopic

    if (!isBroadcast && !this.serviceNames.includes(normalizedServiceName)) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Service "${serviceName}" is not registered in nevo kafka client`,
        availableServices: this.serviceNames
      })
    }

    const kafka = new Kafka({
      clientId: `${this.serviceName || "nevo"}-sub-${randomUUID()}`,
      brokers: this.brokers
    })

    const groupId =
      options?.groupId || (options?.durableKey ? `nevo-sub-${options.durableKey}` : `nevo-sub-${this.serviceName || "client"}-${randomUUID()}`)

    const consumer = kafka.consumer({
      groupId,
      allowAutoTopicCreation: true
    })

    await consumer.connect()

    const topic = isBroadcast ? this.broadcastTopic : `${normalizedServiceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    await consumer.subscribe({ topic, fromBeginning: options?.fromBeginning || false })

    const manualAck = options?.ack === true

    await consumer.run({
      autoCommit: !manualAck,
      eachMessage: async ({ topic, partition, message }) => {
        const raw = message.value?.toString()
        if (!raw) {
          return
        }

        let payload: any
        try {
          payload = parseWithBigInt(raw)
        } catch (error) {
          console.error(`[NevoKafkaClient] Failed to parse subscription message`, error)
          return
        }

        if (method && payload.method !== method) {
          return
        }

        const context: SubscriptionContext = {
          meta: payload.meta || {},
          ack: async () => {
            if (!manualAck) {
              return
            }
            const nextOffset = (Number(message.offset) + 1).toString()
            await consumer.commitOffsets([{ topic, partition, offset: nextOffset }])
          },
          nack: async () => {
            return
          }
        }

        await handler(payload.params as T, context)
      }
    })

    this.subscriptionConsumers.add(consumer)

    return {
      unsubscribe: async () => {
        this.subscriptionConsumers.delete(consumer)
        await consumer.disconnect()
      }
    }
  }

  getDiscoveredServices() {
    this.discoveryRegistry.prune(this.discoveryTtlMs)
    return this.discoveryRegistry.list()
  }

  isServiceAvailable(serviceName: string): boolean {
    return this.discoveryRegistry.isAvailable(serviceName, this.discoveryTtlMs)
  }

  private async initDiscovery(): Promise<void> {
    try {
      const kafka = new Kafka({
        clientId: `${this.serviceName || "nevo"}-discovery`,
        brokers: this.brokers
      })

      this.discoveryProducer = kafka.producer()
      this.discoveryConsumer = kafka.consumer({
        groupId: `${this.serviceName || "nevo"}-discovery-${randomUUID()}`
      })

      await this.discoveryProducer.connect()
      await this.discoveryConsumer.connect()
      await this.discoveryConsumer.subscribe({ topic: this.discoveryTopic, fromBeginning: false })

      await this.discoveryConsumer.run({
        eachMessage: async ({ message }) => {
          const raw = message.value?.toString()
          if (!raw) {
            return
          }

          try {
            const announcement = parseWithBigInt(raw) as DiscoveryAnnouncement
            if (announcement?.serviceName) {
              this.discoveryRegistry.update(announcement)
            }
          } catch (error) {
            console.error("[NevoKafkaClient] Failed to parse discovery message", error)
          }
        }
      })

      this.discoveryTimer = setInterval(() => {
        const announcement: DiscoveryAnnouncement = {
          serviceName: this.serviceName || "unknown",
          clientId: this.serviceName,
          transport: "kafka",
          ts: Date.now()
        }
        void this.discoveryProducer?.send({
          topic: this.discoveryTopic,
          messages: [{ key: announcement.serviceName, value: stringifyWithBigInt(announcement) }]
        })
      }, this.discoveryHeartbeatIntervalMs)
    } catch (error) {
      console.error("[NevoKafkaClient] Discovery initialization failed:", error)
    }
  }
}
