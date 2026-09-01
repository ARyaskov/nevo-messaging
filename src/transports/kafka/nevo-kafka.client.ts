import { ClientKafka } from "@nestjs/microservices"
import type { OnModuleDestroy } from "@nestjs/common"
import { lastValueFrom, timeout, TimeoutError as RxTimeoutError } from "rxjs"
import { uuidv7 } from "../../common/uuid"
import type { Consumer, Producer, Kafka as KafkaType } from "kafkajs"
import {
  MessagingError,
  TimeoutError,
  ErrorCode,
  DEFAULT_BROADCAST_TOPIC,
  DEFAULT_DISCOVERY_TOPIC,
  DEFAULT_SUBSCRIPTION_SUFFIX,
  DiscoveryRegistry,
  MessageMeta,
  MessageType,
  Subscription,
  SubscriptionContext,
  SubscriptionOptions,
  DiscoveryAnnouncement,
  Codec,
  NevoLogger,
  ClientRuntime,
  type ClientCallOptions,
  type EncodedRequest,
  parseMethod,
  TransportClientOptions,
  matchesFilter,
  DlqRouter,
  normalizeServiceName,
  mapLimit
} from "../../common"
import { getKafkaModule } from "../optional-deps"

// Cap on concurrent async encode+compress operations during a batch emit.
const BATCH_ENCODE_CONCURRENCY = 16

export interface NevoKafkaClientOptions extends TransportClientOptions {
  timeoutMs?: number
  brokers?: string[]
  stickyRouter?: boolean
}

interface StickyHandlerEntry {
  method: string
  filter?: SubscriptionOptions["filter"]
  maxAttempts: number
  handler: (data: unknown, context: SubscriptionContext) => Promise<void> | void
}

interface StickyGroup {
  consumer: Consumer
  dispatcher: Map<string, Set<StickyHandlerEntry>>
  manualAck: boolean
  deliveryCounts: Map<string, number>
  running: boolean
  lifecycle: Promise<void>
}

// Defensive upper bound on a consumer's deliveryCounts map.
const MAX_DELIVERY_COUNTS = 10_000

interface KafkaEncodedRequest {
  key: string
  value: Uint8Array
  meta: MessageMeta
  uuid: string
  method: string
  encoding: string
}

export class NevoKafkaClient implements OnModuleDestroy {
  private readonly kafkaClient: ClientKafka
  private readonly serviceNames: string[]
  private readonly timeoutMs: number
  private readonly runtime: ClientRuntime
  private readonly serviceName?: string
  private readonly instanceId: string
  private readonly brokers: string[]
  private readonly logger: NevoLogger
  private readonly codec: Codec
  private readonly maxPayloadBytes: number
  private readonly discoveryRegistry = new DiscoveryRegistry()
  private readonly discoveryEnabled: boolean
  private readonly discoveryHeartbeatIntervalMs: number
  private readonly discoveryTtlMs: number
  private discoveryProducer?: Producer
  private batchProducer?: Producer
  private batchProducerPromise?: Promise<Producer>
  private discoveryConsumer?: Consumer
  private discoveryTimer?: NodeJS.Timeout
  private readonly subscriptionConsumers = new Set<Consumer>()
  private readonly stickyGroups = new Map<string, StickyGroup>()
  private readonly stickyGroupPromises = new Map<string, Promise<StickyGroup>>()
  private readonly enableStickyRouter: boolean
  private readonly sharedKafkaForSubs: KafkaType
  private readonly capabilities?: string[]
  private readonly host?: string
  private readonly port?: number
  private readonly version?: string
  private readonly dlq: DlqRouter

  constructor(kafkaClient: ClientKafka, serviceNames: string[], options?: NevoKafkaClientOptions) {
    this.kafkaClient = kafkaClient
    this.serviceNames = serviceNames.map((n) => n.toLowerCase())
    this.runtime = new ClientRuntime(options, { transport: "kafka" })
    this.timeoutMs = this.runtime.timeoutMs
    this.serviceName = this.runtime.serviceName
    this.instanceId = this.runtime.instanceId
    this.logger = this.runtime.logger
    this.codec = this.runtime.codec
    this.maxPayloadBytes = this.runtime.maxPayloadBytes
    this.brokers = options?.brokers && options.brokers.length > 0 ? options.brokers : ["127.0.0.1:9092"]
    // Opt-in: announcements go to a shared, unauthenticated topic.
    this.discoveryEnabled = options?.discovery?.enabled === true
    this.discoveryHeartbeatIntervalMs = options?.discovery?.heartbeatIntervalMs || 10000
    this.discoveryTtlMs = options?.discovery?.ttlMs || 30000
    this.capabilities = options?.discovery?.capabilities
    this.host = options?.discovery?.host
    this.port = options?.discovery?.port
    this.version = options?.discovery?.version
    this.dlq = new DlqRouter({ enabled: (options as any)?.dlq?.enabled === true })
    this.enableStickyRouter = (options as any)?.stickyRouter !== false

    const { Kafka } = getKafkaModule()
    this.sharedKafkaForSubs = new Kafka({
      clientId: `${this.serviceName || "nevo"}-shared`,
      brokers: this.brokers
    })

    this.serviceNames.forEach((serviceName) => {
      const topicName = `${serviceName}-events`
      this.kafkaClient.subscribeToResponseOf(topicName)
    })

    if (this.discoveryEnabled) {
      this.discoveryRegistry.startBackgroundPrune(this.discoveryTtlMs)
      void this.initDiscovery()
    }
  }

  getInstanceId(): string {
    return this.instanceId
  }

  private toKafkaRequest(request: EncodedRequest): KafkaEncodedRequest {
    return {
      key: request.uuid,
      value: request.payload,
      meta: request.meta,
      uuid: request.uuid,
      method: request.method,
      encoding: request.encoding
    }
  }

  private encodeRequestSync(method: string, params: unknown, type: MessageType, opts?: ClientCallOptions & { uuid?: string }): KafkaEncodedRequest {
    return this.toKafkaRequest(this.runtime.encodeSync(method, params, type, opts))
  }

  private async encodeRequestAsync(
    method: string,
    params: unknown,
    type: MessageType,
    opts?: ClientCallOptions & { uuid?: string }
  ): Promise<KafkaEncodedRequest> {
    return this.toKafkaRequest(await this.runtime.encodeAsync(method, params, type, opts))
  }

  private encodeRequest(
    method: string,
    params: unknown,
    type: MessageType,
    opts?: ClientCallOptions & { uuid?: string }
  ): KafkaEncodedRequest | Promise<KafkaEncodedRequest> {
    const encoded = this.runtime.encode(method, params, type, opts)
    return encoded instanceof Promise ? encoded.then((r) => this.toKafkaRequest(r)) : this.toKafkaRequest(encoded)
  }

  private toKafkaHeaders(encoding: string): Record<string, string> | undefined {
    return encoding !== "identity" ? { "content-encoding": encoding } : undefined
  }

  private decodePayload<T = any>(data: Uint8Array | Buffer | string, encoding?: string): T | Promise<T> {
    const buf = typeof data === "string" ? Buffer.from(data, "utf8") : Buffer.from(data as any)
    return this.runtime.decode<T>(buf, encoding)
  }

  private ensureServiceRegistered(serviceName: string): string {
    const normalized = normalizeServiceName(serviceName)
    if (!this.serviceNames.includes(normalized)) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, {
        message: `Service "${serviceName}" is not registered in nevo kafka client`,
        availableServices: this.serviceNames
      })
    }
    return normalized
  }

  async query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<T> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const topic = `${normalized}-events`
    return this.runtime.query<T>({
      serviceName: normalized,
      method,
      params,
      opts,
      mapError: (err) => (err instanceof RxTimeoutError ? new TimeoutError(serviceName, method, opts?.timeoutMs ?? this.timeoutMs) : err),
      send: async (request) => {
        const response: any = await lastValueFrom(
          this.kafkaClient
            .send<any>(topic, {
              key: request.uuid,
              value: Buffer.from(request.payload),
              headers: this.toKafkaHeaders(request.encoding)
            })
            .pipe(timeout(opts?.timeoutMs ?? this.timeoutMs))
        )
        return typeof response === "string" || response instanceof Uint8Array ? await this.decodePayload(response as any) : response
      }
    })
  }

  async emit(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const topic = `${normalized}-events`
    return this.runtime.emit({
      serviceName: normalized,
      method,
      params,
      opts,
      send: async (request) => {
        await lastValueFrom(
          this.kafkaClient.emit(topic, { key: request.uuid, value: Buffer.from(request.payload), headers: this.toKafkaHeaders(request.encoding) })
        )
      }
    })
  }

  private ensureBatchProducer(): Promise<Producer> {
    if (this.batchProducer) return Promise.resolve(this.batchProducer)
    if (!this.batchProducerPromise) {
      // Idempotent + single in-flight request for ordered, exactly-once appends.
      const producer = this.sharedKafkaForSubs.producer({ idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: true })
      this.batchProducerPromise = producer.connect().then(() => {
        this.batchProducer = producer
        return producer
      })
      this.batchProducerPromise.catch(() => {
        this.batchProducerPromise = undefined
      })
    }
    return this.batchProducerPromise
  }

  async emitBatch(
    items: Array<{
      serviceName: string
      method: string
      params: unknown
      opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string> }
    }>
  ): Promise<void> {
    if (items.length === 0) return
    const producer = await this.ensureBatchProducer()
    const byTopic = new Map<string, Array<{ key: string; value: Buffer; headers?: Record<string, string> }>>()
    if (this.runtime.compression.async && this.runtime.compression.enabled) {
      // Encode under a concurrency cap, then group in input order to keep per-topic order deterministic.
      const encoded = await mapLimit(items, BATCH_ENCODE_CONCURRENCY, async (item) => {
        const normalized = this.ensureServiceRegistered(item.serviceName)
        const topic = `${normalized}-events`
        const { key, value, encoding } = (await this.encodeRequest(item.method, item.params, "emit", item.opts)) as {
          key: string
          value: Uint8Array
          encoding: string
        }
        return { topic, key, value: Buffer.from(value), headers: this.toKafkaHeaders(encoding) }
      })
      for (const e of encoded) {
        let arr = byTopic.get(e.topic)
        if (!arr) {
          arr = []
          byTopic.set(e.topic, arr)
        }
        arr.push({ key: e.key, value: e.value, headers: e.headers })
      }
    } else {
      for (const item of items) {
        const normalized = this.ensureServiceRegistered(item.serviceName)
        const topic = `${normalized}-events`
        const enc = this.encodeRequestSync(item.method, item.params, "emit", item.opts)
        let arr = byTopic.get(topic)
        if (!arr) {
          arr = []
          byTopic.set(topic, arr)
        }
        arr.push({ key: enc.key, value: Buffer.from(enc.value), headers: this.toKafkaHeaders(enc.encoding) })
      }
    }
    const topicMessages: { topic: string; messages: { key: string; value: Buffer; headers?: Record<string, string> }[] }[] = []
    for (const [topic, messages] of byTopic.entries()) topicMessages.push({ topic, messages })
    await producer.sendBatch({ topicMessages, acks: -1 })
  }

  getAvailableServices(): string[] {
    return [...this.serviceNames]
  }

  async publish(serviceName: string, method: string, params: unknown, opts?: { version?: string; headers?: Record<string, string> }): Promise<void> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const topic = `${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    const { key, value, encoding } = await this.encodeRequest(method, params, "sub", opts)
    await lastValueFrom(this.kafkaClient.emit(topic, { key, value: Buffer.from(value), headers: this.toKafkaHeaders(encoding) }))
  }

  async broadcast(method: string, params: unknown, opts?: { version?: string; headers?: Record<string, string> }): Promise<void> {
    const { key, value, encoding } = await this.encodeRequest(method, params, "broadcast", opts)
    await lastValueFrom(this.kafkaClient.emit(DEFAULT_BROADCAST_TOPIC, { key, value: Buffer.from(value), headers: this.toKafkaHeaders(encoding) }))
  }

  // Evict oldest-first (insertion order) once the counter map exceeds its cap.
  private boundDeliveryCounts(counts: Map<string, number>): void {
    if (counts.size <= MAX_DELIVERY_COUNTS) return
    for (const key of counts.keys()) {
      counts.delete(key)
      if (counts.size <= MAX_DELIVERY_COUNTS) break
    }
  }

  private resumeBackoffMs(attempts: number): number {
    const exp = this.runtime.retryOptions.baseMs * Math.pow(2, Math.max(0, attempts - 1))
    return Math.min(this.runtime.retryOptions.maxMs, exp)
  }

  // Pause the partition and resume after a backoff so the failed message is redelivered.
  private scheduleResume(pause: () => () => void, attempts: number): void {
    let resume: () => void
    try {
      resume = pause()
    } catch {
      return
    }
    const timer = setTimeout(() => {
      try {
        resume()
      } catch {}
    }, this.resumeBackoffMs(attempts))
    if (typeof timer.unref === "function") timer.unref()
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

    const explicitGroupId = options?.groupId || (options?.durableKey ? `nevo-sub-${options.durableKey}` : undefined)
    const topic = isBroadcast ? DEFAULT_BROADCAST_TOPIC : `${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    const manualAck = options?.ack === true
    const maxAttempts = options?.maxDeliveryAttempts ?? 3

    if (this.enableStickyRouter && explicitGroupId) {
      return this.subscribeSticky(topic, method, explicitGroupId, options, manualAck, maxAttempts, handler as any)
    }

    const groupId = explicitGroupId || `nevo-sub-${this.serviceName || "client"}-${uuidv7()}`
    const consumer = this.sharedKafkaForSubs.consumer({ groupId, allowAutoTopicCreation: true })
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: options?.fromBeginning || false })

    const deliveryCounts = new Map<string, number>()

    await consumer.run({
      autoCommit: !manualAck,
      eachMessage: async ({ topic, partition, message, pause }) => {
        if (!message.value) return
        let payload: any
        try {
          const encoding = message.headers?.["content-encoding"]?.toString?.()
          payload = await this.decodePayload(message.value, encoding)
        } catch (err) {
          this.logger.error({ event: "kafka.parse_error", topic, err: (err as Error)?.message }, "Failed to parse subscription message")
          await this.dlq.route({ topic, reason: "parse-error", error: { message: (err as Error)?.message }, ts: Date.now() })
          return
        }
        if (method && payload.method !== method && parseMethod(payload.method ?? "").name !== method) return
        if (!matchesFilter(options?.filter, payload.meta)) return

        const msgKey = `${topic}:${partition}:${message.offset}`
        const attempts = (deliveryCounts.get(msgKey) ?? 0) + 1
        deliveryCounts.set(msgKey, attempts)
        this.boundDeliveryCounts(deliveryCounts)

        const context: SubscriptionContext = {
          meta: payload.meta || {},
          ack: async () => {
            if (!manualAck) return
            const nextOffset = (Number(message.offset) + 1).toString()
            await consumer.commitOffsets([{ topic, partition, offset: nextOffset }])
            deliveryCounts.delete(msgKey)
          },
          nack: async (reason) => {
            this.logger.warn({ event: "kafka.nack", topic, offset: message.offset, reason })
          }
        }

        try {
          await handler(payload.params as T, context)
          deliveryCounts.delete(msgKey)
        } catch (err) {
          this.logger.error({ event: "kafka.handler_error", topic, err: (err as Error)?.message }, "subscription handler failed")
          if (attempts >= maxAttempts) {
            await this.dlq.route({
              topic,
              reason: `delivery-exceeded:${attempts}`,
              error: { message: (err as Error)?.message },
              meta: payload.meta,
              rawPayload: payload,
              ts: Date.now()
            })
            deliveryCounts.delete(msgKey)
            if (manualAck) {
              const nextOffset = (Number(message.offset) + 1).toString()
              await consumer.commitOffsets([{ topic, partition, offset: nextOffset }])
            }
            return
          }
          if (manualAck) {
            this.scheduleResume(pause, attempts)
          } else {
            // Rethrow so kafkajs does not resolve the offset and redelivers the message.
            throw err
          }
        }
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

  private async subscribeSticky<T>(
    topic: string,
    method: string,
    groupId: string,
    options: SubscriptionOptions | undefined,
    manualAck: boolean,
    maxAttempts: number,
    handler: (data: T, context: SubscriptionContext) => Promise<void> | void
  ): Promise<Subscription> {
    const group = await this.ensureStickyGroup(groupId, manualAck)
    // The commit mode is fixed by the group's first subscriber; a later, differing
    // ack setting on the same groupId can't take effect on the shared consumer.
    if (group.manualAck !== manualAck) {
      this.logger.warn(
        { event: "kafka.sticky.ack_mismatch", groupId, groupManualAck: group.manualAck, requested: manualAck },
        `Sticky consumer group "${groupId}" already runs with ack=${group.manualAck}; this subscription's ack=${manualAck} is ignored. Use a distinct groupId for a different ack mode.`
      )
    }
    const entry: StickyHandlerEntry = {
      method,
      filter: options?.filter,
      maxAttempts,
      handler: handler as any
    }
    let topicEntries = group.dispatcher.get(topic)
    if (!topicEntries) {
      topicEntries = new Set()
      group.dispatcher.set(topic, topicEntries)
      try {
        await this.addStickyTopic(group, topic, options?.fromBeginning || false)
      } catch (err) {
        group.dispatcher.delete(topic)
        throw err
      }
    }
    topicEntries.add(entry)

    return {
      unsubscribe: async () => {
        topicEntries!.delete(entry)
        if (topicEntries!.size === 0) {
          group.dispatcher.delete(topic)
        }
        if (group.dispatcher.size === 0) {
          this.stickyGroups.delete(groupId)
          this.stickyGroupPromises.delete(groupId)
          this.subscriptionConsumers.delete(group.consumer)
          try {
            await group.consumer.disconnect()
          } catch {}
        }
      }
    }
  }

  private ensureStickyGroup(groupId: string, manualAck: boolean): Promise<StickyGroup> {
    const pending = this.stickyGroupPromises.get(groupId)
    if (pending) return pending
    const created = this.createStickyGroup(groupId, manualAck)
    this.stickyGroupPromises.set(groupId, created)
    created.catch(() => {
      this.stickyGroupPromises.delete(groupId)
    })
    return created
  }

  private async createStickyGroup(groupId: string, manualAck: boolean): Promise<StickyGroup> {
    const consumer = this.sharedKafkaForSubs.consumer({ groupId, allowAutoTopicCreation: true })
    await consumer.connect()
    const group: StickyGroup = {
      consumer,
      dispatcher: new Map<string, Set<StickyHandlerEntry>>(),
      manualAck,
      deliveryCounts: new Map<string, number>(),
      running: false,
      lifecycle: Promise.resolve()
    }
    this.stickyGroups.set(groupId, group)
    this.subscriptionConsumers.add(consumer)
    return group
  }

  private addStickyTopic(group: StickyGroup, topic: string, fromBeginning: boolean): Promise<void> {
    const op = group.lifecycle.then(async () => {
      if (group.running) {
        await group.consumer.stop()
        group.running = false
      }
      await group.consumer.subscribe({ topic, fromBeginning })
      await this.runStickyGroup(group)
      group.running = true
    })
    group.lifecycle = op.catch(() => {})
    return op
  }

  private async runStickyGroup(group: StickyGroup): Promise<void> {
    const { consumer, dispatcher, manualAck, deliveryCounts } = group
    await consumer.run({
      autoCommit: !manualAck,
      eachMessage: async ({ topic, partition, message, pause }) => {
        const entries = dispatcher.get(topic)
        if (!entries || entries.size === 0 || !message.value) return
        let payload: any
        try {
          const encoding = message.headers?.["content-encoding"]?.toString?.()
          payload = await this.decodePayload(message.value, encoding)
        } catch (err) {
          this.logger.error({ event: "kafka.parse_error", topic, err: (err as Error)?.message })
          await this.dlq.route({ topic, reason: "parse-error", error: { message: (err as Error)?.message }, ts: Date.now() })
          return
        }

        const msgKey = `${topic}:${partition}:${message.offset}`
        const attempts = (deliveryCounts.get(msgKey) ?? 0) + 1
        deliveryCounts.set(msgKey, attempts)
        this.boundDeliveryCounts(deliveryCounts)

        let retryScheduled = false
        let redeliveryError: unknown
        for (const entry of entries) {
          if (entry.method && payload.method !== entry.method && parseMethod(payload.method ?? "").name !== entry.method) continue
          if (!matchesFilter(entry.filter, payload.meta)) continue

          const context: SubscriptionContext = {
            meta: payload.meta || {},
            ack: async () => {
              if (!manualAck) return
              const nextOffset = (Number(message.offset) + 1).toString()
              await consumer.commitOffsets([{ topic, partition, offset: nextOffset }])
              deliveryCounts.delete(msgKey)
            },
            nack: async (reason) => {
              this.logger.warn({ event: "kafka.nack", topic, offset: message.offset, reason })
            }
          }

          try {
            await entry.handler(payload.params, context)
          } catch (err) {
            this.logger.error({ event: "kafka.handler_error", topic, err: (err as Error)?.message })
            if (attempts >= (entry.maxAttempts ?? 3)) {
              await this.dlq.route({
                topic,
                reason: `delivery-exceeded:${attempts}`,
                error: { message: (err as Error)?.message },
                meta: payload.meta,
                rawPayload: payload,
                ts: Date.now()
              })
            } else if (manualAck && !retryScheduled) {
              // Pause once for the whole batch of matching handlers.
              this.scheduleResume(pause, attempts)
              retryScheduled = true
            } else if (!manualAck) {
              redeliveryError = err
            }
          }
        }
        // Rethrow so kafkajs does not resolve the offset and redelivers the message.
        // Succeeded sibling handlers will run again; handlers are expected to be idempotent.
        if (redeliveryError !== undefined) throw redeliveryError
        // Drop the per-offset counter unless a redelivery is pending.
        if (!retryScheduled) deliveryCounts.delete(msgKey)
      }
    })
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
      const { Kafka } = getKafkaModule()
      const kafka = new Kafka({ clientId: `${this.serviceName || "nevo"}-discovery-${this.instanceId}`, brokers: this.brokers })
      this.discoveryProducer = kafka.producer()
      this.discoveryConsumer = kafka.consumer({ groupId: `${this.serviceName || "nevo"}-discovery-${this.instanceId}` })

      await this.discoveryProducer.connect()
      await this.discoveryConsumer.connect()
      await this.discoveryConsumer.subscribe({ topic: DEFAULT_DISCOVERY_TOPIC, fromBeginning: false })

      await this.discoveryConsumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) return
          try {
            const announcement = this.codec.decode<DiscoveryAnnouncement>(message.value)
            if (announcement?.serviceName) this.discoveryRegistry.update(announcement)
          } catch (err) {
            this.logger.error({ event: "discovery.parse_error", err: (err as Error)?.message })
          }
        }
      })

      this.discoveryTimer = setInterval(() => {
        const announcement: DiscoveryAnnouncement = {
          serviceName: this.serviceName || "unknown",
          instanceId: this.instanceId,
          clientId: this.serviceName,
          transport: "kafka",
          ts: Date.now(),
          host: this.host,
          port: this.port,
          version: this.version,
          capabilities: this.capabilities
        }
        void this.discoveryProducer?.send({
          topic: DEFAULT_DISCOVERY_TOPIC,
          messages: [{ key: announcement.instanceId, value: Buffer.from(this.codec.encode(announcement)) }]
        })
      }, this.discoveryHeartbeatIntervalMs)
      if (typeof this.discoveryTimer.unref === "function") this.discoveryTimer.unref()
    } catch (err) {
      this.logger.error({ event: "discovery.init_failed", err: (err as Error)?.message })
    }
  }

  onModuleDestroy(): Promise<void> {
    return this.close()
  }

  async close(timeoutMs = 30_000): Promise<void> {
    if (this.discoveryTimer) clearInterval(this.discoveryTimer)
    this.discoveryRegistry.stopBackgroundPrune()
    if (this.discoveryConsumer) {
      try {
        await this.discoveryConsumer.disconnect()
      } catch {}
    }
    if (this.discoveryProducer) {
      try {
        await this.discoveryProducer.disconnect()
      } catch {}
    }
    if (this.batchProducer) {
      try {
        await this.batchProducer.disconnect()
      } catch {}
    } else if (this.batchProducerPromise) {
      try {
        await (await this.batchProducerPromise).disconnect()
      } catch {}
    }
    this.stickyGroups.clear()
    this.stickyGroupPromises.clear()
    for (const c of this.subscriptionConsumers) {
      try {
        await c.disconnect()
      } catch {}
    }
    this.subscriptionConsumers.clear()
    await this.runtime.close(timeoutMs)
    try {
      await this.kafkaClient.close()
    } catch {}
  }
}
