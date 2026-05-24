import { ClientKafka } from "@nestjs/microservices"
import { lastValueFrom, timeout, TimeoutError as RxTimeoutError } from "rxjs"
import { randomUUID } from "node:crypto"
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
  maybeCompress,
  maybeCompressAsync,
  maybeDecompress,
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
  DlqRouter,
  DevToolsBus,
  getDevToolsBus,
  publishClientEvent,
  normalizeServiceName,
  resolveOutboundChainId
} from "../../common"
import { getKafkaModule } from "../optional-deps"

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
}

export class NevoKafkaClient {
  private readonly kafkaClient: ClientKafka
  private readonly serviceNames: string[]
  private readonly timeoutMs: number
  private readonly debug: boolean
  private readonly serviceName?: string
  private readonly instanceId: string
  private readonly authToken?: string
  private readonly brokers: string[]
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
  private readonly discoveryRegistry = new DiscoveryRegistry()
  private readonly discoveryEnabled: boolean
  private readonly discoveryHeartbeatIntervalMs: number
  private readonly discoveryTtlMs: number
  private discoveryProducer?: Producer
  private batchProducer?: Producer
  private discoveryConsumer?: Consumer
  private discoveryTimer?: NodeJS.Timeout
  private readonly subscriptionConsumers = new Set<Consumer>()
  private readonly stickyGroups = new Map<string, StickyGroup>()
  private readonly enableStickyRouter: boolean
  private readonly sharedKafkaForSubs: KafkaType
  private readonly capabilities?: string[]
  private readonly host?: string
  private readonly port?: number
  private readonly version?: string
  private readonly dlq: DlqRouter
  private readonly devtoolsBus: DevToolsBus | null
  private readonly metaStaticPart: Pick<MessageMeta, "service" | "instanceId" | "auth" | "codec">

  constructor(kafkaClient: ClientKafka, serviceNames: string[], options?: NevoKafkaClientOptions) {
    this.kafkaClient = kafkaClient
    this.serviceNames = serviceNames.map((n) => n.toLowerCase())
    this.timeoutMs = options?.timeoutMs ?? options?.timeout ?? 20000
    this.debug = options?.debug || false
    this.serviceName = options?.serviceName
    this.instanceId = options?.instanceId || randomUUID()
    this.authToken = options?.authToken
    this.brokers = options?.brokers && options.brokers.length > 0 ? options.brokers : ["127.0.0.1:9092"]
    this.logger = (options?.logger as NevoLogger) || getDefaultLogger().child({ component: "kafka-client", service: this.serviceName })
    this.codec = typeof options?.codec === "string" ? getCodec(options.codec) : (options?.codec as Codec) || getDefaultCodec()
    this.circuitBreaker = new CircuitBreakerRegistry(options?.circuitBreaker)
    this.retryOptions = resolveRetryOptions(options?.retry)
    this.compression = resolveCompressionOptions(options?.compression)
    this.tracer = getDefaultTracer()
    this.metrics = getDefaultMetrics()
    this.maxPayloadBytes = options?.security?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
    this.idempotencyCache = new LruIdempotencyCache<unknown>(options?.idempotency)
    this.discoveryEnabled = options?.discovery?.enabled !== false
    this.discoveryHeartbeatIntervalMs = options?.discovery?.heartbeatIntervalMs || 10000
    this.discoveryTtlMs = options?.discovery?.ttlMs || 30000
    this.capabilities = options?.discovery?.capabilities
    this.host = options?.discovery?.host
    this.port = options?.discovery?.port
    this.version = options?.discovery?.version
    this.dlq = new DlqRouter({ enabled: (options as any)?.dlq?.enabled === true })
    this.enableStickyRouter = (options as any)?.stickyRouter !== false
    this.devtoolsBus = options?.devtools === false ? null : (options?.devtools instanceof Object ? (options.devtools as DevToolsBus) : getDevToolsBus())
    this.metaStaticPart = Object.freeze({
      service: this.serviceName,
      instanceId: this.instanceId,
      auth: this.authToken ? { token: this.authToken } : undefined,
      codec: this.codec.name
    })

    const { Kafka } = getKafkaModule()
    this.sharedKafkaForSubs = new Kafka({
      clientId: `${this.serviceName || "nevo"}-shared`,
      brokers: this.brokers
    })

    this.serviceNames.forEach((serviceName) => {
      const topicName = `${serviceName}-events`
      const replyTopicName = `${topicName}.reply`
      this.kafkaClient.subscribeToResponseOf(topicName)
      this.kafkaClient.subscribeToResponseOf(replyTopicName)
    })

    if (this.discoveryEnabled) {
      this.discoveryRegistry.startBackgroundPrune(this.discoveryTtlMs)
      void this.initDiscovery()
    }
  }

  getInstanceId(): string { return this.instanceId }

  private buildMeta(type: MessageType, opts?: { idempotencyKey?: string; version?: string; headers?: Record<string, string>; tenantId?: string }): MessageMeta {
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

  private encodeRequestSync(method: string, params: unknown, type: MessageType, opts?: { idempotencyKey?: string; version?: string; headers?: Record<string, string>; tenantId?: string }): { key: string; value: Uint8Array; meta: MessageMeta; uuid: string; method: string } {
    const uuid = uuidv7()
    const meta = this.buildMeta(type, opts)
    const versioned = method.includes("@") ? method : formatMethod(method, opts?.version || DEFAULT_METHOD_VERSION)
    const body = { uuid, method: versioned, params, meta }
    const raw = this.codec.encode(body)
    if (raw.byteLength > this.maxPayloadBytes) {
      throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Payload size ${raw.byteLength}B exceeds ${this.maxPayloadBytes}B` })
    }
    const compressed = maybeCompress(raw, this.compression)
    meta.contentEncoding = compressed.encoding
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "out", service: this.serviceName ?? "unknown" }, compressed.data.byteLength)
    return { key: uuid, value: compressed.data, meta, uuid, method: versioned }
  }

  private encodeRequest(method: string, params: unknown, type: MessageType, opts?: { idempotencyKey?: string; version?: string; headers?: Record<string, string>; tenantId?: string }): { key: string; value: Uint8Array; meta: MessageMeta; uuid: string; method: string } | Promise<{ key: string; value: Uint8Array; meta: MessageMeta; uuid: string; method: string }> {
    if (this.compression.async && this.compression.enabled) return this.encodeRequestAsync(method, params, type, opts)
    return this.encodeRequestSync(method, params, type, opts)
  }

  private async encodeRequestAsync(method: string, params: unknown, type: MessageType, opts?: { idempotencyKey?: string; version?: string; headers?: Record<string, string>; tenantId?: string }): Promise<{ key: string; value: Uint8Array; meta: MessageMeta; uuid: string; method: string }> {
    const uuid = uuidv7()
    const meta = this.buildMeta(type, opts)
    const versioned = method.includes("@") ? method : formatMethod(method, opts?.version || DEFAULT_METHOD_VERSION)
    const body = { uuid, method: versioned, params, meta }
    const raw = this.codec.encode(body)
    if (raw.byteLength > this.maxPayloadBytes) {
      throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Payload size ${raw.byteLength}B exceeds ${this.maxPayloadBytes}B` })
    }
    const compressed = await maybeCompressAsync(raw, this.compression)
    meta.contentEncoding = compressed.encoding
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "out", service: this.serviceName ?? "unknown" }, compressed.data.byteLength)
    return { key: uuid, value: compressed.data, meta, uuid, method: versioned }
  }

  private decodePayload<T = any>(data: Uint8Array | Buffer | string, encoding?: string): T {
    const buf = typeof data === "string" ? Buffer.from(data, "utf8") : Buffer.from(data as any)
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "in", service: this.serviceName ?? "unknown" }, buf.byteLength)
    const decompressed = encoding ? maybeDecompress(buf, encoding) : buf
    return this.codec.decode<T>(decompressed)
  }

  private ensureServiceRegistered(serviceName: string): string {
    const normalized = normalizeServiceName(serviceName)
    if (!this.serviceNames.includes(normalized)) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: `Service "${serviceName}" is not registered in nevo kafka client`, availableServices: this.serviceNames })
    }
    return normalized
  }

  async query<T = any>(serviceName: string, method: string, params: any, opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string>; tenantId?: string; timeoutMs?: number }): Promise<T> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const topic = `${normalized}-events`
    const cbKey = `${normalized}:${method}`

    return this.shutdown.trackInflight((async () => {
      const idempotencyKey = opts?.idempotencyKey
      if (idempotencyKey && this.idempotencyCache.isEnabled() && this.idempotencyCache.has(idempotencyKey)) {
        return this.idempotencyCache.get(idempotencyKey) as T
      }

      const result = await withRetry(async (attempt) => {
        this.circuitBreaker.before(cbKey)
        const startMs = Date.now()
        let lastUuid: string | undefined
        let lastChainId: string | undefined
        try {
          const { key, value, uuid, meta } = await this.encodeRequest(method, params, "query", { ...opts, headers: { ...(opts?.headers || {}), "nevo-attempt": String(attempt) } })
          lastUuid = uuid
          lastChainId = meta.nevoChainId
          const span = this.tracer.startSpan(`nevo.client.query ${normalized}.${method}`, { "nevo.method": method, "nevo.service": normalized, "nevo.attempt": attempt })
          try {
            const response: any = await lastValueFrom(this.kafkaClient.send<any>(topic, { key, value: Buffer.from(value) }).pipe(timeout(opts?.timeoutMs ?? this.timeoutMs)))
            const payload = typeof response === "string" || response instanceof Uint8Array ? this.decodePayload(response as any) : response
            if (payload?.params?.result === "error" && payload?.params?.error) {
              const err = payload.params.error
              throw new MessagingError(err.code, err.details ?? { message: err.message }, err.service || normalized)
            }
            this.circuitBreaker.onSuccess(cbKey)
            span.setStatus({ code: 1 })
            publishClientEvent(this.devtoolsBus, { service: normalized, method, uuid, chainId: lastChainId, durationMs: Date.now() - startMs, status: "ok", transport: "kafka", origin: this.serviceName })
            return payload?.params?.result as T
          } catch (err: any) {
            span.recordException(err)
            span.setStatus({ code: 2, message: err?.message })
            if (err instanceof RxTimeoutError) throw new TimeoutError(serviceName, method, opts?.timeoutMs ?? this.timeoutMs)
            throw err
          } finally {
            span.end()
            this.metrics.incCounter(NEVO_METRIC_NAMES.requestsTotal, { transport: "kafka", service: normalized, method, role: "client" })
            if (attempt > 1) this.metrics.incCounter(NEVO_METRIC_NAMES.retries, { transport: "kafka", service: normalized, method })
          }
        } catch (err: any) {
          this.circuitBreaker.onFailure(cbKey, err)
          publishClientEvent(this.devtoolsBus, {
            service: normalized, method, uuid: lastUuid, chainId: lastChainId,
            durationMs: Date.now() - startMs, status: "error", transport: "kafka", origin: this.serviceName,
            error: { code: err instanceof MessagingError ? err.code : err?.code, message: err?.message ?? String(err) }
          })
          throw err
        }
      }, this.retryOptions)

      if (idempotencyKey && this.idempotencyCache.isEnabled()) this.idempotencyCache.set(idempotencyKey, result)
      return result
    })())
  }

  async emit(serviceName: string, method: string, params: any, opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string> }): Promise<void> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const topic = `${normalized}-events`
    const { key, value } = await this.encodeRequest(method, params, "emit", opts)
    this.kafkaClient.emit(topic, { key, value: Buffer.from(value) })
  }

  async emitBatch(items: Array<{ serviceName: string; method: string; params: any; opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string> } }>): Promise<void> {
    if (items.length === 0) return
    if (!this.batchProducer) {
      this.batchProducer = this.sharedKafkaForSubs.producer({ allowAutoTopicCreation: true })
      await this.batchProducer.connect()
    }
    const byTopic = new Map<string, Array<{ key: string; value: Buffer }>>()
    if (this.compression.async && this.compression.enabled) {
      await Promise.all(items.map(async (item) => {
        const normalized = this.ensureServiceRegistered(item.serviceName)
        const topic = `${normalized}-events`
        const { key, value } = await this.encodeRequest(item.method, item.params, "emit", item.opts) as { key: string; value: Uint8Array }
        let arr = byTopic.get(topic)
        if (!arr) { arr = []; byTopic.set(topic, arr) }
        arr.push({ key, value: Buffer.from(value) })
      }))
    } else {
      for (const item of items) {
        const normalized = this.ensureServiceRegistered(item.serviceName)
        const topic = `${normalized}-events`
        const enc = this.encodeRequestSync(item.method, item.params, "emit", item.opts)
        let arr = byTopic.get(topic)
        if (!arr) { arr = []; byTopic.set(topic, arr) }
        arr.push({ key: enc.key, value: Buffer.from(enc.value) })
      }
    }
    const topicMessages: { topic: string; messages: { key: string; value: Buffer }[] }[] = []
    for (const [topic, messages] of byTopic.entries()) topicMessages.push({ topic, messages })
    await this.batchProducer!.sendBatch({ topicMessages })
  }

  getAvailableServices(): string[] { return [...this.serviceNames] }

  async publish(serviceName: string, method: string, params: any, opts?: { version?: string; headers?: Record<string, string> }): Promise<void> {
    const normalized = this.ensureServiceRegistered(serviceName)
    const topic = `${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    const { key, value } = await this.encodeRequest(method, params, "sub", opts)
    this.kafkaClient.emit(topic, { key, value: Buffer.from(value) })
  }

  async broadcast(method: string, params: any, opts?: { version?: string; headers?: Record<string, string> }): Promise<void> {
    const { key, value } = await this.encodeRequest(method, params, "broadcast", opts)
    this.kafkaClient.emit(DEFAULT_BROADCAST_TOPIC, { key, value: Buffer.from(value) })
  }

  async subscribe<T = any>(
    serviceName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: (data: T, context: SubscriptionContext) => Promise<void> | void
  ): Promise<Subscription> {
    const normalized = normalizeServiceName(serviceName)
    const isBroadcast = normalized === DEFAULT_BROADCAST_TOPIC
    if (!isBroadcast) this.ensureServiceRegistered(serviceName)

    const explicitGroupId = options?.groupId
      || (options?.durableKey ? `nevo-sub-${options.durableKey}` : undefined)
    const topic = isBroadcast ? DEFAULT_BROADCAST_TOPIC : `${normalized}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    const manualAck = options?.ack === true
    const maxAttempts = options?.maxDeliveryAttempts ?? 3

    if (this.enableStickyRouter && explicitGroupId) {
      return this.subscribeSticky(topic, method, explicitGroupId, options, manualAck, maxAttempts, handler as any)
    }

    const groupId = explicitGroupId || `nevo-sub-${this.serviceName || "client"}-${randomUUID()}`
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
          payload = this.decodePayload(message.value, encoding)
        } catch (err) {
          this.logger.error({ event: "kafka.parse_error", topic, err: (err as Error)?.message }, "Failed to parse subscription message")
          await this.dlq.route({ topic, reason: "parse-error", error: { message: (err as Error)?.message }, ts: Date.now() })
          return
        }
        if (method && payload.method !== method && payload.method?.split("@")[0] !== method) return
        if (!matchesFilter(options?.filter, payload.meta)) return

        const msgKey = `${topic}:${partition}:${message.offset}`
        const attempts = (deliveryCounts.get(msgKey) ?? 0) + 1
        deliveryCounts.set(msgKey, attempts)

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
            try { pause() } catch {}
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
      await group.consumer.subscribe({ topic, fromBeginning: options?.fromBeginning || false })
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
          try { await group.consumer.disconnect() } catch {}
        }
      }
    }
  }

  private async ensureStickyGroup(groupId: string, manualAck: boolean): Promise<StickyGroup> {
    let group = this.stickyGroups.get(groupId)
    if (group) return group

    const consumer = this.sharedKafkaForSubs.consumer({ groupId, allowAutoTopicCreation: true })
    await consumer.connect()
    const dispatcher = new Map<string, Set<StickyHandlerEntry>>()
    const deliveryCounts = new Map<string, number>()

    group = { consumer, dispatcher, manualAck }
    this.stickyGroups.set(groupId, group)
    this.subscriptionConsumers.add(consumer)

    await consumer.run({
      autoCommit: !manualAck,
      eachMessage: async ({ topic, partition, message, pause }) => {
        const entries = dispatcher.get(topic)
        if (!entries || entries.size === 0 || !message.value) return
        let payload: any
        try {
          const encoding = message.headers?.["content-encoding"]?.toString?.()
          payload = this.decodePayload(message.value, encoding)
        } catch (err) {
          this.logger.error({ event: "kafka.parse_error", topic, err: (err as Error)?.message })
          await this.dlq.route({ topic, reason: "parse-error", error: { message: (err as Error)?.message }, ts: Date.now() })
          return
        }

        const msgKey = `${topic}:${partition}:${message.offset}`
        const attempts = (deliveryCounts.get(msgKey) ?? 0) + 1
        deliveryCounts.set(msgKey, attempts)

        let anyDelivered = false
        for (const entry of entries) {
          if (entry.method && payload.method !== entry.method && payload.method?.split("@")[0] !== entry.method) continue
          if (!matchesFilter(entry.filter, payload.meta)) continue
          anyDelivered = true

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
            } else if (manualAck) {
              try { pause() } catch {}
            }
          }
        }
        if (anyDelivered) deliveryCounts.delete(msgKey)
      }
    })

    return group
  }

  getDiscoveredServices() { this.discoveryRegistry.prune(this.discoveryTtlMs); return this.discoveryRegistry.list() }
  isServiceAvailable(serviceName: string): boolean { return this.discoveryRegistry.isAvailable(serviceName, this.discoveryTtlMs) }

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

  async close(timeoutMs = 30_000): Promise<void> {
    if (this.discoveryTimer) clearInterval(this.discoveryTimer)
    this.discoveryRegistry.stopBackgroundPrune()
    if (this.discoveryConsumer) { try { await this.discoveryConsumer.disconnect() } catch {} }
    if (this.discoveryProducer) { try { await this.discoveryProducer.disconnect() } catch {} }
    if (this.batchProducer) { try { await this.batchProducer.disconnect() } catch {} }
    this.stickyGroups.clear()
    for (const c of this.subscriptionConsumers) {
      try { await c.disconnect() } catch {}
    }
    this.subscriptionConsumers.clear()
    await this.shutdown.shutdown(timeoutMs)
  }
}
