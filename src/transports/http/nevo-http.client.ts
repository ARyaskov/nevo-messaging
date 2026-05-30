import * as http from "node:http"
import * as https from "node:https"
import { randomUUID } from "node:crypto"
import { uuidv7 } from "../../common/uuid"
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
  TimeoutError,
  ErrorCode,
  Subscription,
  SubscriptionContext,
  SubscriptionOptions,
  Codec,
  JsonCodec,
  MessagePackCodec,
  getCodec,
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
  maybeDecompressAsync,
  shouldDecompressAsync,
  enforcePayloadLimit,
  DEFAULT_MAX_PAYLOAD_BYTES,
  getDefaultTracer,
  NevoTracer,
  getDefaultMetrics,
  NEVO_METRIC_NAMES,
  methodLabel,
  MetricsRegistry,
  GracefulShutdown,
  LruIdempotencyCache,
  TransportClientOptions,
  matchesFilter,
  DEFAULT_METHOD_VERSION,
  formatMethod,
  parseWithBigInt,
  DevToolsBus,
  getDevToolsBus,
  publishClientEvent,
  normalizeServiceName,
  resolveOutboundChainId
} from "../../common"

export interface NevoHttpClientOptions extends TransportClientOptions {
  timeoutMs?: number
  discoveryUrl?: string
  useMessagePack?: boolean
  keepAlive?: boolean
  maxSockets?: number
  maxFreeSockets?: number
  useUndici?: boolean
  tcpNoDelay?: boolean
  socketKeepAliveMs?: number
  recvBufferSize?: number
  cacheableDns?: boolean | { ttl?: number; maxTtl?: number }
}

export class NevoHttpClient {
  private readonly serviceUrls: Map<string, string>
  private readonly timeoutMs: number
  private readonly debug: boolean
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
  private readonly discoveryRegistry = new DiscoveryRegistry()
  private readonly discoveryEnabled: boolean
  private readonly discoveryHeartbeatIntervalMs: number
  private readonly discoveryTtlMs: number
  private readonly discoveryUrl?: string
  private discoveryTimer?: NodeJS.Timeout
  private discoveryAbort?: AbortController
  private readonly httpAgent: http.Agent
  private readonly httpsAgent: https.Agent
  private readonly capabilities?: string[]
  private readonly host?: string
  private readonly port?: number
  private readonly version?: string
  private readonly devtoolsBus: DevToolsBus | null
  private readonly metaStaticPart: Pick<MessageMeta, "service" | "instanceId" | "auth" | "codec">
  private readonly tcpNoDelay: boolean = true
  private readonly recvBufferSize?: number

  constructor(serviceUrls: Record<string, string>, options?: NevoHttpClientOptions) {
    this.serviceUrls = new Map(Object.entries(serviceUrls).map(([k, v]) => [k.toLowerCase(), v]))
    this.timeoutMs = options?.timeoutMs ?? options?.timeout ?? 20000
    this.debug = options?.debug || false
    this.serviceName = options?.serviceName
    this.instanceId = options?.instanceId || randomUUID()
    this.authToken = options?.authToken
    this.logger = (options?.logger as NevoLogger) || getDefaultLogger().child({ component: "http-client", service: this.serviceName })
    this.codec = options?.codec
      ? (typeof options.codec === "string" ? getCodec(options.codec) : (options.codec as Codec))
      : options?.useMessagePack === false
        ? new JsonCodec()
        : tryMsgpackOrJson()
    this.circuitBreaker = new CircuitBreakerRegistry(options?.circuitBreaker)
    this.retryOptions = resolveRetryOptions(options?.retry)
    this.compression = resolveCompressionOptions(options?.compression)
    this.tracer = getDefaultTracer()
    this.metrics = getDefaultMetrics()
    this.maxPayloadBytes = options?.security?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
    this.idempotencyCache = new LruIdempotencyCache<unknown>(options?.idempotency)
    this.discoveryEnabled = options?.discovery?.enabled === true && !!options?.discoveryUrl
    this.discoveryHeartbeatIntervalMs = options?.discovery?.heartbeatIntervalMs || 10000
    this.discoveryTtlMs = options?.discovery?.ttlMs || 30000
    this.discoveryUrl = options?.discoveryUrl
    this.capabilities = options?.discovery?.capabilities
    this.host = options?.discovery?.host
    this.port = options?.discovery?.port
    this.version = options?.discovery?.version
    this.devtoolsBus = options?.devtools === false ? null : (options?.devtools instanceof Object ? (options.devtools as DevToolsBus) : getDevToolsBus())
    this.metaStaticPart = Object.freeze({
      service: this.serviceName,
      instanceId: this.instanceId,
      auth: this.authToken ? { token: this.authToken } : undefined,
      codec: this.codec.name
    })

    const keepAlive = options?.keepAlive !== false
    const maxSockets = options?.maxSockets ?? Infinity
    const maxFreeSockets = options?.maxFreeSockets ?? 256
    const keepAliveMsecs = options?.socketKeepAliveMs ?? 1000
    this.httpAgent = new http.Agent({ keepAlive, maxSockets, maxFreeSockets, keepAliveMsecs })
    this.httpsAgent = new https.Agent({ keepAlive, maxSockets, maxFreeSockets, keepAliveMsecs })

    if (options?.cacheableDns) {
      try {
        const cl = require("cacheable-lookup")
        const Lookup = cl.default ?? cl
        const lookup = new Lookup(typeof options.cacheableDns === "object" ? options.cacheableDns : {})
        lookup.install(this.httpAgent)
        lookup.install(this.httpsAgent)
      } catch (err: any) {
        this.logger.warn({ event: "http.cacheableDns.missing", err: err?.message }, 'Install "cacheable-lookup" to enable cached DNS')
      }
    }

    this.tcpNoDelay = options?.tcpNoDelay !== false
    this.recvBufferSize = options?.recvBufferSize

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

  private createBodySync(method: string, params: unknown, type: MessageType, opts?: any): { buf: Uint8Array; meta: MessageMeta; uuid: string; encoding: "gzip" | "deflate" | "zstd" | "identity"; versionedMethod: string } {
    const uuid = uuidv7()
    const meta = this.buildMeta(type, opts)
    const versionedMethod = method.includes("@") ? method : formatMethod(method, opts?.version || DEFAULT_METHOD_VERSION)
    const envelope = { uuid, method: versionedMethod, params, meta }
    const raw = this.codec.encode(envelope)
    if (raw.byteLength > this.maxPayloadBytes) {
      throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Payload size ${raw.byteLength}B exceeds ${this.maxPayloadBytes}B` })
    }
    const compressed = maybeCompress(raw, this.compression)
    meta.contentEncoding = compressed.encoding
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "out", service: this.serviceName ?? "unknown" }, compressed.data.byteLength)
    return { buf: compressed.data, meta, uuid, encoding: compressed.encoding, versionedMethod }
  }

  private createBody(method: string, params: unknown, type: MessageType, opts?: any): { buf: Uint8Array; meta: MessageMeta; uuid: string; encoding: "gzip" | "deflate" | "zstd" | "identity"; versionedMethod: string } | Promise<{ buf: Uint8Array; meta: MessageMeta; uuid: string; encoding: "gzip" | "deflate" | "zstd" | "identity"; versionedMethod: string }> {
    if (this.compression.async && this.compression.enabled) return this.createBodyAsync(method, params, type, opts)
    return this.createBodySync(method, params, type, opts)
  }

  private async createBodyAsync(method: string, params: unknown, type: MessageType, opts?: any): Promise<{ buf: Uint8Array; meta: MessageMeta; uuid: string; encoding: "gzip" | "deflate" | "zstd" | "identity"; versionedMethod: string }> {
    const uuid = uuidv7()
    const meta = this.buildMeta(type, opts)
    const versionedMethod = method.includes("@") ? method : formatMethod(method, opts?.version || DEFAULT_METHOD_VERSION)
    const envelope = { uuid, method: versionedMethod, params, meta }
    const raw = this.codec.encode(envelope)
    if (raw.byteLength > this.maxPayloadBytes) {
      throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Payload size ${raw.byteLength}B exceeds ${this.maxPayloadBytes}B` })
    }
    const compressed = await maybeCompressAsync(raw, this.compression)
    meta.contentEncoding = compressed.encoding
    this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "out", service: this.serviceName ?? "unknown" }, compressed.data.byteLength)
    return { buf: compressed.data, meta, uuid, encoding: compressed.encoding, versionedMethod }
  }

  private getServiceUrl(serviceName: string): string {
    const normalized = normalizeServiceName(serviceName)
    const url = this.serviceUrls.get(normalized)
    if (!url) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: `Service "${serviceName}" is not registered in nevo http client`, availableServices: this.serviceUrls.keys().toArray() })
    }
    return url.replace(/\/+$/, "")
  }

  private async sendBuffer(url: string, body: Uint8Array, contentType: string, encoding: "gzip" | "deflate" | "zstd" | "identity", headers?: Record<string, string>, timeoutMs?: number): Promise<{ status: number; body: Uint8Array; headers: Record<string, string> }> {
    const u = new URL(url)
    const lib = u.protocol === "https:" ? https : http
    const agent = u.protocol === "https:" ? this.httpsAgent : this.httpAgent
    const reqOpts: http.RequestOptions = {
      method: "POST",
      protocol: u.protocol,
      hostname: u.hostname,
      port: u.port,
      path: u.pathname + u.search,
      agent,
      headers: {
        "content-type": contentType,
        "content-length": String(body.byteLength),
        accept: contentType,
        ...(encoding !== "identity" ? { "content-encoding": encoding } : {}),
        ...(headers || {})
      }
    }

    const { promise, resolve, reject } = Promise.withResolvers<{ status: number; body: Uint8Array; headers: Record<string, string> }>()
    const req = lib.request(reqOpts, (res) => {
      if (this.recvBufferSize) { try { (res.socket as any)?.setRecvBufferSize?.(this.recvBufferSize) } catch {} }
      const expected = Number(res.headers["content-length"])
      const knownLen = Number.isFinite(expected) && expected > 0 ? expected : -1
      let preBuf: Buffer | null = knownLen > 0 ? Buffer.allocUnsafe(knownLen) : null
      let offset = 0
      const chunks: Buffer[] = []
      res.on("data", (c: Buffer) => {
        if (preBuf) {
          if (offset + c.length > preBuf.length) {
            const grown = Buffer.allocUnsafe(offset + c.length)
            preBuf.copy(grown, 0, 0, offset)
            preBuf = grown
          }
          c.copy(preBuf, offset)
          offset += c.length
        } else {
          chunks.push(c)
        }
      })
      res.on("end", () => {
        clearTimeout(deadline)
        const respBuf = preBuf ? preBuf.subarray(0, offset) : Buffer.concat(chunks)
        const respHeaders: Record<string, string> = {}
        for (const [k, v] of Object.entries(res.headers)) respHeaders[k] = Array.isArray(v) ? v.join(",") : String(v)
        resolve({ status: res.statusCode ?? 0, body: respBuf, headers: respHeaders })
      })
    })
    // req.setTimeout is a socket-inactivity timeout: it resets on every byte, so a
    // slowly-trickling response could exceed the requested timeout indefinitely. Pair
    // it with an explicit wall-clock deadline that destroys the request regardless.
    const deadline = setTimeout(() => { req.destroy(new Error("timeout")) }, timeoutMs ?? this.timeoutMs)
    req.setTimeout(timeoutMs ?? this.timeoutMs, () => {
      req.destroy(new Error("timeout"))
    })
    req.on("error", (err) => { clearTimeout(deadline); reject(err) })
    if (this.tcpNoDelay) {
      req.on("socket", (socket) => {
        try { socket.setNoDelay(true) } catch {}
        try { socket.setKeepAlive(true, 1000) } catch {}
      })
    }
    req.write(body)
    req.end()
    return promise
  }

  async query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string>; timeoutMs?: number; tenantId?: string }): Promise<T> {
    const cbKey = `${normalizeServiceName(serviceName)}:${method}`
    const url = this.getServiceUrl(serviceName)
    const endpoint = `${url}/${normalizeServiceName(serviceName)}${DEFAULT_EVENTS_SUFFIX}`

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
          const { buf, encoding, uuid, meta } = await this.createBody(method, params, "query", { ...opts, headers: { ...(opts?.headers || {}), "nevo-attempt": String(attempt) } })
          lastUuid = uuid
          lastChainId = meta.nevoChainId
          const span = this.tracer.startSpan(`nevo.client.query ${serviceName}.${method}`, { "nevo.method": method, "nevo.service": serviceName, "nevo.attempt": attempt })
          try {
            const res = await this.sendBuffer(endpoint, buf, this.codec.contentType, encoding, undefined, opts?.timeoutMs)
            this.metrics.observeHistogram(NEVO_METRIC_NAMES.payloadBytes, { direction: "in", service: this.serviceName ?? "unknown" }, res.body.byteLength)
            const respEncoding = res.headers["content-encoding"]
            const decompressed = shouldDecompressAsync(res.body.byteLength, respEncoding)
              ? await maybeDecompressAsync(res.body, respEncoding, this.maxPayloadBytes)
              : maybeDecompress(res.body, respEncoding, this.maxPayloadBytes)
            enforcePayloadLimit(decompressed, this.maxPayloadBytes)
            let payload: any
            try {
              payload = decompressed.byteLength === 0 ? undefined : this.codec.decode(decompressed)
            } catch (decodeErr) {
              // A non-nevo error response (proxy/gateway HTML, plain text) may not decode.
              // Surface the HTTP status rather than a misleading parse error.
              if (res.status >= 400) throw httpStatusToError(res.status, serviceName)
              throw decodeErr
            }
            if (payload?.params?.result === "error" && payload?.params?.error) {
              const err = payload.params.error
              throw new MessagingError(err.code, err.details ?? { message: err.message }, err.service || serviceName)
            }
            // A server error status whose body is not a nevo error envelope must not be
            // returned as a successful empty result (which would also tell the breaker onSuccess).
            if (res.status >= 400) throw httpStatusToError(res.status, serviceName)
            this.circuitBreaker.onSuccess(cbKey)
            span.setStatus({ code: 1 })
            publishClientEvent(this.devtoolsBus, { service: serviceName, method, uuid, chainId: lastChainId, durationMs: Date.now() - startMs, status: "ok", transport: "http", origin: this.serviceName })
            return payload?.params?.result as T
          } catch (err: any) {
            span.recordException(err)
            span.setStatus({ code: 2, message: err?.message })
            if (err?.message === "timeout") throw new TimeoutError(serviceName, method, opts?.timeoutMs ?? this.timeoutMs)
            throw err
          } finally {
            span.end()
            this.metrics.incCounter(NEVO_METRIC_NAMES.requestsTotal, { transport: "http", service: serviceName, method: methodLabel(method), role: "client" })
            if (attempt > 1) this.metrics.incCounter(NEVO_METRIC_NAMES.retries, { transport: "http", service: serviceName, method: methodLabel(method) })
          }
        } catch (err: any) {
          this.circuitBreaker.onFailure(cbKey, err)
          publishClientEvent(this.devtoolsBus, {
            service: serviceName, method, uuid: lastUuid, chainId: lastChainId,
            durationMs: Date.now() - startMs, status: "error", transport: "http", origin: this.serviceName,
            error: { code: err instanceof MessagingError ? err.code : err?.code, message: err?.message ?? String(err) }
          })
          throw err
        }
      }, this.retryOptions)

      if (opts?.idempotencyKey && this.idempotencyCache.isEnabled()) this.idempotencyCache.set(opts.idempotencyKey, result)
      return result
    })())
  }

  async emit(serviceName: string, method: string, params: unknown, opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string> }): Promise<void> {
    const url = this.getServiceUrl(serviceName)
    const endpoint = `${url}/${normalizeServiceName(serviceName)}${DEFAULT_EVENTS_SUFFIX}`
    const { buf, encoding } = await this.createBody(method, params, "emit", opts)
    await this.sendBuffer(endpoint, buf, this.codec.contentType, encoding)
  }

  async publish(serviceName: string, method: string, params: unknown, opts?: { version?: string; headers?: Record<string, string> }): Promise<void> {
    const url = this.getServiceUrl(serviceName)
    const endpoint = `${url}/__nevo/publish`
    const { buf, encoding } = await this.createBody(method, params, "sub", { ...opts, headers: { ...(opts?.headers || {}), "nevo-service": serviceName } })
    await this.sendBuffer(endpoint, buf, this.codec.contentType, encoding)
  }

  async broadcast(method: string, params: unknown, opts?: { version?: string; headers?: Record<string, string> }): Promise<void> {
    const url = this.discoveryUrl || this.serviceUrls.values().next().value
    if (!url) throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: "No base URL available for broadcast" })
    const endpoint = `${url.replace(/\/+$/, "")}/${DEFAULT_BROADCAST_TOPIC}`
    const { buf, encoding } = await this.createBody(method, params, "broadcast", opts)
    await this.sendBuffer(endpoint, buf, this.codec.contentType, encoding)
  }

  async subscribe<T = unknown>(
    serviceName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: (data: T, context: SubscriptionContext) => Promise<void> | void
  ): Promise<Subscription> {
    const normalized = normalizeServiceName(serviceName)
    const isBroadcast = normalized === DEFAULT_BROADCAST_TOPIC
    const baseUrl = isBroadcast ? (this.discoveryUrl || this.serviceUrls.values().next().value) : this.getServiceUrl(serviceName)
    if (!baseUrl) throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: "No base URL available for subscription" })

    const endpoint = isBroadcast
      ? `${baseUrl.replace(/\/+$/, "")}/${DEFAULT_BROADCAST_TOPIC}`
      : `${baseUrl.replace(/\/+$/, "")}/__nevo/subscribe?service=${encodeURIComponent(serviceName)}`

    const controller = new AbortController()
    const response = await fetch(endpoint, { headers: { Accept: "text/event-stream" }, signal: controller.signal })

    if (!response.ok || !response.body) {
      throw new MessagingError(ErrorCode.SERVICE_UNAVAILABLE, { message: `Failed to subscribe via SSE: ${response.status}` })
    }

    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ""

    const readLoop = async () => {
      while (true) {
        const { value, done } = await reader.read()
        if (done) break
        buffer += decoder.decode(value)
        const parts = buffer.split("\n\n")
        buffer = parts.pop() || ""
        for (const part of parts) {
          const line = part.split("\n").find((l: string) => l.startsWith("data:"))
          if (!line) continue
          const raw = line.replace(/^data:\s*/, "")
          let payload: any
          try {
            payload = parseWithBigInt(raw, { acceptLegacy: true })
          } catch {
            continue
          }
          if (method && payload.method !== method && payload.method?.split("@")[0] !== method) continue
          if (!matchesFilter(options?.filter, payload.meta)) continue
          const context: SubscriptionContext = {
            meta: payload.meta || {},
            ack: async () => {},
            nack: async () => {}
          }
          try {
            await handler(payload.params as T, context)
          } catch (err) {
            this.logger.error({ event: "http.sub.handler_error", err: (err as Error)?.message })
          }
        }
      }
    }
    void readLoop()

    return { unsubscribe: async () => controller.abort() }
  }

  getAvailableServices(): string[] { return this.serviceUrls.keys().toArray() }
  getDiscoveredServices() { this.discoveryRegistry.prune(this.discoveryTtlMs); return this.discoveryRegistry.list() }
  isServiceAvailable(serviceName: string): boolean { return this.discoveryRegistry.isAvailable(serviceName, this.discoveryTtlMs) }

  private async initDiscovery(): Promise<void> {
    if (!this.discoveryUrl) return
    this.discoveryAbort = new AbortController()
    const discoveryEndpoint = `${this.discoveryUrl.replace(/\/+$/, "")}/${DEFAULT_DISCOVERY_TOPIC}`
    try {
      const response = await fetch(discoveryEndpoint, { headers: { Accept: "text/event-stream" }, signal: this.discoveryAbort.signal })
      if (response.ok && response.body) {
        const reader = response.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ""
        ;(async () => {
          while (true) {
            const { value, done } = await reader.read()
            if (done) break
            buffer += decoder.decode(value)
            const parts = buffer.split("\n\n")
            buffer = parts.pop() || ""
            for (const part of parts) {
              const line = part.split("\n").find((l: string) => l.startsWith("data:"))
              if (!line) continue
              const raw = line.replace(/^data:\s*/, "")
              try {
                const payload = parseWithBigInt(raw, { acceptLegacy: true }) as DiscoveryAnnouncement
                if (payload?.serviceName) this.discoveryRegistry.update(payload)
              } catch {}
            }
          }
        })()
      }
    } catch (err) {
      this.logger.warn({ event: "http.discovery.connect_failed", err: (err as Error)?.message })
    }

    this.discoveryTimer = setInterval(async () => {
      const announcement: DiscoveryAnnouncement = {
        serviceName: this.serviceName || "unknown",
        instanceId: this.instanceId,
        clientId: this.serviceName,
        transport: "http",
        ts: Date.now(),
        host: this.host,
        port: this.port,
        version: this.version,
        capabilities: this.capabilities
      }
      try {
        const buf = new JsonCodec().encode(announcement)
        await this.sendBuffer(discoveryEndpoint, buf, "application/json", "identity")
      } catch {}
    }, this.discoveryHeartbeatIntervalMs)
    if (typeof this.discoveryTimer.unref === "function") this.discoveryTimer.unref()
  }

  async close(timeoutMs = 30_000): Promise<void> {
    if (this.discoveryTimer) clearInterval(this.discoveryTimer)
    this.discoveryRegistry.stopBackgroundPrune()
    if (this.discoveryAbort) this.discoveryAbort.abort()
    this.httpAgent.destroy()
    this.httpsAgent.destroy()
    await this.shutdown.shutdown(timeoutMs)
  }
}

function tryMsgpackOrJson(): Codec {
  try {
    const codec = new MessagePackCodec()
    codec.encode({ probe: 1 })
    return codec
  } catch {
    return new JsonCodec()
  }
}

// Maps an HTTP error status (>= 400) to a MessagingError. Only gateway/availability
// statuses (502/503/504) are marked retryable; everything else (incl. 500) is terminal.
function httpStatusToError(status: number, serviceName: string): MessagingError {
  switch (status) {
    case 413:
      return new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Remote returned HTTP ${status}`, httpStatus: status }, serviceName)
    case 502:
    case 503:
      return new MessagingError(ErrorCode.SERVICE_UNAVAILABLE, { message: `Remote returned HTTP ${status}`, httpStatus: status, retryable: true }, serviceName)
    case 504:
      return new MessagingError(ErrorCode.TIMEOUT, { message: `Remote returned HTTP ${status}`, httpStatus: status, retryable: true }, serviceName)
    default:
      return new MessagingError(ErrorCode.REMOTE_ERROR, { message: `Remote returned HTTP ${status}`, httpStatus: status }, serviceName)
  }
}
