import * as http2 from "node:http2"
import {
  DEFAULT_EVENTS_SUFFIX,
  MessageMeta,
  MessageType,
  MessagingError,
  TimeoutError,
  ErrorCode,
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
  maybeDecompress,
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
  DEFAULT_METHOD_VERSION,
  formatMethod,
  DevToolsBus,
  getDevToolsBus,
  publishClientEvent,
  uuidv7,
  normalizeServiceName,
  resolveOutboundChainId
} from "../../common"

export interface NevoHttp2ClientOptions extends TransportClientOptions {
  timeoutMs?: number
}

type SessionEntry = { session: http2.ClientHttp2Session; url: URL; closed: boolean }

export class NevoHttp2Client {
  private readonly serviceUrls: Map<string, string>
  private readonly timeoutMs: number
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
  private readonly sessions = new Map<string, SessionEntry>()
  private readonly devtoolsBus: DevToolsBus | null
  private readonly metaStaticPart: Pick<MessageMeta, "service" | "instanceId" | "auth" | "codec">

  constructor(serviceUrls: Record<string, string>, options?: NevoHttp2ClientOptions) {
    this.serviceUrls = new Map(Object.entries(serviceUrls).map(([k, v]) => [k.toLowerCase(), v]))
    this.timeoutMs = options?.timeoutMs ?? options?.timeout ?? 20000
    this.serviceName = options?.serviceName
    this.instanceId = options?.instanceId || uuidv7()
    this.authToken = options?.authToken
    this.logger = (options?.logger as NevoLogger) || getDefaultLogger().child({ component: "http2-client", service: this.serviceName })
    this.codec = options?.codec ? (typeof options.codec === "string" ? getCodec(options.codec) : (options.codec as Codec)) : tryMsgpackOrJson()
    this.circuitBreaker = new CircuitBreakerRegistry(options?.circuitBreaker)
    this.retryOptions = resolveRetryOptions(options?.retry)
    this.compression = resolveCompressionOptions(options?.compression)
    this.tracer = getDefaultTracer()
    this.metrics = getDefaultMetrics()
    this.maxPayloadBytes = options?.security?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
    this.idempotencyCache = new LruIdempotencyCache<unknown>(options?.idempotency)
    this.devtoolsBus = options?.devtools === false ? null : (options?.devtools instanceof Object ? (options.devtools as DevToolsBus) : getDevToolsBus())
    this.metaStaticPart = Object.freeze({
      service: this.serviceName,
      instanceId: this.instanceId,
      auth: this.authToken ? { token: this.authToken } : undefined,
      codec: this.codec.name
    })
  }

  getInstanceId(): string { return this.instanceId }

  private buildMeta(type: MessageType, opts?: any): MessageMeta {
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

  private createBody(method: string, params: unknown, type: MessageType, opts?: any) {
    const uuid = uuidv7()
    const meta = this.buildMeta(type, opts)
    const versioned = method.includes("@") ? method : formatMethod(method, opts?.version || DEFAULT_METHOD_VERSION)
    const envelope = { uuid, method: versioned, params, meta }
    const raw = this.codec.encode(envelope)
    if (raw.byteLength > this.maxPayloadBytes) {
      throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Payload size ${raw.byteLength}B exceeds ${this.maxPayloadBytes}B` })
    }
    const compressed = maybeCompress(raw, this.compression)
    meta.contentEncoding = compressed.encoding
    return { buf: compressed.data, encoding: compressed.encoding, uuid, meta, versioned }
  }

  private async getSession(serviceName: string): Promise<SessionEntry> {
    const normalized = normalizeServiceName(serviceName)
    const url = this.serviceUrls.get(normalized)
    if (!url) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: `Service "${serviceName}" is not registered`, availableServices: this.serviceUrls.keys().toArray() })
    }
    let entry = this.sessions.get(normalized)
    if (entry && !entry.session.destroyed && !entry.session.closed) return entry
    const u = new URL(url)
    const session = http2.connect(u.origin)
    entry = { session, url: u, closed: false }
    this.sessions.set(normalized, entry)
    session.on("close", () => { entry!.closed = true })
    session.on("error", (err) => { this.logger.warn({ event: "http2.session.error", err: err.message }) })
    return entry
  }

  async query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: any): Promise<T> {
    const cbKey = `${normalizeServiceName(serviceName)}:${method}`
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
          const entry = await this.getSession(serviceName)
          const { buf, encoding, uuid, meta } = this.createBody(method, params, "query", { ...opts, headers: { ...(opts?.headers || {}), "nevo-attempt": String(attempt) } })
          lastUuid = uuid
          lastChainId = meta.nevoChainId
          const path = `${entry.url.pathname.replace(/\/+$/, "")}/${normalizeServiceName(serviceName)}${DEFAULT_EVENTS_SUFFIX}`
          const headers: http2.OutgoingHttpHeaders = {
            ":method": "POST",
            ":path": path,
            "content-type": this.codec.contentType,
            "content-length": String(buf.byteLength),
            accept: this.codec.contentType
          }
          if (encoding !== "identity") headers["content-encoding"] = encoding

          const { promise, resolve, reject } = Promise.withResolvers<T>()
          const stream = entry.session.request(headers)
          const timer = setTimeout(() => { stream.close(http2.constants.NGHTTP2_CANCEL); reject(new TimeoutError(serviceName, method, opts?.timeoutMs ?? this.timeoutMs)) }, opts?.timeoutMs ?? this.timeoutMs)
          const chunks: Buffer[] = []
          let respEncoding: string | undefined
          let status = 0
          stream.on("response", (h) => {
            status = Number(h[":status"]) || 0
            const v = h["content-encoding"]
            respEncoding = typeof v === "string" ? v : Array.isArray(v) ? v[0] : undefined
          })
          stream.on("data", (c) => chunks.push(c as any))
          stream.on("end", () => {
            clearTimeout(timer)
            try {
              const respBuf = Buffer.concat(chunks)
              const decompressed = maybeDecompress(respBuf, respEncoding, this.maxPayloadBytes)
              enforcePayloadLimit(decompressed, this.maxPayloadBytes)
              const payload: any = decompressed.byteLength === 0 ? undefined : this.codec.decode(decompressed)
              if (payload?.params?.result === "error" && payload?.params?.error) {
                const err = payload.params.error
                reject(new MessagingError(err.code, err.details ?? { message: err.message }, err.service || serviceName))
                return
              }
              // A server error status whose body is not a nevo error envelope must not be
              // resolved as a successful empty result (which would also tell the breaker onSuccess).
              if (status >= 400) {
                reject(httpStatusToError(status, serviceName))
                return
              }
              this.circuitBreaker.onSuccess(cbKey)
              publishClientEvent(this.devtoolsBus, { service: serviceName, method, uuid, chainId: lastChainId, durationMs: Date.now() - startMs, status: "ok", transport: "http2", origin: this.serviceName })
              resolve(payload?.params?.result as T)
            } catch (err: any) {
              // A non-nevo error response (proxy/gateway body) may not decode; prefer the
              // mapped HTTP status over a misleading parse error.
              if (status >= 400) { reject(httpStatusToError(status, serviceName)); return }
              reject(err instanceof MessagingError ? err : new MessagingError(ErrorCode.PARSE_ERROR, { message: err?.message ?? "decode failed" }))
            }
          })
          stream.on("error", (err: any) => {
            clearTimeout(timer)
            reject(err instanceof MessagingError ? err : new MessagingError(ErrorCode.CONNECTION_LOST, { message: err?.message ?? "stream error" }, serviceName))
          })
          stream.end(buf)
          return await promise
        } catch (err: any) {
          this.circuitBreaker.onFailure(cbKey, err)
          publishClientEvent(this.devtoolsBus, {
            service: serviceName, method, uuid: lastUuid, chainId: lastChainId,
            durationMs: Date.now() - startMs, status: "error", transport: "http2", origin: this.serviceName,
            error: { code: err instanceof MessagingError ? err.code : err?.code, message: err?.message ?? String(err) }
          })
          throw err
        } finally {
          this.metrics.incCounter(NEVO_METRIC_NAMES.requestsTotal, { transport: "http2", service: serviceName, method: methodLabel(method), role: "client" })
          if (attempt > 1) this.metrics.incCounter(NEVO_METRIC_NAMES.retries, { transport: "http2", service: serviceName, method: methodLabel(method) })
        }
      }, this.retryOptions)
      if (opts?.idempotencyKey && this.idempotencyCache.isEnabled()) this.idempotencyCache.set(opts.idempotencyKey, result)
      return result
    })())
  }

  getAvailableServices(): string[] { return this.serviceUrls.keys().toArray() }

  async close(): Promise<void> {
    for (const [, entry] of this.sessions) {
      entry.closed = true
      try { entry.session.close() } catch {}
    }
    this.sessions.clear()
    await this.shutdown.shutdown()
  }
}

function tryMsgpackOrJson(): Codec {
  try {
    const c = new MessagePackCodec()
    c.encode({ probe: 1 })
    return c
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
