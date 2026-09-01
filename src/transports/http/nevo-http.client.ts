import * as http from "node:http"
import * as https from "node:https"
import { createRequire } from "node:module"
import {
  DEFAULT_BROADCAST_TOPIC,
  DEFAULT_DISCOVERY_TOPIC,
  DEFAULT_EVENTS_SUFFIX,
  DiscoveryRegistry,
  DiscoveryAnnouncement,
  MessagingError,
  TimeoutError,
  ErrorCode,
  Subscription,
  SubscriptionContext,
  SubscriptionOptions,
  Codec,
  JsonCodec,
  MessagePackCodec,
  NevoLogger,
  getCodec,
  ClientRuntime,
  type ClientCallOptions,
  httpStatusToError,
  maybeDecompress,
  maybeDecompressAsync,
  shouldDecompressAsync,
  enforcePayloadLimit,
  NEVO_METRIC_NAMES,
  TransportClientOptions,
  matchesFilter,
  parseMethod,
  parseWithBigInt,
  normalizeServiceName
} from "../../common"

// Ceiling on trusting a server-declared content-length for pre-allocation.
const PREALLOC_CEILING_BYTES = 64 * 1024

const nodeRequire = createRequire(__filename)

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
  /** Reconnect an SSE subscription if no frame (message or heartbeat) arrives within this window. Default 40s. */
  sseIdleTimeoutMs?: number
  /** TLS / mTLS material for https targets (client cert, custom CA, SNI). */
  tls?: {
    ca?: string | Buffer | Array<string | Buffer>
    cert?: string | Buffer
    key?: string | Buffer
    passphrase?: string
    /** Reject servers whose certificate can't be verified. Default true — do not disable in production. */
    rejectUnauthorized?: boolean
    servername?: string
    minVersion?: import("node:tls").SecureVersion
  }
}

export class NevoHttpClient {
  private readonly runtime: ClientRuntime
  private readonly serviceUrls: Map<string, string>
  private readonly timeoutMs: number
  private readonly serviceName?: string
  private readonly instanceId: string
  private readonly logger: NevoLogger
  private readonly codec: Codec
  private readonly maxPayloadBytes: number
  private readonly streamAborts = new Set<AbortController>()
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
  private readonly tcpNoDelay: boolean = true
  private readonly recvBufferSize?: number
  private readonly sseIdleTimeoutMs: number
  private readonly useUndici: boolean
  private readonly undiciDispatcher?: { close(): Promise<void> | void }
  private readonly undiciRequest?: (url: string, opts: Record<string, unknown>) => Promise<any>

  constructor(serviceUrls: Record<string, string>, options?: NevoHttpClientOptions) {
    this.serviceUrls = new Map(Object.entries(serviceUrls).map(([k, v]) => [k.toLowerCase(), v]))
    this.runtime = new ClientRuntime(options, {
      transport: "http",
      fallbackCodec: () => (options?.useMessagePack === false ? new JsonCodec() : tryMsgpackOrJson())
    })
    this.timeoutMs = this.runtime.timeoutMs
    this.serviceName = this.runtime.serviceName
    this.instanceId = this.runtime.instanceId
    this.logger = this.runtime.logger
    this.codec = this.runtime.codec
    this.maxPayloadBytes = this.runtime.maxPayloadBytes
    this.discoveryEnabled = options?.discovery?.enabled === true && !!options?.discoveryUrl
    this.discoveryHeartbeatIntervalMs = options?.discovery?.heartbeatIntervalMs || 10000
    this.discoveryTtlMs = options?.discovery?.ttlMs || 30000
    this.discoveryUrl = options?.discoveryUrl
    this.capabilities = options?.discovery?.capabilities
    this.host = options?.discovery?.host
    this.port = options?.discovery?.port
    this.version = options?.discovery?.version

    const keepAlive = options?.keepAlive !== false
    const maxSockets = options?.maxSockets ?? Infinity
    const maxFreeSockets = options?.maxFreeSockets ?? 256
    const keepAliveMsecs = options?.socketKeepAliveMs ?? 1000
    const tls = options?.tls
    this.sseIdleTimeoutMs = options?.sseIdleTimeoutMs ?? 40_000
    this.httpAgent = new http.Agent({ keepAlive, maxSockets, maxFreeSockets, keepAliveMsecs })
    this.httpsAgent = new https.Agent({
      keepAlive,
      maxSockets,
      maxFreeSockets,
      keepAliveMsecs,
      ...(tls
        ? {
            ca: tls.ca,
            cert: tls.cert,
            key: tls.key,
            passphrase: tls.passphrase,
            rejectUnauthorized: tls.rejectUnauthorized,
            servername: tls.servername,
            minVersion: tls.minVersion
          }
        : {})
    })
    this.useUndici = options?.useUndici === true
    if (this.useUndici) {
      try {
        const undici = nodeRequire("undici") as {
          Agent: new (opts?: Record<string, unknown>) => { close(): Promise<void> | void }
          request: (url: string, opts: Record<string, unknown>) => Promise<any>
        }
        this.undiciDispatcher = new undici.Agent({
          connections: Number.isFinite(maxSockets) ? maxSockets : undefined,
          keepAliveTimeout: keepAliveMsecs,
          keepAliveMaxTimeout: Math.max(keepAliveMsecs, 10_000),
          ...(tls
            ? {
                connect: {
                  ca: tls.ca,
                  cert: tls.cert,
                  key: tls.key,
                  passphrase: tls.passphrase,
                  rejectUnauthorized: tls.rejectUnauthorized,
                  servername: tls.servername,
                  minVersion: tls.minVersion
                }
              }
            : {})
        })
        this.undiciRequest = undici.request
      } catch (err: any) {
        throw new MessagingError(ErrorCode.INTERNAL, {
          message: `useUndici requires the optional "undici" dependency: ${err?.message ?? err}`
        })
      }
    }

    if (options?.cacheableDns) {
      try {
        const cl = nodeRequire("cacheable-lookup")
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

  getInstanceId(): string {
    return this.instanceId
  }

  private getServiceUrl(serviceName: string): string {
    const normalized = normalizeServiceName(serviceName)
    const url = this.serviceUrls.get(normalized)
    if (!url) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, {
        message: `Service "${serviceName}" is not registered in nevo http client`,
        availableServices: this.serviceUrls.keys().toArray()
      })
    }
    return url.replace(/\/+$/, "")
  }

  private decodeResponseBody(data: Uint8Array, contentType?: string): unknown {
    if (contentType) {
      if (contentType.includes("json") && !this.codec.contentType.includes("json")) return getCodec("json").decode(data)
      if (contentType.includes("msgpack") && !this.codec.contentType.includes("msgpack")) return getCodec("msgpack").decode(data)
    }
    return this.codec.decode(data)
  }

  private async sendBuffer(
    url: string,
    body: Uint8Array,
    contentType: string,
    encoding: "gzip" | "deflate" | "zstd" | "identity",
    headers?: Record<string, string>,
    timeoutMs?: number
  ): Promise<{ status: number; body: Uint8Array; headers: Record<string, string> }> {
    if (this.useUndici) {
      return this.sendBufferUndici(url, body, contentType, encoding, headers, timeoutMs)
    }
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
    // Cap the raw download so a broken/hostile server can't OOM the client.
    const wireCap = this.maxPayloadBytes
    const req = lib.request(reqOpts, (res) => {
      if (this.recvBufferSize) {
        try {
          ;(res.socket as any)?.setRecvBufferSize?.(this.recvBufferSize)
        } catch {}
      }
      const expected = Number(res.headers["content-length"])
      if (Number.isFinite(expected) && expected > wireCap) {
        clearTimeout(deadline)
        req.destroy()
        reject(
          new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, {
            message: `Response content-length ${expected}B exceeds ${wireCap}B`,
            size: expected,
            limit: wireCap
          })
        )
        return
      }
      const knownLen = Number.isFinite(expected) && expected > 0 ? Math.min(expected, PREALLOC_CEILING_BYTES) : -1
      let preBuf: Buffer | null = knownLen > 0 ? Buffer.allocUnsafe(knownLen) : null
      let offset = 0
      const chunks: Buffer[] = []
      let received = 0
      res.on("data", (c: Buffer) => {
        received += c.length
        if (received > wireCap) {
          clearTimeout(deadline)
          req.destroy()
          reject(new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Response body exceeds ${wireCap}B`, limit: wireCap }))
          return
        }
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
    // Wall-clock deadline alongside the socket-inactivity timeout below.
    const deadline = setTimeout(() => {
      req.destroy(new Error("timeout"))
    }, timeoutMs ?? this.timeoutMs)
    req.setTimeout(timeoutMs ?? this.timeoutMs, () => {
      req.destroy(new Error("timeout"))
    })
    req.on("error", (err) => {
      clearTimeout(deadline)
      reject(err)
    })
    if (this.tcpNoDelay) {
      req.on("socket", (socket) => {
        try {
          socket.setNoDelay(true)
        } catch {}
        try {
          socket.setKeepAlive(true, 1000)
        } catch {}
      })
    }
    req.write(body)
    req.end()
    return promise
  }

  private async sendBufferUndici(
    url: string,
    body: Uint8Array,
    contentType: string,
    encoding: "gzip" | "deflate" | "zstd" | "identity",
    headers?: Record<string, string>,
    timeoutMs?: number
  ): Promise<{ status: number; body: Uint8Array; headers: Record<string, string> }> {
    if (!this.undiciRequest || !this.undiciDispatcher) {
      throw new MessagingError(ErrorCode.INTERNAL, { message: "Undici dispatcher is not initialized" })
    }
    const timeout = timeoutMs ?? this.timeoutMs
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(new Error("timeout")), timeout)
    if (typeof timer.unref === "function") timer.unref()
    try {
      const response = await this.undiciRequest(url, {
        method: "POST",
        dispatcher: this.undiciDispatcher,
        signal: controller.signal,
        headersTimeout: timeout,
        bodyTimeout: timeout,
        headers: {
          "content-type": contentType,
          "content-length": String(body.byteLength),
          accept: contentType,
          ...(encoding !== "identity" ? { "content-encoding": encoding } : {}),
          ...(headers || {})
        },
        body
      })
      const responseHeaders: Record<string, string> = {}
      for (const [key, value] of Object.entries(response.headers ?? {})) {
        responseHeaders[key] = Array.isArray(value) ? value.join(",") : String(value)
      }
      // Stream the body under a cap so a broken/hostile server can't OOM the client.
      const wireCap = this.maxPayloadBytes
      const declared = Number(responseHeaders["content-length"])
      if (Number.isFinite(declared) && declared > wireCap) {
        controller.abort()
        throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, {
          message: `Response content-length ${declared}B exceeds ${wireCap}B`,
          size: declared,
          limit: wireCap
        })
      }
      const parts: Buffer[] = []
      let received = 0
      for await (const chunk of response.body as AsyncIterable<Buffer>) {
        received += chunk.length
        if (received > wireCap) {
          controller.abort()
          throw new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Response body exceeds ${wireCap}B`, limit: wireCap })
        }
        parts.push(chunk)
      }
      return {
        status: Number(response.statusCode) || 0,
        body: new Uint8Array(Buffer.concat(parts, received)),
        headers: responseHeaders
      }
    } catch (err) {
      if (err instanceof MessagingError) throw err
      if (controller.signal.aborted) throw new Error("timeout", { cause: err })
      throw err
    } finally {
      clearTimeout(timer)
    }
  }

  async query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<T> {
    const normalized = normalizeServiceName(serviceName)
    const endpoint = `${this.getServiceUrl(serviceName)}/${normalized}${DEFAULT_EVENTS_SUFFIX}`
    return this.runtime.query<T>({
      serviceName: normalized,
      method,
      params,
      opts,
      mapError: (err: any) => (err?.message === "timeout" ? new TimeoutError(serviceName, method, opts?.timeoutMs ?? this.timeoutMs) : err),
      send: async (request) => {
        const res = await this.sendBuffer(endpoint, request.payload, this.codec.contentType, request.encoding, undefined, opts?.timeoutMs)
        let envelope: unknown
        try {
          const decompressed = await this.decompressResponse(res)
          envelope = decompressed.byteLength === 0 ? undefined : this.decodeResponseBody(decompressed, res.headers["content-type"])
        } catch (decodeErr) {
          if (res.status >= 400) throw httpStatusToError(res.status, serviceName, res.headers)
          throw decodeErr
        }
        if (res.status >= 400 && !(envelope as any)?.params?.error) throw httpStatusToError(res.status, serviceName, res.headers)
        return envelope
      }
    })
  }

  private async decompressResponse(res: { body: Uint8Array; headers: Record<string, string> }): Promise<Uint8Array> {
    this.runtime.metrics.observeHistogram(
      NEVO_METRIC_NAMES.payloadBytes,
      { direction: "in", service: this.serviceName ?? "unknown" },
      res.body.byteLength
    )
    const respEncoding = res.headers["content-encoding"]
    const decompressed = shouldDecompressAsync(res.body.byteLength, respEncoding)
      ? await maybeDecompressAsync(res.body, respEncoding, this.maxPayloadBytes)
      : maybeDecompress(res.body, respEncoding, this.maxPayloadBytes)
    enforcePayloadLimit(decompressed, this.maxPayloadBytes)
    return decompressed
  }

  async emit(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    const normalized = normalizeServiceName(serviceName)
    const endpoint = `${this.getServiceUrl(serviceName)}/${normalized}${DEFAULT_EVENTS_SUFFIX}`
    return this.runtime.emit({
      serviceName: normalized,
      method,
      params,
      opts,
      send: async (request) => {
        const res = await this.sendBuffer(endpoint, request.payload, this.codec.contentType, request.encoding)
        if (res.status >= 400) throw httpStatusToError(res.status, serviceName, res.headers)
      }
    })
  }

  async publish(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    const normalized = normalizeServiceName(serviceName)
    const endpoint = `${this.getServiceUrl(serviceName)}/__nevo/publish`
    return this.runtime.emit({
      serviceName: normalized,
      method,
      params,
      opts: { ...opts, headers: { ...(opts?.headers ?? {}), "nevo-service": serviceName } },
      type: "sub",
      send: async (request) => {
        const res = await this.sendBuffer(endpoint, request.payload, this.codec.contentType, request.encoding)
        if (res.status >= 400) throw httpStatusToError(res.status, serviceName, res.headers)
      }
    })
  }

  async broadcast(method: string, params: unknown, opts?: ClientCallOptions): Promise<void> {
    const url = this.discoveryUrl || this.serviceUrls.values().next().value
    if (!url) throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: "No base URL available for broadcast" })
    const endpoint = `${url.replace(/\/+$/, "")}/${DEFAULT_BROADCAST_TOPIC}`
    return this.runtime.emit({
      serviceName: DEFAULT_BROADCAST_TOPIC,
      method,
      params,
      opts,
      type: "broadcast",
      send: async (request) => {
        const res = await this.sendBuffer(endpoint, request.payload, this.codec.contentType, request.encoding)
        if (res.status >= 400) throw httpStatusToError(res.status, DEFAULT_BROADCAST_TOPIC, res.headers)
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
    const baseUrl = isBroadcast ? this.discoveryUrl || this.serviceUrls.values().next().value : this.getServiceUrl(serviceName)
    if (!baseUrl) throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, { message: "No base URL available for subscription" })

    const endpoint = isBroadcast
      ? `${baseUrl.replace(/\/+$/, "")}/${DEFAULT_BROADCAST_TOPIC}`
      : `${baseUrl.replace(/\/+$/, "")}/__nevo/subscribe?service=${encodeURIComponent(serviceName)}`

    const controller = new AbortController()
    const signal = controller.signal
    this.streamAborts.add(controller)

    const connect = async (): Promise<ReadableStreamDefaultReader<Uint8Array>> => {
      const response = await fetch(endpoint, {
        headers: { Accept: "text/event-stream" },
        signal,
        ...(this.undiciDispatcher ? { dispatcher: this.undiciDispatcher } : {})
      } as RequestInit)
      if (!response.ok || !response.body) {
        throw new MessagingError(ErrorCode.SERVICE_UNAVAILABLE, { message: `Failed to subscribe via SSE: ${response.status}` })
      }
      return response.body.getReader()
    }

    // No frame (message or heartbeat) within this window means the connection is
    // dead (half-open TCP); tear it down so readLoop reconnects.
    const idleMs = this.sseIdleTimeoutMs
    type ReadResult = Awaited<ReturnType<ReadableStreamDefaultReader<Uint8Array>["read"]>>
    const readWithIdle = (reader: ReadableStreamDefaultReader<Uint8Array>): Promise<ReadResult> =>
      new Promise((resolve, reject) => {
        const t = setTimeout(() => {
          void reader.cancel().catch(() => {})
          reject(new MessagingError(ErrorCode.TIMEOUT, { message: `SSE idle timeout after ${idleMs}ms`, retryable: true }))
        }, idleMs)
        if (typeof t.unref === "function") t.unref()
        reader.read().then(
          (r) => {
            clearTimeout(t)
            resolve(r)
          },
          (e) => {
            clearTimeout(t)
            reject(e)
          }
        )
      })

    const consume = async (reader: ReadableStreamDefaultReader<Uint8Array>) => {
      const decoder = new TextDecoder()
      let buffer = ""
      for (;;) {
        const { value, done } = await readWithIdle(reader)
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const parts = buffer.split("\n\n")
        buffer = parts.pop() || ""
        for (const part of parts) {
          const line = part.split("\n").find((l: string) => l.startsWith("data:"))
          if (!line) continue
          const raw = line.replace(/^data:\s*/, "")
          let payload: any
          try {
            payload = parseWithBigInt(raw)
          } catch {
            continue
          }
          if (method && payload.method !== method && parseMethod(payload.method ?? "").name !== method) continue
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

    const initialReader = await connect()

    const readLoop = async (firstReader: ReadableStreamDefaultReader<Uint8Array>) => {
      let reader: ReadableStreamDefaultReader<Uint8Array> | null = firstReader
      for (let attempt = 0; ; ) {
        if (reader) {
          try {
            await consume(reader)
            if (!signal.aborted) this.logger.warn({ event: "http.sub.stream_closed", service: serviceName })
          } catch (err: any) {
            if (signal.aborted || isAbortError(err)) return
            this.logger.error({ event: "http.sub.read_error", service: serviceName, err: err?.message })
          }
          reader = null
        }
        if (signal.aborted) return
        attempt++
        await sleepUnlessAborted(reconnectDelayMs(attempt), signal)
        if (signal.aborted) return
        try {
          reader = await connect()
          attempt = 0
          this.logger.info({ event: "http.sub.reconnected", service: serviceName })
        } catch (err: any) {
          if (signal.aborted || isAbortError(err)) return
          this.logger.warn({ event: "http.sub.reconnect_failed", service: serviceName, attempt, err: err?.message })
        }
      }
    }
    readLoop(initialReader).catch((err) => this.logger.error({ event: "http.sub.loop_crashed", err: (err as Error)?.message }))

    return {
      unsubscribe: async () => {
        this.streamAborts.delete(controller)
        controller.abort()
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

  private async initDiscovery(): Promise<void> {
    if (!this.discoveryUrl) return
    this.discoveryAbort = new AbortController()
    const signal = this.discoveryAbort.signal
    const discoveryEndpoint = `${this.discoveryUrl.replace(/\/+$/, "")}/${DEFAULT_DISCOVERY_TOPIC}`

    const listenLoop = async () => {
      for (let attempt = 0; ; ) {
        try {
          const response = await fetch(discoveryEndpoint, {
            headers: { Accept: "text/event-stream" },
            signal,
            ...(this.undiciDispatcher ? { dispatcher: this.undiciDispatcher } : {})
          } as RequestInit)
          if (!response.ok || !response.body) {
            throw new MessagingError(ErrorCode.SERVICE_UNAVAILABLE, { message: `Discovery stream returned HTTP ${response.status}` })
          }
          attempt = 0
          const reader = response.body.getReader()
          const decoder = new TextDecoder()
          let buffer = ""
          for (;;) {
            const { value, done } = await reader.read()
            if (done) break
            buffer += decoder.decode(value, { stream: true })
            const parts = buffer.split("\n\n")
            buffer = parts.pop() || ""
            for (const part of parts) {
              const line = part.split("\n").find((l: string) => l.startsWith("data:"))
              if (!line) continue
              const raw = line.replace(/^data:\s*/, "")
              try {
                const payload = parseWithBigInt(raw) as DiscoveryAnnouncement
                if (payload?.serviceName) this.discoveryRegistry.update(payload)
              } catch {}
            }
          }
        } catch (err: any) {
          if (signal.aborted || isAbortError(err)) return
          this.logger.warn({ event: "http.discovery.connect_failed", err: err?.message })
        }
        if (signal.aborted) return
        attempt++
        await sleepUnlessAborted(reconnectDelayMs(attempt), signal)
        if (signal.aborted) return
      }
    }
    void listenLoop()

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
    for (const controller of this.streamAborts) controller.abort()
    this.streamAborts.clear()
    try {
      await this.undiciDispatcher?.close()
    } catch {}
    this.httpAgent.destroy()
    this.httpsAgent.destroy()
    await this.runtime.close(timeoutMs)
  }
}

function isAbortError(err: unknown): boolean {
  return (err as Error)?.name === "AbortError"
}

function reconnectDelayMs(attempt: number): number {
  return Math.min(30_000, 1000 * 2 ** Math.min(attempt - 1, 5))
}

function sleepUnlessAborted(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const onAbort = () => {
      clearTimeout(timer)
      resolve()
    }
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort)
      resolve()
    }, ms)
    signal.addEventListener("abort", onAbort, { once: true })
  })
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
