import * as http2 from "node:http2"
import {
  DEFAULT_EVENTS_SUFFIX,
  MessagingError,
  TimeoutError,
  ErrorCode,
  Codec,
  JsonCodec,
  MessagePackCodec,
  TransportClientOptions,
  ClientRuntime,
  type ClientCallOptions,
  type EncodedRequest,
  httpStatusToError,
  normalizeServiceName
} from "../../common"

export interface NevoHttp2ClientOptions extends TransportClientOptions {
  timeoutMs?: number
}

type SessionEntry = { session: http2.ClientHttp2Session; url: URL; closed: boolean }

export class NevoHttp2Client {
  private readonly runtime: ClientRuntime
  private readonly serviceUrls: Map<string, string>
  private readonly sessions = new Map<string, SessionEntry>()

  constructor(serviceUrls: Record<string, string>, options?: NevoHttp2ClientOptions) {
    this.serviceUrls = new Map(Object.entries(serviceUrls).map(([k, v]) => [k.toLowerCase(), v]))
    this.runtime = new ClientRuntime(options, { transport: "http2", fallbackCodec: tryMsgpackOrJson })
  }

  getInstanceId(): string {
    return this.runtime.instanceId
  }

  private async getSession(serviceName: string): Promise<SessionEntry> {
    const normalized = normalizeServiceName(serviceName)
    const url = this.serviceUrls.get(normalized)
    if (!url) {
      throw new MessagingError(ErrorCode.SERVICE_NOT_FOUND, {
        message: `Service "${serviceName}" is not registered`,
        availableServices: this.serviceUrls.keys().toArray()
      })
    }
    const existing = this.sessions.get(normalized)
    if (existing && !existing.session.destroyed && !existing.session.closed) return existing
    const u = new URL(url)
    const session = http2.connect(u.origin)
    const entry: SessionEntry = { session, url: u, closed: false }
    this.sessions.set(normalized, entry)
    session.on("close", () => {
      entry.closed = true
    })
    session.on("error", (err) => {
      this.runtime.logger.warn({ event: "http2.session.error", err: err.message })
    })
    return entry
  }

  async query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: ClientCallOptions): Promise<T> {
    const normalized = normalizeServiceName(serviceName)
    return this.runtime.query<T>({
      serviceName: normalized,
      method,
      params,
      opts,
      send: (request) => this.sendOverStream(normalized, serviceName, method, request, opts)
    })
  }

  private async sendOverStream(
    normalized: string,
    serviceName: string,
    method: string,
    request: EncodedRequest,
    opts?: ClientCallOptions
  ): Promise<unknown> {
    const entry = await this.getSession(serviceName)
    const timeout = opts?.timeoutMs ?? this.runtime.timeoutMs
    const path = `${entry.url.pathname.replace(/\/+$/, "")}/${normalized}${DEFAULT_EVENTS_SUFFIX}`
    const headers: http2.OutgoingHttpHeaders = {
      ":method": "POST",
      ":path": path,
      "content-type": this.runtime.codec.contentType,
      "content-length": String(request.payload.byteLength),
      accept: this.runtime.codec.contentType
    }
    if (request.encoding !== "identity") headers["content-encoding"] = request.encoding

    const { promise, resolve, reject } = Promise.withResolvers<unknown>()
    const stream = entry.session.request(headers)
    const timer = setTimeout(() => {
      stream.close(http2.constants.NGHTTP2_CANCEL)
      reject(new TimeoutError(serviceName, method, timeout))
    }, timeout)

    const chunks: Buffer[] = []
    let received = 0
    let respEncoding: string | undefined
    let status = 0

    stream.on("response", (h) => {
      status = Number(h[":status"]) || 0
      const declared = Number(h["content-length"])
      if (Number.isFinite(declared) && declared > this.runtime.maxPayloadBytes) {
        clearTimeout(timer)
        stream.close(http2.constants.NGHTTP2_CANCEL)
        reject(
          new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, {
            message: `Response content-length ${declared}B exceeds ${this.runtime.maxPayloadBytes}B`,
            size: declared,
            limit: this.runtime.maxPayloadBytes
          })
        )
        return
      }
      const v = h["content-encoding"]
      respEncoding = typeof v === "string" ? v : Array.isArray(v) ? v[0] : undefined
    })
    stream.on("data", (c: Buffer) => {
      received += c.length
      if (received > this.runtime.maxPayloadBytes) {
        clearTimeout(timer)
        stream.close(http2.constants.NGHTTP2_CANCEL)
        reject(
          new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, {
            message: `Response body exceeds ${this.runtime.maxPayloadBytes}B`,
            limit: this.runtime.maxPayloadBytes
          })
        )
        return
      }
      chunks.push(c)
    })
    stream.on("end", () => {
      clearTimeout(timer)
      void (async () => {
        try {
          const body = Buffer.concat(chunks, received)
          const envelope = body.byteLength === 0 ? undefined : await this.runtime.decodeAsync(body, respEncoding)
          if (status >= 400 && !(envelope as any)?.params?.error) {
            reject(httpStatusToError(status, serviceName))
            return
          }
          resolve(envelope)
        } catch (err: any) {
          if (status >= 400) {
            reject(httpStatusToError(status, serviceName))
            return
          }
          reject(err instanceof MessagingError ? err : new MessagingError(ErrorCode.PARSE_ERROR, { message: err?.message ?? "decode failed" }))
        }
      })()
    })
    stream.on("error", (err: any) => {
      clearTimeout(timer)
      reject(
        err instanceof MessagingError ? err : new MessagingError(ErrorCode.CONNECTION_LOST, { message: err?.message ?? "stream error" }, serviceName)
      )
    })
    stream.end(request.payload)
    return promise
  }

  getAvailableServices(): string[] {
    return this.serviceUrls.keys().toArray()
  }

  async close(timeoutMs = 30_000): Promise<void> {
    for (const [, entry] of this.sessions) {
      entry.closed = true
      try {
        entry.session.close()
      } catch {}
    }
    this.sessions.clear()
    await this.runtime.close(timeoutMs)
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
