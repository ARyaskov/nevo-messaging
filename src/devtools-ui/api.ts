import type { IncomingMessage, ServerResponse } from "node:http"
import { getDevToolsBus } from "../common/devtools"
import { getDevToolsRegistry } from "../common/devtools-registry"
import { configureSource, getRegistry, snapshot, sourceHealth } from "./source"

const SSE_KEEP_ALIVE_MS = 15000
const MAX_BODY_BYTES = 1_000_000

function sendJson(res: ServerResponse, status: number, payload: unknown): void {
  const body = JSON.stringify(payload)
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "content-length": Buffer.byteLength(body)
  })
  res.end(body)
}

async function readJsonBody<T>(req: IncomingMessage): Promise<T | null> {
  const chunks: Buffer[] = []
  let size = 0
  for await (const chunk of req) {
    size += (chunk as Buffer).length
    if (size > MAX_BODY_BYTES) return null
    chunks.push(chunk as Buffer)
  }
  if (chunks.length === 0) return null
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8")) as T
  } catch {
    return null
  }
}

function intParam(url: URL, name: string, fallback: number): number {
  const raw = url.searchParams.get(name)
  if (!raw) return fallback
  const n = Number.parseInt(raw, 10)
  return Number.isFinite(n) && n > 0 ? n : fallback
}

/** Server-Sent Events stream of every event landing on the bus. */
function handleEvents(req: IncomingMessage, res: ServerResponse): void {
  const bus = configureSource()

  res.writeHead(200, {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive",
    // Defeats proxy buffering, which otherwise makes the stream look frozen.
    "x-accel-buffering": "no"
  })
  // Flush headers immediately so EventSource fires `onopen` without waiting
  // for the first event.
  res.write(": connected\n\n")

  let closed = false
  const off = bus.on((event) => {
    if (closed) return
    res.write(`data: ${JSON.stringify(event)}\n\n`)
  })
  const keepAlive = setInterval(() => {
    if (!closed) res.write(": keep-alive\n\n")
  }, SSE_KEEP_ALIVE_MS)

  const cleanup = () => {
    if (closed) return
    closed = true
    off()
    clearInterval(keepAlive)
  }

  req.on("close", cleanup)
  res.on("close", cleanup)
  res.on("error", cleanup)
}

interface ReplayBody {
  service?: string
  method?: string
  params?: unknown
  uuid?: string
}

interface ConfigUpdate {
  service?: string
  accessControl?: {
    rules?: Array<{ topic?: string; method?: string; allow?: string[]; deny?: string[] }>
    allowAllByDefault?: boolean
    logDenied?: boolean
  }
}

/**
 * Routes `/api/*`. Returns false when the path is not an API route, so the
 * caller can fall through to the static handler.
 */
export async function handleApi(req: IncomingMessage, res: ServerResponse, url: URL): Promise<boolean> {
  if (!url.pathname.startsWith("/api/")) return false

  const method = req.method ?? "GET"

  switch (url.pathname) {
    case "/api/events": {
      if (method !== "GET") return sendJson(res, 405, { ok: false, error: "method not allowed" }), true
      handleEvents(req, res)
      return true
    }

    case "/api/snapshot": {
      configureSource()
      sendJson(res, 200, { events: snapshot(intParam(url, "limit", 500)) })
      return true
    }

    case "/api/registry": {
      const reg = getRegistry()
      sendJson(res, 200, { services: reg.listServices(), circuits: reg.listCircuits() })
      return true
    }

    case "/api/circuits": {
      sendJson(res, 200, { circuits: getRegistry().listCircuits() })
      return true
    }

    case "/api/health": {
      sendJson(res, 200, { ok: true, ...sourceHealth() })
      return true
    }

    case "/api/replay": {
      if (method !== "POST") return sendJson(res, 405, { ok: false, error: "method not allowed" }), true
      const body = await readJsonBody<ReplayBody>(req)
      if (!body?.service || !body.method) {
        sendJson(res, 400, { ok: false, error: "missing service/method" })
        return true
      }
      getDevToolsBus().publish({
        ts: Date.now(),
        type: "custom",
        service: body.service,
        method: body.method,
        extra: { kind: "replay-requested", originalUuid: body.uuid, params: body.params }
      })
      sendJson(res, 200, { ok: true, queued: true })
      return true
    }

    case "/api/config": {
      const reg = getDevToolsRegistry()

      if (method === "GET") {
        const service = url.searchParams.get("service")
        if (!service) {
          sendJson(res, 200, { services: reg.listServices() })
          return true
        }
        const info = reg.getService(service)
        if (!info) sendJson(res, 404, { ok: false, error: "unknown service" })
        else sendJson(res, 200, { service: info })
        return true
      }

      if (method !== "POST") return sendJson(res, 405, { ok: false, error: "method not allowed" }), true

      const body = await readJsonBody<ConfigUpdate>(req)
      if (!body?.service) {
        sendJson(res, 400, { ok: false, error: "missing service" })
        return true
      }
      const info = reg.getService(body.service)
      if (!info) {
        sendJson(res, 404, { ok: false, error: "unknown service" })
        return true
      }
      if (body.accessControl) reg.registerService({ ...info, accessControl: body.accessControl })
      sendJson(res, 200, { ok: true, applied: { service: body.service } })
      return true
    }

    default:
      sendJson(res, 404, { ok: false, error: "unknown endpoint" })
      return true
  }
}
