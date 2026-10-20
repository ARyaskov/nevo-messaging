import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http"
import { handleApi } from "./api"
import { assertAssetsPresent, handleStatic } from "./static"
import { configureSource, teardownSource, type DevToolsUiSourceOptions } from "./source"

export const DEFAULT_DEVTOOLS_UI_PORT = 3499

export interface DevToolsUiServerOptions extends DevToolsUiSourceOptions {
  port?: number
  /** Defaults to 127.0.0.1: the dashboard exposes service topology and ACL rules. */
  host?: string
  /**
   * Host header values (hostname only) the dashboard answers to. Defaults to the
   * loopback names when bound to loopback, which blocks DNS-rebinding; any host otherwise.
   */
  allowedHosts?: string[]
}

const LOOPBACK_HOSTS = ["localhost", "127.0.0.1", "::1"]

function hostnameOf(hostHeader: string | undefined): string | null {
  if (!hostHeader) return null
  try {
    return new URL(`http://${hostHeader}`).hostname.replace(/^\[|\]$/g, "").toLowerCase()
  } catch {
    return null
  }
}

function resolveAllowedHosts(bindHost: string, allowedHosts?: string[]): Set<string> | null {
  if (allowedHosts) return new Set(allowedHosts.map((h) => h.toLowerCase()))
  if (LOOPBACK_HOSTS.includes(bindHost.toLowerCase())) return new Set(LOOPBACK_HOSTS)
  return null
}

function isCrossOrigin(origin: string | undefined, hostHeader: string | undefined): boolean {
  if (!origin) return false
  try {
    return new URL(origin).host !== hostHeader
  } catch {
    return true
  }
}

const SECURITY_HEADERS: Record<string, string> = {
  "x-content-type-options": "nosniff",
  "x-frame-options": "DENY",
  "content-security-policy": "frame-ancestors 'none'",
  "referrer-policy": "no-referrer"
}

/** Sets security headers and rejects rebinding/cross-site requests. Returns false when it already responded. */
export function createDevToolsRequestGuard(bindHost: string, allowedHosts?: string[]): (req: IncomingMessage, res: ServerResponse) => boolean {
  const allowed = resolveAllowedHosts(bindHost, allowedHosts)
  return (req, res) => {
    for (const [name, value] of Object.entries(SECURITY_HEADERS)) res.setHeader(name, value)

    const hostname = hostnameOf(req.headers.host)
    if (allowed && (!hostname || !allowed.has(hostname))) {
      res.writeHead(421, { "content-type": "text/plain; charset=utf-8" })
      res.end("Misdirected request")
      return false
    }
    const method = req.method ?? "GET"
    if (method !== "GET" && method !== "HEAD" && isCrossOrigin(req.headers.origin, req.headers.host)) {
      res.writeHead(403, { "content-type": "application/json; charset=utf-8" })
      res.end(JSON.stringify({ ok: false, error: "cross-origin request rejected" }))
      return false
    }
    return true
  }
}

export interface DevToolsUiServerHandle {
  server: Server
  port: number
  host: string
  url: string
  close: () => Promise<void>
}

/**
 * Starts the DevTools dashboard on a plain `node:http` server.
 *
 * Two ways to feed it:
 *  - pass `natsServers` and it ingests events published by every wired service;
 *  - mount it inside a service process and it reads that process's own bus.
 */
export async function startDevToolsUiServer(opts: DevToolsUiServerOptions = {}): Promise<DevToolsUiServerHandle> {
  await assertAssetsPresent()

  const port = opts.port ?? DEFAULT_DEVTOOLS_UI_PORT
  const host = opts.host ?? "127.0.0.1"

  configureSource(opts)

  const guard = createDevToolsRequestGuard(host, opts.allowedHosts)

  const server = createServer((req, res) => {
    if (!guard(req, res)) return

    let url: URL
    try {
      url = new URL(req.url ?? "/", `http://${req.headers.host ?? `${host}:${port}`}`)
    } catch {
      res.writeHead(400, { "content-type": "text/plain; charset=utf-8" })
      res.end("Bad request")
      return
    }

    void (async () => {
      try {
        if (await handleApi(req, res, url)) return
        await handleStatic(res, url.pathname)
      } catch (err) {
        if (res.headersSent) {
          res.end()
          return
        }
        const message = err instanceof Error ? err.message : String(err)
        res.writeHead(500, { "content-type": "application/json; charset=utf-8" })
        res.end(JSON.stringify({ ok: false, error: message }))
      }
    })()
  })

  // keepAliveTimeout only applies between requests, so live SSE streams are not reaped by it;
  // a bounded headersTimeout keeps slow-header clients from pinning sockets.
  server.headersTimeout = 30_000

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(port, host, () => {
      server.removeListener("error", reject)
      resolve()
    })
  })

  const actualPort = (server.address() as { port: number } | null)?.port ?? port

  return {
    server,
    port: actualPort,
    host,
    url: `http://${host === "0.0.0.0" ? "127.0.0.1" : host}:${actualPort}`,
    close: async () => {
      await teardownSource()
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  }
}
