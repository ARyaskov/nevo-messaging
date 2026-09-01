import { createServer, type Server } from "node:http"
import { handleApi } from "./api"
import { assertAssetsPresent, handleStatic } from "./static"
import { configureSource, teardownSource, type DevToolsUiSourceOptions } from "./source"

export const DEFAULT_DEVTOOLS_UI_PORT = 3499

export interface DevToolsUiServerOptions extends DevToolsUiSourceOptions {
  port?: number
  /** Defaults to 127.0.0.1: the dashboard exposes service topology and ACL rules. */
  host?: string
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

  const server = createServer((req, res) => {
    const url = new URL(req.url ?? "/", `http://${req.headers.host ?? `${host}:${port}`}`)

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

  // SSE clients hold the socket open indefinitely; the default 5s headers timeout
  // and 0 request timeout are fine, but keep-alive must not reap live streams.
  server.keepAliveTimeout = 0
  server.headersTimeout = 0

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
