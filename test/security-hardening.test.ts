import { test } from "node:test"
import assert from "node:assert/strict"
import { createServer, request, type Server } from "node:http"
import { createServer as createNetServer } from "node:net"
import { PgOutboxStore, PgInboxStore, type PgClient } from "../src/common/pg-stores"
import { SqliteOutboxStore } from "../src/common/sqlite-outbox"
import { PgAuditSink } from "../src/common/audit-log"
import { createNevoHttpGateway } from "../src/transports/http/gateway.middleware"
import { createDevToolsRequestGuard } from "../src/devtools-ui/server"
import { handleApi } from "../src/devtools-ui/api"
import { SocketSignalRouter } from "../src/transports/socket-io/socket.signal-router.decorator"
import { bindSignalRouterForTesting } from "../src/signal-router.utils"

const noopPg: PgClient = { query: async () => ({ rows: [] }) }

test("pg stores reject table/schema names that are not plain identifiers", () => {
  assert.throws(() => new PgOutboxStore({ client: noopPg, table: 'x"; DROP TABLE users; --' }), /Invalid SQL identifier/)
  assert.throws(() => new PgInboxStore({ client: noopPg, schema: "a b" }), /Invalid SQL identifier/)
  assert.doesNotThrow(() => new PgOutboxStore({ client: noopPg, schema: "tenant_1", table: "nevo_outbox_v2" }))
})

test("sqlite outbox rejects an injected tableName", () => {
  assert.throws(() => new SqliteOutboxStore({ tableName: "t; DROP TABLE x" }), /Invalid SQL identifier/)
})

test("pg audit sink accepts schema-qualified tables but rejects injection", () => {
  assert.doesNotThrow(() => new PgAuditSink({ client: noopPg, table: "audit.nevo_audit" }))
  assert.throws(() => new PgAuditSink({ client: noopPg, table: "nevo_audit (uuid) VALUES ('x'); --" }), /Invalid SQL identifier/)
  assert.throws(() => new PgAuditSink({ client: noopPg, table: "a.b.c" }), /Invalid SQL identifier/)
})

function runGateway(cors: Parameters<typeof createNevoHttpGateway>[0]["cors"], origin?: string) {
  const headers: Record<string, string> = {}
  const res = { statusCode: 200, setHeader: (k: string, v: string) => (headers[k] = v), end: () => {} }
  createNevoHttpGateway({ cors })({ method: "GET", headers: origin ? { origin } : {} }, res, () => {})
  return headers
}

test("gateway CORS reflects only an allow-listed origin", () => {
  const cors = { origin: ["https://a.example", "https://b.example"], credentials: true }
  assert.equal(runGateway(cors, "https://b.example")["access-control-allow-origin"], "https://b.example")
  assert.equal(runGateway(cors, "https://b.example")["vary"], "Origin")
  assert.equal(runGateway(cors, "https://evil.example")["access-control-allow-origin"], undefined)
  assert.equal(runGateway(cors)["access-control-allow-origin"], undefined)
})

test("gateway CORS never pairs a wildcard with credentials", () => {
  assert.equal(runGateway({ origin: "*" }, "https://x.example")["access-control-allow-origin"], "*")
  assert.equal(runGateway({ origin: "*", credentials: true }, "https://x.example")["access-control-allow-origin"], undefined)
})

async function startDevToolsApi(): Promise<{ server: Server; port: number }> {
  const guard = createDevToolsRequestGuard("127.0.0.1")
  const server = createServer((req, res) => {
    if (!guard(req, res)) return
    const url = new URL(req.url ?? "/", "http://127.0.0.1")
    void handleApi(req, res, url).then((handled) => {
      if (!handled) res.writeHead(404).end()
    })
  })
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))
  return { server, port: (server.address() as { port: number }).port }
}

function send(port: number, opts: { method?: string; path: string; headers?: Record<string, string>; body?: string }) {
  return new Promise<{ status: number; headers: Record<string, unknown> }>((resolve, reject) => {
    const req = request({ host: "127.0.0.1", port, method: opts.method ?? "GET", path: opts.path, headers: opts.headers }, (res) => {
      res.resume()
      res.on("end", () => resolve({ status: res.statusCode ?? 0, headers: res.headers }))
    })
    req.on("error", reject)
    req.end(opts.body)
  })
}

test("devtools API rejects DNS-rebinding hosts, cross-site posts and non-JSON bodies", async () => {
  const { server, port } = await startDevToolsApi()
  const body = JSON.stringify({ service: "user", method: "user.get" })
  try {
    const ok = await send(port, { path: "/api/health", headers: { host: `localhost:${port}` } })
    assert.equal(ok.status, 200)
    assert.equal(ok.headers["x-frame-options"], "DENY")

    assert.equal((await send(port, { path: "/api/health", headers: { host: `attacker.example:${port}` } })).status, 421)

    const crossSite = await send(port, {
      method: "POST",
      path: "/api/replay",
      headers: { host: `127.0.0.1:${port}`, origin: "https://attacker.example", "content-type": "application/json" },
      body
    })
    assert.equal(crossSite.status, 403)

    const simpleRequest = await send(port, {
      method: "POST",
      path: "/api/replay",
      headers: { host: `127.0.0.1:${port}`, "content-type": "text/plain" },
      body
    })
    assert.equal(simpleRequest.status, 415)

    const sameOrigin = await send(port, {
      method: "POST",
      path: "/api/replay",
      headers: { host: `127.0.0.1:${port}`, origin: `http://127.0.0.1:${port}`, "content-type": "application/json" },
      body
    })
    assert.equal(sameOrigin.status, 200)
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()))
  }
})

async function freePort(): Promise<number> {
  const srv = createNetServer()
  await new Promise<void>((resolve) => srv.listen(0, "127.0.0.1", resolve))
  const port = (srv.address() as { port: number }).port
  await new Promise<void>((resolve) => srv.close(() => resolve()))
  return port
}

test("socket.io router refuses explicit joins into user:* rooms", async () => {
  class UserService {}
  class Ctrl {
    svc = new UserService()
  }
  const port = await freePort()
  SocketSignalRouter([UserService], { serviceName: "user", port })(Ctrl)
  bindSignalRouterForTesting(Ctrl, { devtools: false, tracing: { enabled: false } })
  const ctrl: any = new Ctrl()
  await ctrl.onModuleInit()
  if (!ctrl.socketHttpServer.listening) await new Promise<void>((resolve) => ctrl.socketHttpServer.once("listening", resolve))

  const { io: connect } = await import("socket.io-client")
  const client = connect(`http://127.0.0.1:${port}`, { transports: ["websocket"] })
  try {
    await new Promise<void>((resolve, reject) => {
      client.once("connect", () => resolve())
      client.once("connect_error", reject)
    })
    client.emit("nevo:subscribe", { serviceName: "user", room: "user:victim" })
    client.emit("nevo:subscribe", { serviceName: "user", room: "dashboard" })
    await new Promise((resolve) => setTimeout(resolve, 150))
    const rooms: Map<string, Set<string>> = ctrl.socketServer.sockets.adapter.rooms
    assert.equal(rooms.has("user:victim"), false)
    assert.equal(rooms.has("dashboard"), true)
  } finally {
    client.close()
    await ctrl.onModuleDestroy()
  }
})
