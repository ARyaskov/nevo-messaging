import { test } from "node:test"
import assert from "node:assert/strict"
import * as http from "node:http"
import type { AddressInfo } from "node:net"
import { NevoHttpClient } from "../src/transports/http/nevo-http.client"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"
import { JsonCodec } from "../src/common/codec"

type Stub = { url: string; requests: () => number; close: () => Promise<void> }

// Spins up a throwaway HTTP server whose every request is handed to `onReq` once its
// body has been drained. `requests()` reports how many times it was hit (for retries).
async function startStub(onReq: (req: http.IncomingMessage, res: http.ServerResponse) => void): Promise<Stub> {
  let count = 0
  const server = http.createServer((req, res) => {
    count++
    req.on("data", () => {})
    req.on("end", () => onReq(req, res))
  })
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()))
  const port = (server.address() as AddressInfo).port
  return {
    url: `http://127.0.0.1:${port}`,
    requests: () => count,
    close: () => new Promise<void>((resolve) => { server.closeAllConnections?.(); server.close(() => resolve()) })
  }
}

async function expectCode(p: Promise<unknown>, code: ErrorCode): Promise<void> {
  await assert.rejects(p, (err: unknown) => {
    assert.ok(err instanceof MessagingError, `expected MessagingError, got ${String(err)}`)
    assert.equal((err as MessagingError).code, code)
    return true
  })
}

test("query throws a mapped MessagingError (not undefined) on error statuses", async () => {
  const cases: Array<{ status: number; code: ErrorCode }> = [
    { status: 503, code: ErrorCode.SERVICE_UNAVAILABLE },
    { status: 502, code: ErrorCode.SERVICE_UNAVAILABLE },
    { status: 504, code: ErrorCode.TIMEOUT },
    { status: 413, code: ErrorCode.PAYLOAD_TOO_LARGE },
    { status: 500, code: ErrorCode.REMOTE_ERROR },
    { status: 404, code: ErrorCode.REMOTE_ERROR }
  ]
  for (const { status, code } of cases) {
    const stub = await startStub((_req, res) => { res.writeHead(status); res.end() })
    const client = new NevoHttpClient({ svc: stub.url }, { codec: new JsonCodec(), retry: { enabled: false } })
    try {
      await expectCode(client.query("svc", "m", {}), code)
    } finally {
      await client.close()
      await stub.close()
    }
  }
})

test("an error status with an undecodable body still maps to the status, not a parse error", async () => {
  const stub = await startStub((_req, res) => { res.writeHead(503, { "content-type": "text/plain" }); res.end("upstream down") })
  const client = new NevoHttpClient({ svc: stub.url }, { codec: new JsonCodec(), retry: { enabled: false } })
  try {
    await expectCode(client.query("svc", "m", {}), ErrorCode.SERVICE_UNAVAILABLE)
  } finally {
    await client.close()
    await stub.close()
  }
})

test("only 502/503/504 are retried; 500 is terminal", async () => {
  const retry = { maxAttempts: 3, baseMs: 1, maxMs: 2, jitter: false }

  const s503 = await startStub((_req, res) => { res.writeHead(503); res.end() })
  const c503 = new NevoHttpClient({ svc: s503.url }, { codec: new JsonCodec(), retry })
  try {
    await assert.rejects(() => c503.query("svc", "m", {}))
    assert.equal(s503.requests(), 3, "503 should be retried up to maxAttempts")
  } finally {
    await c503.close()
    await s503.close()
  }

  const s500 = await startStub((_req, res) => { res.writeHead(500); res.end() })
  const c500 = new NevoHttpClient({ svc: s500.url }, { codec: new JsonCodec(), retry })
  try {
    await assert.rejects(() => c500.query("svc", "m", {}))
    assert.equal(s500.requests(), 1, "500 must not be retried")
  } finally {
    await c500.close()
    await s500.close()
  }
})

test("a 2xx nevo envelope is still returned as the result (happy path intact)", async () => {
  const codec = new JsonCodec()
  const stub = await startStub((_req, res) => {
    const body = Buffer.from(codec.encode({ uuid: "u", method: "m@1.0.0", params: { result: { ok: true } }, meta: {} }))
    res.writeHead(200, { "content-type": codec.contentType })
    res.end(body)
  })
  const client = new NevoHttpClient({ svc: stub.url }, { codec: new JsonCodec(), retry: { enabled: false } })
  try {
    const r = await client.query<{ ok: boolean }>("svc", "m", {})
    assert.deepEqual(r, { ok: true })
  } finally {
    await client.close()
    await stub.close()
  }
})

test("an overall deadline fires even while the response keeps trickling", { timeout: 5000 }, async () => {
  // Headers arrive, then a byte every 20ms and the response never ends. req.setTimeout
  // (socket inactivity) keeps resetting, so only the wall-clock deadline can end this.
  const stub = await startStub((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" })
    res.on("error", () => {})
    const iv = setInterval(() => {
      if (res.writableEnded || res.destroyed) { clearInterval(iv); return }
      res.write("x")
    }, 20)
    res.on("close", () => clearInterval(iv))
  })
  const client = new NevoHttpClient({ svc: stub.url }, { codec: new JsonCodec(), retry: { enabled: false }, timeoutMs: 150 })
  try {
    await expectCode(client.query("svc", "m", {}), ErrorCode.TIMEOUT)
  } finally {
    await client.close()
    await stub.close()
  }
})
