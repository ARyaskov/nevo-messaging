import { test } from "node:test"
import assert from "node:assert/strict"
import * as http2 from "node:http2"
import type { AddressInfo } from "node:net"
import { NevoHttp2Client } from "../src/transports/http2/nevo-http2.client"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"
import { JsonCodec } from "../src/common/codec"

type Stub = { url: string; close: () => Promise<void> }

// A throwaway cleartext (h2c) HTTP/2 server; each request stream is handed to `onReq`
// once its body has been drained.
async function startStubH2(onReq: (stream: http2.ServerHttp2Stream) => void): Promise<Stub> {
  const server = http2.createServer()
  server.on("stream", (stream: http2.ServerHttp2Stream) => {
    stream.on("error", () => {})
    stream.on("data", () => {})
    stream.on("end", () => onReq(stream))
  })
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()))
  const port = (server.address() as AddressInfo).port
  return {
    url: `http://127.0.0.1:${port}`,
    close: () => new Promise<void>((resolve) => { server.close(() => resolve()) })
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
    { status: 504, code: ErrorCode.TIMEOUT },
    { status: 413, code: ErrorCode.PAYLOAD_TOO_LARGE },
    { status: 500, code: ErrorCode.REMOTE_ERROR }
  ]
  for (const { status, code } of cases) {
    const stub = await startStubH2((stream) => { stream.respond({ ":status": status }); stream.end() })
    const client = new NevoHttp2Client({ svc: stub.url }, { codec: new JsonCodec(), retry: { enabled: false } })
    try {
      await expectCode(client.query("svc", "m", {}), code)
    } finally {
      await client.close()
      await stub.close()
    }
  }
})

test("a raw stream error is wrapped as CONNECTION_LOST", async () => {
  const stub = await startStubH2((stream) => { stream.close(http2.constants.NGHTTP2_INTERNAL_ERROR) })
  const client = new NevoHttp2Client({ svc: stub.url }, { codec: new JsonCodec(), retry: { enabled: false } })
  try {
    await expectCode(client.query("svc", "m", {}), ErrorCode.CONNECTION_LOST)
  } finally {
    await client.close()
    await stub.close()
  }
})

test("a 2xx nevo envelope is still returned as the result (happy path intact)", async () => {
  const codec = new JsonCodec()
  const stub = await startStubH2((stream) => {
    const body = Buffer.from(codec.encode({ uuid: "u", method: "m@1.0.0", params: { result: { ok: true } }, meta: {} }))
    stream.respond({ ":status": 200, "content-type": codec.contentType })
    stream.end(body)
  })
  const client = new NevoHttp2Client({ svc: stub.url }, { codec: new JsonCodec(), retry: { enabled: false } })
  try {
    const r = await client.query<{ ok: boolean }>("svc", "m", {})
    assert.deepEqual(r, { ok: true })
  } finally {
    await client.close()
    await stub.close()
  }
})
