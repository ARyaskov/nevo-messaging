import { test } from "node:test"
import assert from "node:assert/strict"
import { ClientRuntime, type EncodedRequest } from "../src/common/client-runtime"
import { JsonCodec } from "../src/common/codec"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"

function runtime(options: Record<string, unknown> = {}) {
  return new ClientRuntime({ codec: new JsonCodec(), devtools: false, serviceName: "caller", ...options } as any, { transport: "test" })
}

function okEnvelope(result: unknown) {
  return { uuid: "u", method: "m", params: { result }, meta: {} }
}

test("query returns the unwrapped result", async () => {
  const r = runtime()
  const value = await r.query<{ ok: boolean }>({
    serviceName: "svc",
    method: "m",
    params: {},
    send: async () => okEnvelope({ ok: true })
  })
  assert.deepEqual(value, { ok: true })
})

test("query turns an error envelope into a MessagingError and keeps the message alongside details", async () => {
  const r = runtime({ retry: { enabled: false } })
  await assert.rejects(
    () =>
      r.query({
        serviceName: "svc",
        method: "m",
        params: {},
        send: async () => ({
          uuid: "u",
          method: "m",
          params: { result: "error", error: { code: ErrorCode.VALIDATION_FAILED, message: "bad input", details: { field: "id" } } }
        })
      }),
    (err: unknown) => {
      assert.ok(err instanceof MessagingError)
      assert.equal(err.code, ErrorCode.VALIDATION_FAILED)
      // The message used to be dropped whenever `details` was present.
      assert.equal(err.message, "bad input")
      assert.equal(err.details.field, "id")
      return true
    }
  )
})

test("query caches by idempotency key, scoped to service and method", async () => {
  const r = runtime({ idempotency: { enabled: true, ttlMs: 60_000 } })
  let calls = 0
  const call = (serviceName: string, method: string) =>
    r.query<number>({
      serviceName,
      method,
      params: {},
      opts: { idempotencyKey: "same-key" },
      send: async () => okEnvelope(++calls)
    })

  assert.equal(await call("svc-a", "m"), 1)
  assert.equal(await call("svc-a", "m"), 1, "second identical call is served from cache")
  assert.equal(await call("svc-b", "m"), 2, "another service must not read the first one's entry")
  assert.equal(await call("svc-a", "other"), 3, "another method must not read it either")
})

test("query records exactly one breaker outcome per logical call despite retries", async () => {
  const r = runtime({
    retry: { maxAttempts: 3, baseMs: 1, maxMs: 2, jitter: false },
    circuitBreaker: { enabled: true, failureThreshold: 2, resetTimeoutMs: 60_000 }
  })
  let attempts = 0

  await assert.rejects(() =>
    r.query({
      serviceName: "svc",
      method: "m",
      params: {},
      send: async () => {
        attempts++
        throw new MessagingError(ErrorCode.SERVICE_UNAVAILABLE, { message: "down", retryable: true })
      }
    })
  )

  assert.equal(attempts, 3, "all retry attempts ran")
  const snap = r.circuitBreaker.snapshot()["svc:m"]
  assert.equal(snap.failures, 1, "three physical attempts are one logical failure")
  assert.equal(snap.state, "closed", "a single logical failure must not trip a threshold of 2")
})

test("query stamps the attempt number on every retry", async () => {
  const r = runtime({ retry: { maxAttempts: 3, baseMs: 1, maxMs: 2, jitter: false } })
  const seen: string[] = []
  await assert.rejects(() =>
    r.query({
      serviceName: "svc",
      method: "m",
      params: {},
      send: async (request: EncodedRequest) => {
        seen.push(String(request.meta.headers?.["nevo-attempt"]))
        throw new MessagingError(ErrorCode.SERVICE_UNAVAILABLE, { message: "down", retryable: true })
      }
    })
  )
  assert.deepEqual(seen, ["1", "2", "3"])
})

test("query issues a fresh envelope uuid per attempt but a stable idempotency key", async () => {
  const r = runtime({ retry: { maxAttempts: 3, baseMs: 1, maxMs: 2, jitter: false } })
  const uuids: string[] = []
  const idemKeys: string[] = []
  await assert.rejects(() =>
    r.query({
      serviceName: "svc",
      method: "m",
      params: {},
      send: async (request: EncodedRequest) => {
        uuids.push(request.uuid)
        idemKeys.push(String(request.meta.idempotencyKey))
        throw new MessagingError(ErrorCode.SERVICE_UNAVAILABLE, { message: "down", retryable: true })
      }
    })
  )
  assert.equal(new Set(uuids).size, 3, "replay protection needs a distinct uuid per attempt")
  assert.equal(new Set(idemKeys).size, 1, "server-side dedupe needs one stable key across attempts")
})

test("query does not retry a non-retryable failure", async () => {
  const r = runtime({ retry: { maxAttempts: 3, baseMs: 1, maxMs: 2, jitter: false } })
  let attempts = 0
  await assert.rejects(() =>
    r.query({
      serviceName: "svc",
      method: "m",
      params: {},
      send: async () => {
        attempts++
        throw new MessagingError(ErrorCode.VALIDATION_FAILED, { message: "nope" })
      }
    })
  )
  assert.equal(attempts, 1)
})

test("mapError translates a transport-native failure before retry classification", async () => {
  const r = runtime({ retry: { maxAttempts: 2, baseMs: 1, maxMs: 2, jitter: false } })
  let attempts = 0
  await assert.rejects(
    () =>
      r.query({
        serviceName: "svc",
        method: "m",
        params: {},
        mapError: () => new MessagingError(ErrorCode.TIMEOUT, { message: "mapped", retryable: true }),
        send: async () => {
          attempts++
          throw Object.assign(new Error("native"), { code: "TIMEOUT" })
        }
      }),
    (err: unknown) => {
      assert.ok(err instanceof MessagingError)
      assert.equal(err.code, ErrorCode.TIMEOUT)
      return true
    }
  )
  assert.equal(attempts, 2, "the mapped error is retryable, so the retry loop saw it as such")
})

test("emit runs under the breaker and retry policy", async () => {
  const r = runtime({
    retry: { maxAttempts: 2, baseMs: 1, maxMs: 2, jitter: false },
    circuitBreaker: { enabled: true, failureThreshold: 5, resetTimeoutMs: 60_000 }
  })
  let attempts = 0
  await assert.rejects(() =>
    r.emit({
      serviceName: "svc",
      method: "m",
      params: {},
      send: async () => {
        attempts++
        throw new MessagingError(ErrorCode.SERVICE_UNAVAILABLE, { message: "down", retryable: true })
      }
    })
  )
  assert.equal(attempts, 2)
  assert.equal(r.circuitBreaker.snapshot()["svc:m"].failures, 1)
})

test("emit stamps a stable idempotency key so a retried publish can be deduped", async () => {
  const r = runtime({ retry: { maxAttempts: 3, baseMs: 1, maxMs: 2, jitter: false } })
  const keys: string[] = []
  await assert.rejects(() =>
    r.emit({
      serviceName: "svc",
      method: "m",
      params: {},
      send: async (request: EncodedRequest) => {
        keys.push(String(request.meta.idempotencyKey))
        throw new MessagingError(ErrorCode.SERVICE_UNAVAILABLE, { message: "down", retryable: true })
      }
    })
  )
  assert.equal(new Set(keys).size, 1)
})

test("in-flight work is tracked so close() drains it", async () => {
  const r = runtime()
  let release!: () => void
  const gate = new Promise<void>((res) => {
    release = res
  })
  const inflight = r.query({
    serviceName: "svc",
    method: "m",
    params: {},
    send: async () => {
      await gate
      return okEnvelope("done")
    }
  })

  let closed = false
  const closing = r.close(5_000).then(() => {
    closed = true
  })
  await new Promise((res) => setTimeout(res, 20))
  assert.equal(closed, false, "close must wait for the in-flight request")

  release()
  assert.equal(await inflight, "done")
  await closing
  assert.equal(closed, true)
})

test("a transport that cannot carry a content-encoding marker forces compression off", () => {
  const capable = new ClientRuntime({ codec: new JsonCodec(), devtools: false, compression: { enabled: true } } as any, { transport: "http" })
  assert.equal(capable.compression.enabled, true)

  const incapable = new ClientRuntime({ codec: new JsonCodec(), devtools: false, compression: { enabled: true } } as any, {
    transport: "ws",
    compressionCapable: false
  })
  assert.equal(incapable.compression.enabled, false, "a compressed body the peer cannot detect is worse than an uncompressed one")

  const encoded = incapable.encodeSync("m", { a: "x".repeat(5000) }, "query")
  assert.equal(encoded.encoding, "identity")
})

test("encode rejects a payload over the size limit before it reaches the wire", () => {
  const r = runtime({ security: { maxPayloadBytes: 128 } })
  assert.throws(
    () => r.encodeSync("m", { blob: "x".repeat(1000) }, "query"),
    (err: unknown) => {
      assert.ok(err instanceof MessagingError)
      assert.equal(err.code, ErrorCode.PAYLOAD_TOO_LARGE)
      return true
    }
  )
})

test("encode carries both the envelope object and its encoded bytes", () => {
  const r = runtime()
  const encoded = r.encodeSync("user.get", { id: 1 }, "query", { version: "v2", tenantId: "t1" })
  assert.equal(encoded.method, "user.get@v2")
  assert.equal(encoded.envelope.method, "user.get@v2")
  assert.deepEqual(encoded.envelope.params, { id: 1 })
  assert.equal(encoded.envelope.meta?.tenantId, "t1")
  assert.equal(encoded.envelope.uuid, encoded.uuid)
  assert.ok(encoded.payload.byteLength > 0)
})

test("decode round-trips what encode produced", async () => {
  const r = runtime()
  const encoded = r.encodeSync("m", { hello: "world" }, "query")
  const decoded: any = await r.decode(encoded.payload)
  assert.deepEqual(decoded.params, { hello: "world" })
})
