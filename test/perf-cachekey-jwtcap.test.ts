import "reflect-metadata"
import { test } from "node:test"
import assert from "node:assert/strict"
import { HttpSignalRouter } from "../src/transports/http/http.signal-router.decorator"
import { bindSignalRouterForTesting } from "../src/signal-router.utils"
import { addSignalMetadata } from "../src/signal.decorator"
import { Cacheable } from "../src/common/method-decorators"
import { createJwksVerifier } from "../src/common/jwt-verifier"

const SILENT_LOGGER: any = {
  trace() {},
  debug() {},
  info() {},
  warn() {},
  error() {},
  fatal() {},
  child() {
    return SILENT_LOGGER
  },
  isLevelEnabled() {
    return false
  }
}

// ---------------------------------------------------------------------------
// Fix A — bounded default @Cacheable key.
//
// We drive the real HttpSignalRouter so the assertions exercise the actual
// `buildDefaultCacheKey` path rather than a private copy. The cacheable service
// method counts its invocations: a cache HIT short-circuits in the router before
// the method runs, so the counter is our observable proxy for "same key vs
// different key". @Cacheable lives on the *service* method because the router
// reads cache metadata from the service instance.
// ---------------------------------------------------------------------------

// A param object whose JSON serialization comfortably exceeds the 1024-char
// threshold, forcing the FNV-1a digest branch of the cache key.
function bigParams(tag: string): { tag: string; blob: string } {
  return { tag, blob: "x".repeat(4096) }
}

// A fresh decorated controller per test so signal/cache state does not leak
// between cases. The service is exposed as an instance property so
// findServiceInstances discovers it by type. Returns a `call(params)` driver and
// the shared invocation counter.
function makeRouter() {
  const counter = { calls: 0 }

  class EchoService {
    @Cacheable({ ttlMs: 60_000 })
    echo(params: any) {
      counter.calls++
      return { ok: true, tag: params?.tag }
    }
  }

  class Controller {
    svc = new EchoService()

    handleSignalMessage(_data: any): any {}
  }
  // Register the signal directly (not via @Signal) so the wiring is independent
  // of the host toolchain's decorator mode — the test runner emits TC39 standard
  // decorators, under which a legacy method decorator cannot reach the class.
  addSignalMetadata(Controller, "doThing", "echo")
  HttpSignalRouter(EchoService, { serviceName: "svc", debug: false })(Controller)
  bindSignalRouterForTesting(Controller, { logger: SILENT_LOGGER, devtools: false, tracing: { enabled: false } })
  const controller = new Controller() as any

  let n = 0
  const call = (params: any) => controller.handleSignalMessage({ method: "doThing", params, uuid: `u-${++n}`, meta: {} })
  return { call, counter }
}

test("large params yield a stable cache key — repeat is a HIT", async () => {
  const { call, counter } = makeRouter()

  const r1 = await call(bigParams("same"))
  assert.deepEqual(r1.params.result, { ok: true, tag: "same" })
  assert.equal(counter.calls, 1)

  // Identical (but distinct-instance) large params must collapse to the same
  // bounded key and serve from cache without re-running the handler.
  const r2 = await call(bigParams("same"))
  assert.deepEqual(r2.params.result, { ok: true, tag: "same" })
  assert.equal(counter.calls, 1, "second identical large call must be a cache hit")
})

test("different large params are a cache MISS (distinct bounded keys)", async () => {
  const { call, counter } = makeRouter()

  await call(bigParams("alpha"))
  assert.equal(counter.calls, 1)

  // A different large payload must hash to a different key and re-run.
  await call(bigParams("beta"))
  assert.equal(counter.calls, 2, "distinct large params must miss and re-execute")

  // And the alpha key is still cached — repeating it is again a hit.
  await call(bigParams("alpha"))
  assert.equal(counter.calls, 2, "original large key must still be cached")
})

test("small params keep the verbatim-stringify key and still cache", async () => {
  const { call, counter } = makeRouter()

  await call({ tag: "tiny", n: 1 })
  assert.equal(counter.calls, 1)
  await call({ tag: "tiny", n: 1 })
  assert.equal(counter.calls, 1, "small identical params must be a cache hit")
  await call({ tag: "tiny", n: 2 })
  assert.equal(counter.calls, 2, "small distinct params must miss")
})

// Regression guard: the digest branch must NOT leak one large request's cached
// result to a DIFFERENT large request. With a narrow 32-bit digest and no
// disambiguator, distinct large payloads could share a key and the second call
// would wrongly return the first call's result (a cross-request data leak). We
// drive many distinct large payloads and assert every one re-executes AND
// returns its own tag — a stale hit would both under-count and mis-tag.
test("distinct large params never collide into a wrong cached result", async () => {
  const { call, counter } = makeRouter()

  const N = 300
  for (let i = 0; i < N; i++) {
    const tag = `req-${i}`
    const r = await call(bigParams(tag))
    // Each distinct large payload must run the handler (a collision would skip it)
    assert.equal(counter.calls, i + 1, `call #${i} must be a cache miss, not a collision`)
    // And must receive ITS OWN result — a wrong hit would carry an earlier tag.
    assert.equal(r.params.result.tag, tag, `call #${i} returned a leaked result`)
  }

  // Replaying any earlier large payload must still be a hit with the right tag
  // (the bounded key is stable and not shadowed by a later collision).
  const replay = await call(bigParams("req-0"))
  assert.equal(replay.params.result.tag, "req-0")
  assert.equal(counter.calls, N, "replaying an earlier large key must hit, not re-run")
})

// Two large payloads that serialize to the SAME length but differ in content
// must still get distinct keys. This stresses the digest itself (the length
// disambiguator alone cannot separate them), proving the 64-bit widening does
// the work for equal-length distinct params.
test("equal-length distinct large params get distinct keys", async () => {
  const { call, counter } = makeRouter()

  // Both blobs are exactly the same length; only the content differs.
  const a = { tag: "len-a", blob: "a".repeat(4096) }
  const b = { tag: "len-b", blob: "b".repeat(4096) }
  assert.equal(JSON.stringify(a).length, JSON.stringify(b).length)

  const ra = await call(a)
  assert.equal(counter.calls, 1)
  assert.equal(ra.params.result.tag, "len-a")

  const rb = await call(b)
  assert.equal(counter.calls, 2, "equal-length distinct content must miss")
  assert.equal(rb.params.result.tag, "len-b", "must not return the other payload's result")
})

// ---------------------------------------------------------------------------
// Fix B — explicit JWT token-size cap.
//
// An over-cap token must be rejected before any base64-decode / JSON.parse. We
// prove "no parsing" two ways: the verifier returns null, and it never touches
// the JWKS fetch (which it would if it had progressed past header decode).
// ---------------------------------------------------------------------------

function b64url(s: string): string {
  return Buffer.from(s, "utf8").toString("base64").replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "")
}

function countingFetch() {
  let calls = 0
  const fn = (async () => {
    calls++
    return { ok: true, status: 200, json: async () => ({ keys: [] }) }
  }) as unknown as typeof fetch
  return { fn, calls: () => calls }
}

test("over-cap JWT is rejected without parsing or fetching JWKS", async () => {
  const { fn, calls } = countingFetch()
  const verify = createJwksVerifier({ jwksUri: "https://issuer/jwks.json", fetchImpl: fn })

  // A structurally well-formed but absurdly large token (> 8192 chars). If the
  // cap were missing, header/payload decode and a JWKS fetch would be attempted.
  const header = b64url(JSON.stringify({ alg: "RS256", kid: "k1" }))
  const payload = b64url(JSON.stringify({ sub: "x", big: "A".repeat(10000) }))
  const oversized = `${header}.${payload}.${b64url("sig")}`
  assert.ok(oversized.length > 8192)

  assert.equal(await verify(oversized), null)
  assert.equal(calls(), 0, "over-cap token must short-circuit before JWKS fetch")
})

test("a token at/under the cap is not rejected by the size guard", async () => {
  const { fn, calls } = countingFetch()
  const verify = createJwksVerifier({ jwksUri: "https://issuer/jwks.json", fetchImpl: fn })

  // Small, decodable token: it will ultimately fail verification (empty key set),
  // but the size guard must let it reach the JWKS lookup — proving the cap is a
  // size gate, not a blanket reject.
  const header = b64url(JSON.stringify({ alg: "RS256", kid: "k1" }))
  const payload = b64url(JSON.stringify({ sub: "x", exp: Math.floor(Date.now() / 1000) + 3600 }))
  const small = `${header}.${payload}.${b64url("sig")}`
  assert.ok(small.length <= 8192)

  assert.equal(await verify(small), null)
  assert.ok(calls() >= 1, "under-cap token must progress to the JWKS fetch")
})
