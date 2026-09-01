import { test } from "node:test"
import assert from "node:assert/strict"
import { createSignalRouterDecorator, bindSignalRouterForTesting } from "../src/signal-router.utils"
import { addSignalMetadata } from "../src/signal.decorator"
import { Cacheable, RateLimit, rateLimitToOptions } from "../src/common/method-decorators"
import { clientIdempotencyKey, serverIdempotencyKey } from "../src/common/idempotency"
import type { MessageResponse } from "../src/common/types"

function buildRouter(serviceType: any, serviceInstance: any, signals: Array<[string, string]>, options?: Record<string, unknown>) {
  class Ctrl {
    svc: any = serviceInstance
  }
  for (const [signalName, methodName] of signals) addSignalMetadata(Ctrl, signalName, methodName)
  const opts: any = { serviceName: "router", tracing: { enabled: false }, devtools: false, ...options }
  const decorate = createSignalRouterDecorator(
    serviceType,
    opts,
    (data: any) => ({ method: data.method, params: data.params, uuid: data.uuid, meta: data.meta }),
    () => {}
  )
  decorate(Ctrl)
  bindSignalRouterForTesting(Ctrl, opts)
  const ctrl: any = new Ctrl()
  return (data: any) => ctrl.handleSignalMessage(data) as Promise<MessageResponse>
}

test("serverIdempotencyKey separates methods sharing one caller-supplied key", () => {
  const base = { callerService: "web", tenantId: "t1", suppliedKey: "job-1" }
  const a = serverIdempotencyKey({ ...base, method: "wallet.debit" })
  const b = serverIdempotencyKey({ ...base, method: "wallet.credit" })
  assert.notEqual(a, b)
})

test("serverIdempotencyKey is stable for a retry of the same call", () => {
  const a = serverIdempotencyKey({ callerService: "web", tenantId: "t1", method: "m", suppliedKey: "k", envelopeUuid: "u1" })
  const b = serverIdempotencyKey({ callerService: "web", tenantId: "t1", method: "m", suppliedKey: "k", envelopeUuid: "u2" })
  assert.equal(a, b)
})

test("serverIdempotencyKey separates callers and tenants", () => {
  const k = (callerService?: string, tenantId?: string) => serverIdempotencyKey({ callerService, tenantId, method: "m", suppliedKey: "k" })
  assert.notEqual(k("a", "t1"), k("b", "t1"))
  assert.notEqual(k("a", "t1"), k("a", "t2"))
  assert.notEqual(k(undefined, "t1"), k("anon", "t1"), "an absent caller must not collide with a caller literally named anon")
})

test("serverIdempotencyKey falls back to the envelope uuid and returns undefined with neither", () => {
  assert.ok(serverIdempotencyKey({ method: "m", envelopeUuid: "u1" })?.includes("u1"))
  assert.equal(serverIdempotencyKey({ method: "m" }), undefined)
})

test("clientIdempotencyKey separates service and method", () => {
  assert.notEqual(clientIdempotencyKey("svc-a", "m", "k"), clientIdempotencyKey("svc-b", "m", "k"))
  assert.notEqual(clientIdempotencyKey("svc", "m1", "k"), clientIdempotencyKey("svc", "m2", "k"))
  assert.equal(clientIdempotencyKey("svc", "m", "k"), clientIdempotencyKey("svc", "m", "k"))
  // Length prefixes stop a boundary shift from aliasing two different calls.
  assert.notEqual(clientIdempotencyKey("a", "bc", "k"), clientIdempotencyKey("ab", "c", "k"))
})

test("@Cacheable does not serve one tenant's result to another", async () => {
  let calls = 0
  class Svc {
    @Cacheable({ ttlMs: 60_000 })
    async read(params: any) {
      calls++
      return { tenant: params.tenantEcho, n: calls }
    }
  }
  const handle = buildRouter(Svc, new Svc(), [["data.read", "read"]])
  const msg = (uuid: string, tenantId: string) => ({
    method: "data.read",
    params: { id: 1, tenantEcho: tenantId },
    uuid,
    meta: { tenantId }
  })

  const r1 = await handle(msg("u1", "tenant-a"))
  const r2 = await handle(msg("u2", "tenant-b"))

  assert.equal((r1.params.result as any).tenant, "tenant-a")
  assert.equal((r2.params.result as any).tenant, "tenant-b")
  assert.equal(calls, 2, "identical params under a different tenant must miss the cache")
})

test("@Cacheable still caches within one tenant", async () => {
  let calls = 0
  class Svc {
    @Cacheable({ ttlMs: 60_000 })
    async read() {
      calls++
      return { n: calls }
    }
  }
  const handle = buildRouter(Svc, new Svc(), [["data.read", "read"]])
  const msg = (uuid: string) => ({ method: "data.read", params: { id: 1 }, uuid, meta: { tenantId: "t1" } })

  await handle(msg("u1"))
  const second = await handle(msg("u2"))

  assert.equal(calls, 1)
  assert.deepEqual(second.params.result, { n: 1 })
})

test("@Cacheable scope: [] opts into a shared cache across tenants", async () => {
  let calls = 0
  class Svc {
    @Cacheable({ ttlMs: 60_000, scope: [] })
    async read() {
      calls++
      return { n: calls }
    }
  }
  const handle = buildRouter(Svc, new Svc(), [["data.read", "read"]])
  await handle({ method: "data.read", params: { id: 1 }, uuid: "u1", meta: { tenantId: "t1" } })
  await handle({ method: "data.read", params: { id: 1 }, uuid: "u2", meta: { tenantId: "t2" } })
  assert.equal(calls, 1)
})

test("@Cacheable does not alias two versions of a method", async () => {
  let calls = 0
  class Svc {
    @Cacheable({ ttlMs: 60_000 })
    async read() {
      calls++
      return { n: calls, v: "v1" }
    }
    @Cacheable({ ttlMs: 60_000 })
    async readV2() {
      calls++
      return { n: calls, v: "v2" }
    }
  }
  const handle = buildRouter(Svc, new Svc(), [
    ["data.read", "read"],
    ["data.read", "readV2"]
  ])
  const r1 = await handle({ method: "data.read", params: {}, uuid: "u1", meta: {} })
  assert.equal((r1.params.result as any).v, "v1")
  assert.equal(calls, 1)
})

test("@Cacheable keyBy receives the request context", async () => {
  const seen: Array<Record<string, unknown>> = []
  class Svc {
    @Cacheable({
      ttlMs: 60_000,
      keyBy: (params, ctx) => {
        seen.push({ method: ctx.method, tenantId: ctx.tenantId, callerService: ctx.callerService, version: ctx.version })
        return `${ctx.tenantId}:${JSON.stringify(params)}`
      }
    })
    async read() {
      return { ok: true }
    }
  }
  const handle = buildRouter(Svc, new Svc(), [["data.read", "read"]])
  await handle({ method: "data.read@v1", params: { id: 7 }, uuid: "u1", meta: { tenantId: "t9", service: "web" } })

  assert.equal(seen.length, 1)
  assert.equal(seen[0].method, "data.read")
  assert.equal(seen[0].tenantId, "t9")
  assert.equal(seen[0].version, "v1")
  assert.equal(seen[0].callerService, "web")
})

test("@RateLimit default bucket key includes the service dimension", () => {
  const opts = rateLimitToOptions({ capacity: 5, refillPerSec: 1 })
  const key = (topic: string) => opts.keyExtractor!({ topic, method: "m", callerService: "c" })
  assert.notEqual(key("svc-a"), key("svc-b"), "two services sharing a method name must not share one bucket")
  assert.equal(key("svc-a"), key("svc-a"))
})

test("@RateLimit honours an explicit keyBy", () => {
  const opts = rateLimitToOptions({ capacity: 5, refillPerSec: 1, keyBy: ["tenantId"] })
  const key = (tenantId?: string) => opts.keyExtractor!({ topic: "svc", method: "m", tenantId })
  assert.equal(key("t1"), "t1")
  assert.equal(key(undefined), "no-tenant")
})

test("@RateLimit buckets are not shared between two services", async () => {
  class Svc {
    @RateLimit({ capacity: 1, refillPerSec: 0 })
    async hit() {
      return { ok: true }
    }
  }
  const a = buildRouter(Svc, new Svc(), [["svc.hit", "hit"]], { serviceName: "alpha", eventPattern: "alpha-events" })
  const b = buildRouter(Svc, new Svc(), [["svc.hit", "hit"]], { serviceName: "beta", eventPattern: "beta-events" })

  const call = (handle: (d: any) => Promise<MessageResponse>, uuid: string) => handle({ method: "svc.hit", params: {}, uuid, meta: {} })

  assert.notEqual((await call(a, "a1")).params.result, "error")
  // Alpha's single token is spent; beta must still have its own.
  assert.notEqual((await call(b, "b1")).params.result, "error")
})
