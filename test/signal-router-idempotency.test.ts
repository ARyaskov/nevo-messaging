import { test } from "node:test"
import assert from "node:assert/strict"
import { createSignalRouterDecorator, type SignalRouterOptions } from "../src/signal-router.utils"
import { addSignalMetadata } from "../src/signal.decorator"
import { RedisIdempotencyStore, type IdempotencyRedisLike } from "../src/common/idempotency-store"
import { AuditLog, InMemoryAuditSink } from "../src/common/audit-log"
import { TenantPolicyRegistry, setTenantPolicyRegistry, getTenantPolicyRegistry } from "../src/common/tenant-policy"
import { ErrorCode } from "../src/common/error-code"
import type { MessageResponse } from "../src/common/types"

// ---------------------------------------------------------------------------
// A single-process, atomic fake Redis — `SET ... NX` is honoured, which is all
// the claim machinery needs. Shared across instances to simulate replicas.
// ---------------------------------------------------------------------------
function fakeRedis(): IdempotencyRedisLike & { storage: Map<string, string> } {
  const storage = new Map<string, string>()
  return {
    storage,
    async get(key) {
      return storage.get(key) ?? null
    },
    async set(key, value, opts) {
      if (opts.ifNotExists && storage.has(key)) return null
      storage.set(key, value)
      return "OK"
    },
    async del(key) {
      return storage.delete(key) ? 1 : 0
    }
  } as IdempotencyRedisLike & { storage: Map<string, string> }
}

/** Build a `handleSignalMessage`-style entrypoint wired to a service instance. */
function buildRouter(serviceType: any, serviceInstance: any, signalName: string, methodName: string, options?: Partial<SignalRouterOptions>) {
  class Ctrl {
    svc: any = serviceInstance
  }
  addSignalMetadata(Ctrl, signalName, methodName)
  const decorate = createSignalRouterDecorator(
    serviceType,
    { serviceName: "router", tracing: { enabled: false }, devtools: false, ...options } as SignalRouterOptions,
    (data: any) => ({ method: data.method, params: data.params, uuid: data.uuid, meta: data.meta }),
    () => {}
  )
  decorate(Ctrl)
  const ctrl: any = new Ctrl()
  return (data: any) => ctrl.handleSignalMessage(data) as Promise<MessageResponse>
}

test("signal-router: concurrent same-key requests execute the handler exactly once", async () => {
  let calls = 0
  let release!: () => void
  const gate = new Promise<void>((r) => {
    release = r
  })
  class Svc {
    async run() {
      calls++
      await gate
      return { n: calls }
    }
  }
  const store = new RedisIdempotencyStore<MessageResponse>({ client: fakeRedis(), enabled: true, ttlMs: 60_000 })
  const handle = buildRouter(Svc, new Svc(), "echo.run", "run", { idempotencyStore: store })

  // Same wire-level idempotency key, distinct envelope uuids (as a timeout-retry
  // would produce). Both should collapse to a single handler invocation.
  const msg = (uuid: string) => ({ method: "echo.run", params: {}, uuid, meta: { idempotencyKey: "same-key" } })
  const p1 = handle(msg("u1"))
  const p2 = handle(msg("u2"))
  await new Promise((r) => setTimeout(r, 25)) // let both reach their claim/await point
  release()
  const [r1, r2] = await Promise.all([p1, p2])

  assert.equal(calls, 1, "handler must run exactly once for the same idempotency key")
  assert.deepEqual(r1.params.result, r2.params.result, "both callers see the same result")
  assert.deepEqual(r1.params.result, { n: 1 })
})

test("signal-router: concurrent same-key requests dedup with only the in-process L1 (no distributed store)", async () => {
  let calls = 0
  let release!: () => void
  const gate = new Promise<void>((r) => {
    release = r
  })
  class Svc {
    async run() {
      calls++
      await gate
      return { n: calls }
    }
  }
  // No idempotencyStore — exactly-once relies on in-process leader election.
  const handle = buildRouter(Svc, new Svc(), "echo.run", "run")
  const msg = (uuid: string) => ({ method: "echo.run", params: {}, uuid, meta: { idempotencyKey: "k2" } })
  const p1 = handle(msg("u1"))
  const p2 = handle(msg("u2"))
  await new Promise((r) => setTimeout(r, 25))
  release()
  const [r1, r2] = await Promise.all([p1, p2])

  assert.equal(calls, 1, "in-process leader election must dedup same-key concurrency")
  assert.deepEqual(r1.params.result, r2.params.result)
})

test("signal-router: tenant kill-switch denies on the live path (before dispatch)", async () => {
  setTenantPolicyRegistry(new TenantPolicyRegistry())
  getTenantPolicyRegistry().setEnabled("router", "evicted", false, "non-payment")
  try {
    let calls = 0
    class Svc {
      async run() {
        calls++
        return "ok"
      }
    }
    const handle = buildRouter(Svc, new Svc(), "echo.run", "run")

    const r = await handle({ method: "echo.run", params: {}, uuid: "t1", meta: { tenantId: "evicted" } })
    assert.equal(r.params.result, "error")
    assert.equal(r.params.error?.code, ErrorCode.UNAUTHORIZED)
    assert.match(r.params.error?.message ?? "", /evicted/)
    assert.equal(calls, 0, "handler must not run for a disabled tenant")

    // A non-disabled tenant on the same service is unaffected.
    const ok = await handle({ method: "echo.run", params: {}, uuid: "t2", meta: { tenantId: "good" } })
    assert.equal(ok.params.result, "ok")
    assert.equal(calls, 1)
  } finally {
    getTenantPolicyRegistry().clear()
  }
})

test("signal-router: wire-level idempotency key dedups across two simulated replicas", async () => {
  let calls = 0
  class Svc {
    async run(p: any) {
      calls++
      return { handledBy: p.replica, n: calls }
    }
  }
  // Two routers with independent L1 caches + in-flight maps, sharing ONE Redis.
  const sharedRedis = fakeRedis()
  const storeA = new RedisIdempotencyStore<MessageResponse>({ client: sharedRedis, enabled: true, ttlMs: 60_000 })
  const storeB = new RedisIdempotencyStore<MessageResponse>({ client: sharedRedis, enabled: true, ttlMs: 60_000 })
  const replicaA = buildRouter(Svc, new Svc(), "echo.run", "run", { idempotencyStore: storeA })
  const replicaB = buildRouter(Svc, new Svc(), "echo.run", "run", { idempotencyStore: storeB })

  // Same idempotency key, different uuids — exactly what a client retry after a
  // timeout sends. Replica B must serve replica A's stored result, not re-run.
  const r1 = await replicaA({ method: "echo.run", params: { replica: "A" }, uuid: "uuid-1", meta: { idempotencyKey: "order-42" } })
  const r2 = await replicaB({ method: "echo.run", params: { replica: "B" }, uuid: "uuid-2", meta: { idempotencyKey: "order-42" } })

  assert.equal(calls, 1, "handler ran on exactly one replica")
  assert.deepEqual(r1.params.result, r2.params.result)
  assert.equal((r2.params.result as any).handledBy, "A", "replica B returned replica A's cached result")
})

test("signal-router: distinct idempotency keys do NOT dedup", async () => {
  let calls = 0
  class Svc {
    async run() {
      calls++
      return calls
    }
  }
  const store = new RedisIdempotencyStore<MessageResponse>({ client: fakeRedis(), enabled: true, ttlMs: 60_000 })
  const handle = buildRouter(Svc, new Svc(), "echo.run", "run", { idempotencyStore: store })
  await handle({ method: "echo.run", params: {}, uuid: "a", meta: { idempotencyKey: "k-a" } })
  await handle({ method: "echo.run", params: {}, uuid: "b", meta: { idempotencyKey: "k-b" } })
  assert.equal(calls, 2)
})

test("signal-router: audit log records one redacted entry per request", async () => {
  const sink = new InMemoryAuditSink()
  const auditLog = new AuditLog({ enabled: true, sink })
  class Svc {
    async run() {
      return "ok"
    }
  }
  const handle = buildRouter(Svc, new Svc(), "echo.run", "run", { auditLog })

  await handle({ method: "echo.run", params: { a: 1 }, uuid: "a1", meta: { service: "caller" } })
  // Audit writes are fire-and-forget — let the microtask settle.
  await new Promise((r) => setImmediate(r))

  const entries = sink.list()
  assert.equal(entries.length, 1)
  assert.equal(entries[0].method, "echo.run")
  assert.equal(entries[0].outcome, "ok")
  assert.equal(entries[0].caller, "caller")
})
