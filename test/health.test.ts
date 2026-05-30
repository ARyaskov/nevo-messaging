import { test } from "node:test"
import assert from "node:assert/strict"
import { HealthRegistry } from "../src/common/health"
import { kafkaAdminPing } from "../src/common/health-checks"

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms))

test("aggregates check statuses", async () => {
  const r = new HealthRegistry({ serviceName: "user", version: "1.0.0" })
  r.register("db", () => ({ status: "ok" }))
  r.register("cache", () => ({ status: "down", message: "redis down" }))
  const report = await r.report()
  assert.equal(report.service, "user")
  assert.equal(report.status, "degraded")
  assert.equal(report.checks?.db.status, "ok")
  assert.equal(report.checks?.cache.status, "down")
})

test("thrown check is treated as down", async () => {
  const r = new HealthRegistry({ serviceName: "user" })
  r.register("x", () => { throw new Error("boom") })
  const report = await r.report()
  assert.equal(report.status, "down")
})

test("checks run concurrently, not sequentially", async () => {
  const reg = new HealthRegistry({ serviceName: "svc" })
  let started = 0
  let release!: () => void
  const gate = new Promise<void>((r) => { release = r })
  // Each check parks on the gate; the gate only opens once BOTH have started.
  // Under sequential execution the first check would park forever (the second
  // never starts) and time out — so this passing fast proves concurrency.
  const make = () => async () => {
    started += 1
    if (started === 2) release()
    await gate
    return { status: "ok" as const }
  }
  reg.register("a", make())
  reg.register("b", make())
  const report = await reg.report()
  assert.equal(started, 2)
  assert.equal(report.status, "ok")
})

test("second call within the cache window does not re-invoke the probe fn", async () => {
  const reg = new HealthRegistry({ serviceName: "svc", cacheMs: 5_000 })
  let calls = 0
  reg.register("dep", () => { calls += 1; return { status: "ok" } })
  await reg.report()
  await reg.report()
  assert.equal(calls, 1)
})

test("cached result is refreshed after the cache window expires", async () => {
  const reg = new HealthRegistry({ serviceName: "svc", cacheMs: 20 })
  let calls = 0
  reg.register("dep", () => { calls += 1; return { status: "ok" } })
  await reg.report()
  await delay(80)
  await reg.report()
  assert.equal(calls, 2)
})

test("concurrent probes coalesce into a single check invocation", async () => {
  const reg = new HealthRegistry({ serviceName: "svc", cacheMs: 0 })
  let calls = 0
  reg.register("dep", async () => {
    calls += 1
    await delay(30)
    return { status: "ok" }
  })
  const [a, b] = await Promise.all([reg.readiness(), reg.readiness()])
  assert.equal(calls, 1)
  assert.equal(a.status, "ok")
  assert.equal(b.status, "ok")
})

test("kafkaAdminPing checks the cluster without producing", async () => {
  let described = 0
  // admin only exposes describeCluster — structurally it cannot publish a message.
  const admin = {
    describeCluster: async () => {
      described += 1
      return { brokers: [{ nodeId: 1 }, { nodeId: 2 }] }
    }
  }
  const r = await kafkaAdminPing(admin)()
  assert.equal(r.status, "ok")
  assert.equal(described, 1)
})

test("kafkaAdminPing reports down when no brokers are present", async () => {
  const r = await kafkaAdminPing({ describeCluster: async () => ({ brokers: [] }) })()
  assert.equal(r.status, "down")
})
