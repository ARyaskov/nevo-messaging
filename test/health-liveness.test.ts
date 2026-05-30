import { test } from "node:test"
import assert from "node:assert/strict"
import { HealthRegistry, memoryUsagePing, eventLoopLagPing } from "../src/common"

test("liveness vs readiness separation", async () => {
  const reg = new HealthRegistry({ serviceName: "svc" })
  reg.register("loop", eventLoopLagPing(1000), { kind: "liveness" })
  reg.register("db", () => ({ status: "down", message: "not ready" }), { kind: "readiness" })

  const live = await reg.liveness()
  const ready = await reg.readiness()

  assert.equal(live.status, "ok")
  assert.equal(ready.status, "down")
})

test("check timeout marks as down", async () => {
  const reg = new HealthRegistry({ serviceName: "svc" })
  reg.register("slow", () => new Promise((r) => setTimeout(() => r({ status: "ok" }), 100)), { timeoutMs: 10 })
  const report = await reg.report()
  assert.equal(report.checks?.slow.status, "down")
})

test("a check exceeding the default timeout fails fast instead of hanging", async () => {
  const reg = new HealthRegistry({ serviceName: "svc", timeoutMs: 20 })
  // No per-check timeout: the registry default must still bound this hung probe.
  reg.register("hang", async () => {
    await new Promise(() => {})
    return { status: "ok" }
  })
  const start = Date.now()
  const report = await reg.report()
  const elapsed = Date.now() - start
  assert.equal(report.checks?.hang.status, "down")
  assert.ok(elapsed < 1_000, `expected fast fail, took ${elapsed}ms`)
})

test("memoryUsagePing reports under threshold", async () => {
  const fn = memoryUsagePing(100_000)
  const r = await fn()
  assert.equal(r.status, "ok")
})
