import { test } from "node:test"
import assert from "node:assert/strict"
import { HealthRegistry } from "../src/common/health"

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
