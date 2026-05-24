import { test } from "node:test"
import assert from "node:assert/strict"
import { InMemoryMetrics, NEVO_METRIC_NAMES } from "../src/common/metrics"

test("counter increments and exposes", () => {
  const m = new InMemoryMetrics()
  m.incCounter(NEVO_METRIC_NAMES.requestsTotal, { service: "user", method: "ping", status: "ok" }, 2)
  m.incCounter(NEVO_METRIC_NAMES.requestsTotal, { service: "user", method: "ping", status: "ok" }, 1)
  const out = m.expose()
  assert.match(out, /nevo_messaging_requests_total/)
  assert.match(out, /method=ping/)
  assert.match(out, /} 3/)
})

test("histogram observe and expose", () => {
  const m = new InMemoryMetrics()
  m.observeHistogram(NEVO_METRIC_NAMES.requestDuration, { service: "user", method: "x", status: "ok" }, 0.02)
  const out = m.expose()
  assert.match(out, /nevo_messaging_request_duration_seconds_bucket/)
  assert.match(out, /nevo_messaging_request_duration_seconds_sum/)
})
