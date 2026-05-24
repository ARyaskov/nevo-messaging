import { test } from "node:test"
import assert from "node:assert/strict"
import { DevToolsBus } from "../src/common/devtools"

test("ring buffer keeps only last N events", () => {
  const bus = new DevToolsBus({ maxEvents: 5 })
  for (let i = 0; i < 12; i++) bus.publish({ ts: i, type: "request", method: `m${i}` })
  const recent = bus.recent(100)
  assert.equal(recent.length, 5)
  assert.deepEqual(recent.map((e) => e.method), ["m7", "m8", "m9", "m10", "m11"])
})

test("recent(N) returns last N in order", () => {
  const bus = new DevToolsBus({ maxEvents: 1000 })
  for (let i = 0; i < 100; i++) bus.publish({ ts: i, type: "response", method: `m${i}` })
  const last5 = bus.recent(5)
  assert.deepEqual(last5.map((e) => e.method), ["m95", "m96", "m97", "m98", "m99"])
})

test("batched flush emits via setImmediate", async () => {
  const bus = new DevToolsBus({ maxEvents: 1000, batchFlushMs: 5 })
  const received: number[] = []
  bus.on((e) => received.push(e.ts))
  bus.publish({ ts: 1, type: "request" })
  bus.publish({ ts: 2, type: "request" })
  assert.equal(received.length, 0, "should not emit synchronously when batched")
  await new Promise((r) => setImmediate(r))
  assert.deepEqual(received, [1, 2])
})

test("no listeners: skip emit but keep in ring", () => {
  const bus = new DevToolsBus({ maxEvents: 10 })
  bus.publish({ ts: 1, type: "request" })
  assert.equal(bus.size(), 1)
  assert.equal(bus.recent(10).length, 1)
})
