import { test } from "node:test"
import assert from "node:assert/strict"
import { DevToolsBus } from "../src/common/devtools"

// Helper to peek at the private listener-emission queue without yielding to the
// event loop (which would let the scheduled setImmediate flush run).
function pendingLength(bus: DevToolsBus): number {
  return (bus as unknown as { pendingEmissions: unknown[] }).pendingEmissions.length
}

test("drop-oldest caps pendingEmissions at maxPending under flush suppression", () => {
  const cap = 8
  const bus = new DevToolsBus({ maxEvents: 100000, batchFlushMs: 5, maxPending: cap, dropStrategy: "drop-oldest" })
  bus.on(() => {})
  // Synchronous loop never yields, so the scheduled flush cannot run yet.
  for (let i = 0; i < cap * 50; i++) bus.publish({ ts: i, type: "request", method: `m${i}` })
  assert.ok(pendingLength(bus) <= cap, `pending ${pendingLength(bus)} should be <= ${cap}`)
  // drop-oldest keeps the newest events.
  const received: number[] = []
  bus.on((e) => received.push(e.ts))
  bus.drain()
  assert.equal(received.length, cap)
  assert.deepEqual(received, [
    cap * 50 - cap,
    cap * 50 - cap + 1,
    cap * 50 - cap + 2,
    cap * 50 - cap + 3,
    cap * 50 - cap + 4,
    cap * 50 - cap + 5,
    cap * 50 - cap + 6,
    cap * 50 - cap + 7
  ])
})

test("maxPending defaults to capacity", () => {
  const cap = 6
  const bus = new DevToolsBus({ maxEvents: cap, batchFlushMs: 5, dropStrategy: "drop-oldest" })
  bus.on(() => {})
  for (let i = 0; i < cap * 20; i++) bus.publish({ ts: i, type: "request" })
  assert.ok(pendingLength(bus) <= cap, `pending ${pendingLength(bus)} should be <= ${cap}`)
})

test("drop-newest skips beyond cap", () => {
  const cap = 4
  const bus = new DevToolsBus({ maxEvents: 100000, batchFlushMs: 5, maxPending: cap, dropStrategy: "drop-newest" })
  bus.on(() => {})
  for (let i = 0; i < cap * 10; i++) bus.publish({ ts: i, type: "request" })
  assert.ok(pendingLength(bus) <= cap, `pending ${pendingLength(bus)} should be <= ${cap}`)
  // drop-newest keeps the oldest events.
  const received: number[] = []
  bus.on((e) => received.push(e.ts))
  bus.drain()
  assert.deepEqual(received, [0, 1, 2, 3])
})

test("back-pressure signals and drops beyond cap", () => {
  const cap = 4
  const depths: number[] = []
  const bus = new DevToolsBus({
    maxEvents: 100000,
    batchFlushMs: 5,
    maxPending: cap,
    dropStrategy: "back-pressure",
    onBackpressure: (d) => depths.push(d)
  })
  bus.on(() => {})
  for (let i = 0; i < cap * 10; i++) bus.publish({ ts: i, type: "request" })
  assert.ok(pendingLength(bus) <= cap, `pending ${pendingLength(bus)} should be <= ${cap}`)
  assert.ok(depths.length > 0, "onBackpressure should have fired")
})

test("events still flow when flush runs after batching", async () => {
  const bus = new DevToolsBus({ maxEvents: 1000, batchFlushMs: 5, maxPending: 1000, dropStrategy: "drop-oldest" })
  const received: number[] = []
  bus.on((e) => received.push(e.ts))
  bus.publish({ ts: 1, type: "request" })
  bus.publish({ ts: 2, type: "request" })
  assert.equal(received.length, 0, "should not emit synchronously when batched")
  await new Promise((r) => setImmediate(r))
  assert.deepEqual(received, [1, 2])
})
