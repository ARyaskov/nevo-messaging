import { test } from "node:test"
import assert from "node:assert/strict"
import { InMemoryOutboxStore, Outbox } from "../src/common/outbox"

test("outbox uses emitBatch when publisher supports it", async () => {
  const store = new InMemoryOutboxStore()
  let batched = 0
  let single = 0
  const publisher = {
    emit: async () => { single++ },
    emitBatch: async () => { batched++ }
  }
  const ob = new Outbox(store, publisher, { batch: 10, maxAttempts: 3 })
  for (let i = 0; i < 5; i++) await ob.enqueue("user", `m${i}`, { i })
  const res = await ob.flushOnce()
  assert.equal(res.published, 5)
  assert.equal(batched, 1)
  assert.equal(single, 0)
})

test("outbox falls back to per-record emit when batch missing", async () => {
  const store = new InMemoryOutboxStore()
  let single = 0
  const publisher = {
    emit: async () => { single++ }
  }
  const ob = new Outbox(store, publisher, { batch: 10, maxAttempts: 3 })
  for (let i = 0; i < 3; i++) await ob.enqueue("user", `m${i}`, { i })
  const res = await ob.flushOnce()
  assert.equal(res.published, 3)
  assert.equal(single, 3)
})

test("outbox batch failure marks all records failed", async () => {
  const store = new InMemoryOutboxStore()
  const publisher = {
    emit: async () => {},
    emitBatch: async () => { throw new Error("network") }
  }
  const ob = new Outbox(store, publisher, { batch: 10, maxAttempts: 1 })
  for (let i = 0; i < 4; i++) await ob.enqueue("user", `m${i}`, { i })
  const res = await ob.flushOnce()
  assert.equal(res.published, 0)
  assert.equal(res.failed, 4)
})

test("emitBatch partial success only re-sends the unaccepted items", async () => {
  const store = new InMemoryOutboxStore()
  const seen: string[] = []
  let call = 0
  const publisher = {
    emit: async (_svc: string, m: string) => { seen.push(`emit:${m}`) },
    emitBatch: async (items: Array<{ method: string }>) => {
      call++
      seen.push(`batch:${items.map((i) => i.method).join(",")}`)
      // First batch: broker accepts m0 and m2 but rejects m1.
      if (call === 1) return items.map((it, i) => (i === 1 ? { ok: false, error: "rejected" } : { ok: true }))
      return items.map(() => ({ ok: true }))
    }
  }
  const ob = new Outbox(store, publisher, { batch: 10, maxAttempts: 5 })
  await ob.enqueue("svc", "m0", {})
  await ob.enqueue("svc", "m1", {})
  await ob.enqueue("svc", "m2", {})

  const r1 = await ob.flushOnce()
  assert.equal(r1.published, 2)   // m0 + m2 accepted
  assert.equal(r1.failed, 0)      // m1 only deferred (under maxAttempts), not parked

  // Only the rejected item remains pending — the accepted ones are gone.
  const pending = await store.listPending(10)
  assert.deepEqual(pending.map((p) => p.method), ["m1"])

  // Second flush must re-send m1 and must NOT re-send the already-accepted m0/m2.
  seen.length = 0
  const r2 = await ob.flushOnce()
  assert.equal(r2.published, 1)
  assert.ok(seen.some((s) => s.includes("m1")), "m1 should be retried")
  assert.ok(!seen.some((s) => s.includes("m0")), "m0 must not be re-sent")
  assert.ok(!seen.some((s) => s.includes("m2")), "m2 must not be re-sent")
})

test("ordered partition halts at first failure and preserves order on retry", async () => {
  const store = new InMemoryOutboxStore()
  const sent: string[] = []
  let failM1 = true
  const publisher = {
    emit: async (_svc: string, m: string) => {
      if (m === "m1" && failM1) throw new Error("broker rejected m1")
      sent.push(m)
    }
  }
  const ob = new Outbox(store, publisher, { batch: 10, maxAttempts: 5 })
  // All three belong to the same aggregate and must stay ordered.
  await ob.enqueue("svc", "m0", {}, { partitionKey: "order-1" })
  await ob.enqueue("svc", "m1", {}, { partitionKey: "order-1" })
  await ob.enqueue("svc", "m2", {}, { partitionKey: "order-1" })

  const r1 = await ob.flushOnce()
  // m0 publishes, m1 fails, and m2 is NOT relayed ahead of the stuck m1.
  assert.deepEqual(sent, ["m0"])
  assert.equal(r1.published, 1)

  // Broker recovers; the retry resumes the partition in order: m1 then m2.
  failM1 = false
  const r2 = await ob.flushOnce()
  assert.deepEqual(sent, ["m0", "m1", "m2"])
  assert.equal(r2.published, 2)
})

test("independent records are not blocked by a sibling failure", async () => {
  const store = new InMemoryOutboxStore()
  const sent: string[] = []
  const publisher = {
    emit: async (_svc: string, m: string) => {
      if (m === "bad") throw new Error("nope")
      sent.push(m)
    }
  }
  // No partitionKey -> independent. One failure must not stop the others.
  const ob = new Outbox(store, publisher, { batch: 10, maxAttempts: 5 })
  await ob.enqueue("svc", "a", {})
  await ob.enqueue("svc", "bad", {})
  await ob.enqueue("svc", "c", {})
  const r = await ob.flushOnce()
  assert.equal(r.published, 2)
  assert.deepEqual(sent.sort(), ["a", "c"])
})
