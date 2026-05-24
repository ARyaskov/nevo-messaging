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
