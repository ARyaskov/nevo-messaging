import { test } from "node:test"
import assert from "node:assert/strict"
import { InMemoryOutboxStore, Outbox } from "../src/common/outbox"

test("outbox publishes pending records", async () => {
  const store = new InMemoryOutboxStore()
  const calls: string[] = []
  const ob = new Outbox(store, { emit: async (svc, m) => { calls.push(`${svc}:${m}`) } }, { batch: 10, maxAttempts: 3 })
  await ob.enqueue("user", "user.created", { id: 1 })
  await ob.enqueue("user", "user.deleted", { id: 2 })
  const { published, failed } = await ob.flushOnce()
  assert.equal(published, 2)
  assert.equal(failed, 0)
  assert.deepEqual(calls, ["user:user.created", "user:user.deleted"])
})

test("outbox records failed on persistent error", async () => {
  const store = new InMemoryOutboxStore()
  const ob = new Outbox(store, { emit: async () => { throw new Error("nope") } }, { batch: 1, maxAttempts: 1 })
  await ob.enqueue("user", "x", {})
  const { failed } = await ob.flushOnce()
  assert.equal(failed, 1)
})
