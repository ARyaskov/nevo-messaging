import assert from "node:assert/strict"
import { test } from "node:test"
import { InMemoryOutboxStore, Outbox } from "../src/common/outbox"

test("outbox start is idempotent across a stop and restart during a flush", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] })
  const { promise, resolve } = Promise.withResolvers<{ published: number; failed: number }>()
  const outbox = new Outbox(new InMemoryOutboxStore(), { emit: async () => {} }, { intervalMs: 10 })
  const flush = t.mock.method(outbox, "flushOnce", () => promise)
  outbox.start()
  outbox.start()
  t.mock.timers.tick(10)
  assert.equal(flush.mock.callCount(), 1)
  outbox.stop()
  outbox.start()
  t.mock.timers.tick(10)
  assert.equal(flush.mock.callCount(), 1)
  resolve({ published: 0, failed: 0 })
  await promise
  t.mock.timers.tick(10)
  assert.equal(flush.mock.callCount(), 2)
  outbox.stop()
})
