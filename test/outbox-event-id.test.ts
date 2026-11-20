import assert from "node:assert/strict"
import { test } from "node:test"
import { InMemoryOutboxStore, Outbox } from "../src/common/outbox"

test("outbox retries preserve the stored idempotency key for every relay path", async () => {
  const attempts: (string | undefined)[] = []
  let fail = true
  const outbox = new Outbox(new InMemoryOutboxStore(), {
    async emit(_service, _method, _params, opts) {
      attempts.push(opts?.idempotencyKey)
      if (fail) throw new Error("acknowledgement lost")
    },
    async emitBatch(items) {
      attempts.push(...items.map((item) => item.opts?.idempotencyKey))
      if (fail) throw new Error("acknowledgement lost")
    }
  })
  const ids = [
    await outbox.enqueue("catalog", "created", {}),
    await outbox.enqueue("catalog", "created", {}),
    await outbox.enqueue("catalog", "updated", {}, { partitionKey: "ordered" })
  ]
  assert.equal((await outbox.flushOnce()).published, 0)
  fail = false
  assert.equal((await outbox.flushOnce()).published, ids.length)
  assert.deepEqual(attempts, [...ids, ...ids])
  const single = await outbox.enqueue("catalog", "created", {})
  await outbox.flushOnce()
  assert.equal(attempts.at(-1), single)
})
