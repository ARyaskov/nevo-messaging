import { test } from "node:test"
import assert from "node:assert/strict"
import { createSaga, InMemorySagaStore } from "../src/common/saga"
import { InMemoryMetrics } from "../src/common/metrics"
import type { DlqEntry } from "../src/common/dlq"

test("saga executes steps in order", async () => {
  const order: string[] = []
  const result = await createSaga<{ x: number }>()
    .step({ name: "a", execute: () => { order.push("a") } })
    .step({ name: "b", execute: () => { order.push("b") } })
    .run({ x: 1 })
  assert.equal(result.status, "success")
  assert.deepEqual(order, ["a", "b"])
})

test("saga compensates on failure", async () => {
  const order: string[] = []
  const result = await createSaga<{ x: number }>()
    .step({ name: "a", execute: () => { order.push("a") }, compensate: () => { order.push("-a") } })
    .step({ name: "b", execute: () => { order.push("b") }, compensate: () => { order.push("-b") } })
    .step({ name: "c", execute: () => { throw new Error("boom") } })
    .run({ x: 1 })
  assert.equal(result.status, "failed")
  assert.deepEqual(order, ["a", "b", "-b", "-a"])
})

test("failed compensation yields compensation_failed (not compensated) and hits the DLQ", async () => {
  const store = new InMemorySagaStore()
  const metrics = new InMemoryMetrics()
  const dlq: DlqEntry[] = []
  const sagaId = "saga-comp-fail-1"

  const result = await createSaga<{ reserved: boolean }>("wallet-checkout")
    .withStore(store, sagaId)
    .withDlq((e) => { dlq.push(e) })
    .withMetrics(metrics)
    .step({
      name: "reserveWallet",
      execute: (c) => { c.reserved = true },
      // Compensation can never succeed → it exhausts its retries and throws.
      compensate: () => { throw new Error("release failed") },
      compensateRetries: 1,
      compensateBackoff: { baseMs: 1, maxMs: 1, jitter: false }
    })
    .step({ name: "charge", execute: () => { throw new Error("boom") } })
    .run({ reserved: false })

  // Overall the saga failed AND compensation did NOT cleanly complete.
  assert.equal(result.status, "failed")
  assert.deepEqual(result.compensationFailed, ["reserveWallet"])
  assert.deepEqual(result.compensated, [])

  // The snapshot must NOT be marked clean — it stays for manual intervention.
  const snap = await store.load(sagaId)
  assert.ok(snap)
  assert.equal(snap!.status, "compensation_failed")

  // The failure was routed to the DLQ so it is alertable / actionable.
  assert.equal(dlq.length, 1)
  assert.equal(dlq[0].reason, "saga_compensation_failed")
  assert.equal(dlq[0].topic, "saga.wallet-checkout")
  assert.equal((dlq[0].rawPayload as any).step, "reserveWallet")

  // And a metric was emitted on the same path.
  assert.match(metrics.expose(), /saga_compensation_failures_total\{[^}]*\} 1/)
})
