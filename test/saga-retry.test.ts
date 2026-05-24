import { test } from "node:test"
import assert from "node:assert/strict"
import { createSaga, InMemorySagaStore, Saga } from "../src/common/saga"

test("step retries on failure", async () => {
  let attempts = 0
  const result = await createSaga<{ x: number }>()
    .step({
      name: "a",
      execute: () => { attempts++; if (attempts < 3) throw new Error("flaky") },
      retries: 5,
      backoff: { baseMs: 1, maxMs: 1, jitter: false }
    })
    .run({ x: 1 })
  assert.equal(result.status, "success")
  assert.equal(attempts, 3)
})

test("step timeout triggers failure path", async () => {
  const result = await createSaga<{}>()
    .step({
      name: "slow",
      execute: () => new Promise((r) => setTimeout(r, 200)),
      timeoutMs: 30,
      retries: 0
    })
    .run({})
  assert.equal(result.status, "failed")
})

test("compensation runs LIFO and is retried on failure", async () => {
  const order: string[] = []
  let compensateAttempts = 0
  const result = await createSaga<{}>()
    .step({ name: "a", execute: () => { order.push("a") }, compensate: () => { order.push("-a") } })
    .step({
      name: "b",
      execute: () => { order.push("b") },
      compensate: () => {
        compensateAttempts++
        if (compensateAttempts < 2) throw new Error("comp fail")
        order.push("-b")
      },
      compensateRetries: 3,
      compensateBackoff: { baseMs: 1, maxMs: 1, jitter: false }
    })
    .step({ name: "c", execute: () => { throw new Error("boom") } })
    .run({})
  assert.equal(result.status, "failed")
  assert.deepEqual(order, ["a", "b", "-b", "-a"])
})

test("saga persists snapshots and can resume", async () => {
  const store = new InMemorySagaStore()
  const sagaId = "saga-resume-1"
  const ctx = { counter: 0 }

  await new Saga<typeof ctx>()
    .withStore(store, sagaId)
    .step({ name: "a", execute: (c) => { c.counter++ } })
    .step({ name: "fail", execute: () => { throw new Error("die") }, retries: 0 })
    .run(ctx)

  const snap = await store.load(sagaId)
  // saga ended in compensated state; we mock a resume after fix
  if (snap) {
    snap.status = "pending"
    snap.executed = ["a"]
    await store.save(snap)
  }

  const resumeResult = await Saga.resume(store, sagaId, [
    { name: "a", execute: (c: any) => { c.counter++ } },
    { name: "fixed", execute: () => undefined }
  ])
  assert.equal(resumeResult.status, "success")
  assert.ok(resumeResult.executed.includes("fixed"))
})
