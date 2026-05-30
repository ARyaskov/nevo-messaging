import { test } from "node:test"
import assert from "node:assert/strict"
import { createSaga, InMemorySagaStore, Saga, SagaRecovery, SagaStepRegistry } from "../src/common/saga"

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

test("recovery worker resumes a pending saga from the store after a simulated crash", async () => {
  const store = new InMemorySagaStore()
  const sagaId = "saga-recover-1"
  const sagaType = "order-checkout"

  // Simulate a process that executed step "a", persisted, then crashed before "b".
  await store.save({
    sagaId,
    type: sagaType,
    steps: ["a", "b"],
    executed: ["a"],
    ctx: { done: ["a"] },
    status: "pending",
    updatedAt: Date.now()
  })

  // A fresh process re-registers step definitions by saga type + step name so a
  // recovered saga can find them again.
  const ran: string[] = []
  const registry = new SagaStepRegistry<{ done: string[] }>()
    .register(sagaType, { name: "a", execute: (c) => { ran.push("a"); c.done.push("a") } })
    .register(sagaType, { name: "b", execute: (c) => { ran.push("b"); c.done.push("b") } })

  const recovery = new SagaRecovery(store, registry, { intervalMs: 5_000 })
  const result = await recovery.recoverOnce()

  assert.equal(result.recovered, 1)
  assert.equal(result.failed, 0)
  // "a" already ran before the crash, so only "b" executes on resume.
  assert.deepEqual(ran, ["b"])
  // A completed saga is deleted from the store.
  assert.equal(await store.load(sagaId), null)

  // The worker is stoppable: after stop() it ignores new pending sagas.
  recovery.stop()
  await store.save({
    sagaId: "saga-recover-2", type: sagaType, steps: ["a"], executed: [],
    ctx: { done: [] }, status: "pending", updatedAt: Date.now()
  })
  const afterStop = await recovery.recoverOnce()
  assert.equal(afterStop.recovered, 0)
})

test("recovery worker skips a saga whose step type is not registered", async () => {
  const store = new InMemorySagaStore()
  await store.save({
    sagaId: "saga-unknown-type", type: "never-registered", steps: ["a"], executed: [],
    ctx: {}, status: "pending", updatedAt: Date.now()
  })
  const registry = new SagaStepRegistry()
    .register("some-other-type", { name: "a", execute: () => undefined })

  const recovery = new SagaRecovery(store, registry)
  const result = await recovery.recoverOnce()

  assert.equal(result.recovered, 0)
  assert.equal(result.skipped, 1)
  // The saga is left untouched for later (it is not deleted or mutated).
  const snap = await store.load("saga-unknown-type")
  assert.equal(snap!.status, "pending")
})

test("step receives and can observe the abort signal on timeout", async () => {
  let sawSignal = false
  let abortedDuringRun = false

  const result = await createSaga<{}>()
    .step({
      name: "slow",
      execute: (_ctx, signal) =>
        new Promise<void>((resolve) => {
          sawSignal = signal instanceof AbortSignal
          // Honour cancellation: stop as soon as the timeout aborts us instead of
          // running to completion behind a retry (which would double a side effect).
          signal.addEventListener("abort", () => { abortedDuringRun = true; resolve() }, { once: true })
        }),
      timeoutMs: 20,
      retries: 0
    })
    .run({})

  assert.equal(result.status, "failed")
  assert.ok(sawSignal, "execute should receive an AbortSignal")
  assert.ok(abortedDuringRun, "the signal should fire when the step times out")
})
