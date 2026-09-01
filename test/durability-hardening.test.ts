import { test } from "node:test"
import assert from "node:assert/strict"
import { InMemoryOutboxStore, Outbox, type OutboxPublisher, type OutboxRecord } from "../src/common/outbox"
import { InMemorySagaStore, SagaRecovery, SagaStepRegistry, type SagaSnapshot } from "../src/common/saga"
import { WorkflowEngine, InMemoryWorkflowLock } from "../src/common/workflow"
import { InMemoryEventStore } from "../src/common/event-store"

function record(id: string, partitionKey?: string, createdAt = Date.now()): OutboxRecord {
  return { id, serviceName: "svc", method: "m", params: { id }, createdAt, attempts: 0, status: "pending", partitionKey }
}

test("outbox relays distinct partitions in parallel instead of head-of-line blocking", async () => {
  const store = new InMemoryOutboxStore()
  await store.save(record("a1", "A"))
  await store.save(record("b1", "B"))

  let releaseA!: () => void
  const slowA = new Promise<void>((res) => {
    releaseA = res
  })
  const started: string[] = []
  const publisher: OutboxPublisher = {
    emit: async (_svc, _method, params: any) => {
      started.push(params.id)
      if (params.id === "a1") await slowA
    }
  }

  const outbox = new Outbox(store, publisher, { maxAttempts: 3 })
  const flush = outbox.flushOnce()
  await new Promise((res) => setTimeout(res, 30))

  assert.deepEqual(started.sort(), ["a1", "b1"], "partition B must not wait on the stalled partition A")
  releaseA()
  const result = await flush
  assert.equal(result.published, 2)
})

test("outbox keeps ordering within one partition", async () => {
  const store = new InMemoryOutboxStore()
  const base = Date.now()
  await store.save(record("k1", "K", base))
  await store.save(record("k2", "K", base + 1))
  await store.save(record("k3", "K", base + 2))

  const order: string[] = []
  const publisher: OutboxPublisher = {
    emit: async (_svc, _method, params: any) => {
      order.push(params.id)
      await new Promise((res) => setTimeout(res, 5))
    }
  }
  await new Outbox(store, publisher, { maxAttempts: 3 }).flushOnce()
  assert.deepEqual(order, ["k1", "k2", "k3"])
})

test("outbox halts a partition at its first failure so later records cannot overtake", async () => {
  const store = new InMemoryOutboxStore()
  const base = Date.now()
  await store.save(record("h1", "H", base))
  await store.save(record("h2", "H", base + 1))

  const attempted: string[] = []
  const publisher: OutboxPublisher = {
    emit: async (_svc, _method, params: any) => {
      attempted.push(params.id)
      if (params.id === "h1") throw new Error("boom")
    }
  }
  await new Outbox(store, publisher, { maxAttempts: 3 }).flushOnce()
  assert.deepEqual(attempted, ["h1"], "h2 must not be published ahead of the failed h1")
})

test("in-memory outbox prunes published records instead of growing forever", async () => {
  const store = new InMemoryOutboxStore({ publishedRetentionMs: 0, maxPublished: 0 })
  const publisher: OutboxPublisher = { emit: async () => {} }
  const outbox = new Outbox(store, publisher, {})

  for (let i = 0; i < 50; i++) await store.save(record(`r${i}`))
  await outbox.flushOnce()
  assert.equal(store.size(), 50, "records are still there immediately after publishing")

  // The next poll is what prunes them.
  await store.listPending(10)
  assert.equal(store.size(), 0, "published records must not be retained indefinitely")
})

test("saga recovery bounds its batch and resumes in parallel", async () => {
  const store = new InMemorySagaStore()
  const stale = Date.now() - 10 * 60_000
  for (let i = 0; i < 10; i++) {
    const snapshot: SagaSnapshot = {
      sagaId: `s${i}`,
      type: "t",
      steps: ["one"],
      executed: [],
      compensated: [],
      ctx: {},
      status: "pending",
      updatedAt: stale
    }
    await store.save(snapshot)
  }

  let concurrent = 0
  let peak = 0
  const registry = new SagaStepRegistry()
  registry.register("t", {
    name: "one",
    execute: async () => {
      concurrent++
      peak = Math.max(peak, concurrent)
      await new Promise((res) => setTimeout(res, 20))
      concurrent--
    }
  })

  const recovery = new SagaRecovery(store, registry, { staleAfterMs: 1000, batchSize: 4, concurrency: 3 })
  const result = await recovery.recoverOnce()

  assert.equal(result.recovered, 4, "batchSize caps how many sagas one pass pulls")
  assert.ok(peak > 1, `expected parallel resume, peak concurrency was ${peak}`)
  assert.ok(peak <= 3, `concurrency must be capped, peak was ${peak}`)
})

test("saga store returns pending sagas oldest first under a limit", async () => {
  const store = new InMemorySagaStore()
  const now = Date.now()
  for (const [id, updatedAt] of [
    ["new", now],
    ["old", now - 100_000],
    ["mid", now - 50_000]
  ] as const) {
    await store.save({ sagaId: id, type: "t", steps: [], executed: [], compensated: [], ctx: {}, status: "pending", updatedAt })
  }
  const page = await store.listPending(2)
  assert.deepEqual(
    page.map((s) => s.sagaId),
    ["old", "mid"]
  )
})

test("workflow lock keeps a second replica from replaying the same run", async () => {
  const store = new InMemoryEventStore()
  const lock = new InMemoryWorkflowLock()
  let runs = 0

  const makeEngine = () => {
    const engine = new WorkflowEngine({ store, lock, lockLeaseMs: 5_000 })
    engine.register("flow", async (ctx) => {
      runs++
      return ctx.step("only", async () => "done")
    })
    return engine
  }

  const a = makeEngine()
  const b = makeEngine()

  const started = await a.start("flow", {})
  assert.equal(started.status, "completed")
  assert.equal(runs, 1)

  // A resume from the second engine while the first holds nothing must still work…
  const resumed = await b.resume(started.workflowId)
  assert.equal(resumed.status, "completed", "a completed workflow reports its terminal status")
})

test("workflow lock reports suspended rather than double-executing when held elsewhere", async () => {
  const store = new InMemoryEventStore()
  const lock = new InMemoryWorkflowLock()

  const engine = new WorkflowEngine({ store, lock, lockLeaseMs: 5_000 })
  engine.register("flow", async (ctx) => ctx.step("only", async () => "done"))
  const started = await engine.start("flow", {})

  // Simulate a peer holding the lease.
  assert.equal(await lock.acquire(started.workflowId, "other-replica", 5_000), true)

  const blocked = new WorkflowEngine({ store, lock, lockLeaseMs: 5_000 })
  blocked.register("flow", async (ctx) => ctx.step("only", async () => "done"))
  const result = await blocked.resume(started.workflowId)
  assert.equal(result.status, "suspended", "a run that cannot take the lease must not replay history")
})

test("in-memory workflow lock is re-entrant for the same owner and expires for others", async () => {
  const lock = new InMemoryWorkflowLock()
  assert.equal(await lock.acquire("w", "owner-a", 50), true)
  assert.equal(await lock.acquire("w", "owner-a", 50), true, "same owner may re-acquire")
  assert.equal(await lock.acquire("w", "owner-b", 50), false)

  assert.equal(await lock.renew("w", "owner-b", 50), false, "a non-owner cannot renew")
  await new Promise((res) => setTimeout(res, 70))
  assert.equal(await lock.acquire("w", "owner-b", 50), true, "an expired lease is stealable")

  await lock.release("w", "owner-a")
  assert.equal(await lock.acquire("w", "owner-c", 50), false, "release by a non-owner is a no-op")
})
