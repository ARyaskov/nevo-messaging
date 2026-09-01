import { test } from "node:test"
import assert from "node:assert/strict"
import { InMemoryScheduledTaskStore, type ScheduledTask } from "../src/common/scheduler"

function task(id: string, runAt: number, over: Partial<ScheduledTask> = {}): ScheduledTask {
  return { id, name: "n", payload: null, runAt, attempts: 0, maxAttempts: 3, status: "pending", createdAt: 0, ...over }
}

test("claimDue returns due tasks in due order", async () => {
  const store = new InMemoryScheduledTaskStore()
  for (const [id, runAt] of [
    ["c", 300],
    ["a", 100],
    ["d", 400],
    ["b", 200]
  ] as const) {
    await store.enqueue(task(id, runAt))
  }

  const claimed = await store.claimDue("w", 350, 10, 10_000)
  assert.deepEqual(
    claimed.map((t) => t.id),
    ["a", "b", "c"],
    "the not-yet-due task must stay behind"
  )
})

test("a backlog of far-future tasks costs nothing to skip", async () => {
  const store = new InMemoryScheduledTaskStore()
  const far = Date.now() + 3_600_000
  for (let i = 0; i < 20_000; i++) await store.enqueue(task(`f${i}`, far + i))
  await store.enqueue(task("due", 1))

  const claimed = await store.claimDue("w", 1_000, 10, 10_000)
  assert.deepEqual(
    claimed.map((t) => t.id),
    ["due"]
  )
  // The heap is only consulted at its head, so the 20k pending tasks are untouched.
  assert.ok(store.dueQueueSize() >= 20_000)
})

test("respects the batch limit and leaves the rest claimable", async () => {
  const store = new InMemoryScheduledTaskStore()
  for (let i = 0; i < 5; i++) await store.enqueue(task(`t${i}`, 10 + i))

  const first = await store.claimDue("w", 1_000, 2, 10_000)
  assert.equal(first.length, 2)
  const second = await store.claimDue("w2", 1_000, 10, 10_000)
  assert.equal(second.length, 3, "the remainder is still claimable on the next pass")
})

test("a rescheduled task is claimed at its new time, not its old one", async () => {
  const store = new InMemoryScheduledTaskStore()
  await store.enqueue(task("cron", 100))

  const claimed = await store.claimDue("w", 1_000, 10, 10_000)
  assert.equal(claimed.length, 1)
  await store.reschedule("cron", 10_000, "w")

  assert.equal((await store.claimDue("w", 5_000, 10, 10_000)).length, 0, "not due yet at its new time")
  assert.equal((await store.claimDue("w", 10_000, 10, 10_000)).length, 1)
})

test("a completed task is dropped from the due queue rather than retried forever", async () => {
  const store = new InMemoryScheduledTaskStore()
  await store.enqueue(task("one", 10))

  const claimed = await store.claimDue("w", 1_000, 10, 10_000)
  assert.equal(claimed.length, 1)
  await store.markCompleted("one", "w")

  assert.equal((await store.claimDue("w", 2_000, 10, 10_000)).length, 0)
  // Draining it once is enough; the entry must not linger in the heap.
  await store.claimDue("w", 3_000, 10, 10_000)
  assert.equal(store.dueQueueSize(), 0)
})

test("a failed-but-retryable task stays claimable; an exhausted one does not", async () => {
  const store = new InMemoryScheduledTaskStore()
  await store.enqueue(task("retry", 10, { maxAttempts: 2 }))

  await store.claimDue("w", 1_000, 10, 10_000)
  await store.markFailed("retry", "boom", "w")
  assert.equal((await store.claimDue("w", 2_000, 10, 10_000)).length, 1, "attempt 2 is still owed")

  await store.markFailed("retry", "boom", "w")
  assert.equal((await store.claimDue("w", 3_000, 10, 10_000)).length, 0, "attempts are exhausted")
})

test("terminal tasks are pruned by retention without scanning live ones", async () => {
  const store = new InMemoryScheduledTaskStore({ terminalRetentionMs: 0, maxTerminalTasks: 0 })
  for (let i = 0; i < 20; i++) await store.enqueue(task(`t${i}`, 10))
  const claimed = await store.claimDue("w", 1_000, 100, 10_000)
  for (const t of claimed) await store.markCompleted(t.id, "w")

  // Any subsequent operation prunes; nothing terminal should be retained.
  await store.list()
  assert.equal(store.size(), 0)
})

test("cancel removes a task from the due queue", async () => {
  const store = new InMemoryScheduledTaskStore()
  await store.enqueue(task("gone", 10))
  await store.cancel("gone")
  assert.equal((await store.claimDue("w", 1_000, 10, 10_000)).length, 0)
})
