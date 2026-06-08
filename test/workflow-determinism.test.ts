import { test } from "node:test"
import assert from "node:assert/strict"
import {
  WorkflowEngine,
  WorkflowSignalTimeout,
  type WorkflowContext
} from "../src/common/workflow"
import { Scheduler } from "../src/common/scheduler"
import { InMemoryEventStore } from "../src/common/event-store"

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms))

test("two distinct waitForSignal calls each get their own signal across resumes (no double-consume)", async () => {
  const store = new InMemoryEventStore()
  const engine = new WorkflowEngine({ store })

  const seen: unknown[] = []
  engine.register("twoWaits", async (ctx: WorkflowContext) => {
    const a = await ctx.waitForSignal<{ v: number }>("a")
    seen.push(a)
    const b = await ctx.waitForSignal<{ v: number }>("b")
    seen.push(b)
    return [a.v, b.v]
  })

  const started = await engine.start("twoWaits", null)
  assert.equal(started.status, "suspended")

  // Deliver the first signal — the workflow consumes it for the FIRST wait and
  // then suspends again on the second wait.
  await engine.signal(started.workflowId, "a", { v: 1 })
  let state = await engine.getState(started.workflowId)
  assert.equal(state?.status, "suspended")

  // Deliver the second signal. The replay must re-hand signal "a" to the first
  // wait (not re-consume it for the second) and give "b" to the second wait.
  await engine.signal(started.workflowId, "b", { v: 2 })
  state = await engine.getState(started.workflowId)
  assert.equal(state?.status, "completed")
  assert.deepEqual(state?.result, [1, 2])

  // A spurious extra resume must not re-run anything or change the outcome.
  await engine.resume(started.workflowId)
  const after = await engine.getState(started.workflowId)
  assert.deepEqual(after?.result, [1, 2])

  // Exactly one consumption per distinct wait, mapping to distinct payloads.
  const events = await store.read({ aggregateId: started.workflowId })
  const consumed = events.filter((e) => e.type === "workflow.signal.consumed")
  assert.equal(consumed.length, 2, "each wait consumes exactly once, durably")
  const byOrdinal = consumed.map((e) => e.payload as { name: string; ordinal: number })
  assert.deepEqual(
    byOrdinal.map((p) => `${p.ordinal}:${p.name}`).sort(),
    ["1:a", "2:b"]
  )
})

test("two same-name waitForSignal calls consume two distinct payloads in FIFO order", async () => {
  const store = new InMemoryEventStore()
  const engine = new WorkflowEngine({ store })

  engine.register("twoSameName", async (ctx: WorkflowContext) => {
    const first = await ctx.waitForSignal<number>("x")
    const second = await ctx.waitForSignal<number>("x")
    return [first, second]
  })

  const started = await engine.start("twoSameName", null)
  assert.equal(started.status, "suspended")

  await engine.signal(started.workflowId, "x", 10)
  await engine.signal(started.workflowId, "x", 20)

  const state = await engine.getState(started.workflowId)
  assert.equal(state?.status, "completed")
  assert.deepEqual(state?.result, [10, 20], "FIFO, each payload consumed once")

  // Re-running from history reproduces the same per-wait assignment.
  await engine.resume(started.workflowId)
  const again = await engine.getState(started.workflowId)
  assert.deepEqual(again?.result, [10, 20])
})

test("a timed-out waitForSignal resolves deterministically after resume (caught)", async () => {
  const store = new InMemoryEventStore()
  const scheduler = new Scheduler({ pollIntervalMs: 10 })
  const engine = new WorkflowEngine({ store, scheduler })

  engine.register("waitOrTimeout", async (ctx: WorkflowContext) => {
    try {
      const v = await ctx.waitForSignal<string>("approval", { timeoutMs: 20 })
      return `signal:${v}`
    } catch (err) {
      if (err instanceof WorkflowSignalTimeout) return "timed-out"
      throw err
    }
  })

  const started = await engine.start("waitOrTimeout", null)
  assert.equal(started.status, "suspended")

  // No signal arrives. Let the timeout become due, then drive the scheduler.
  await delay(40)
  await scheduler.flushOnce()

  const state = await engine.getState(started.workflowId)
  assert.equal(state?.status, "completed")
  assert.equal(state?.result, "timed-out")

  // Determinism: a later resume re-derives the timeout from history (it does not
  // re-suspend forever and does not change the result).
  await engine.resume(started.workflowId)
  const again = await engine.getState(started.workflowId)
  assert.equal(again?.status, "completed")
  assert.equal(again?.result, "timed-out")

  // The timeout fired exactly once and is recorded as an event.
  const events = await store.read({ aggregateId: started.workflowId })
  const timeouts = events.filter((e) => e.type === "workflow.signal.timeout")
  assert.equal(timeouts.length, 1)
})

test("an uncaught signal timeout fails the workflow deterministically", async () => {
  const store = new InMemoryEventStore()
  const scheduler = new Scheduler({ pollIntervalMs: 10 })
  const engine = new WorkflowEngine({ store, scheduler })

  engine.register("strictWait", async (ctx: WorkflowContext) => {
    const v = await ctx.waitForSignal<string>("approval", { timeoutMs: 20 })
    return `signal:${v}`
  })

  const started = await engine.start("strictWait", null)
  assert.equal(started.status, "suspended")

  await delay(40)
  await scheduler.flushOnce()

  const state = await engine.getState(started.workflowId)
  assert.equal(state?.status, "failed")
  assert.match(state?.error ?? "", /timed out/)
})

test("a signal that arrives before the timeout wins; the late timeout is a no-op", async () => {
  const store = new InMemoryEventStore()
  const scheduler = new Scheduler({ pollIntervalMs: 10 })
  const engine = new WorkflowEngine({ store, scheduler })

  engine.register("raceWait", async (ctx: WorkflowContext) => {
    try {
      const v = await ctx.waitForSignal<string>("approval", { timeoutMs: 20 })
      return `signal:${v}`
    } catch (err) {
      if (err instanceof WorkflowSignalTimeout) return "timed-out"
      throw err
    }
  })

  const started = await engine.start("raceWait", null)
  assert.equal(started.status, "suspended")

  // Signal first — the workflow completes from the signal.
  await engine.signal(started.workflowId, "approval", "ok")
  let state = await engine.getState(started.workflowId)
  assert.equal(state?.status, "completed")
  assert.equal(state?.result, "signal:ok")

  // Now let the previously-scheduled timeout fire. It must NOT overwrite the
  // already-settled outcome.
  await delay(40)
  await scheduler.flushOnce()

  state = await engine.getState(started.workflowId)
  assert.equal(state?.status, "completed")
  assert.equal(state?.result, "signal:ok")

  const events = await store.read({ aggregateId: started.workflowId })
  assert.equal(
    events.filter((e) => e.type === "workflow.signal.timeout").length,
    0,
    "no timeout event once the signal settled the wait"
  )
})

test("a sleep is driven by a completion event, not the wall clock", async () => {
  const store = new InMemoryEventStore()
  const scheduler = new Scheduler({ pollIntervalMs: 10 })
  const engine = new WorkflowEngine({ store, scheduler })

  let afterSleep = 0
  engine.register("sleeper", async (ctx: WorkflowContext) => {
    await ctx.step("before", async () => "ready")
    await ctx.sleep(5)
    afterSleep++
    return "done"
  })

  const started = await engine.start("sleeper", null)
  assert.equal(started.status, "suspended")
  assert.equal(afterSleep, 0)

  // Let the wall clock move WELL past wakeAt (sleep was only 5ms). A
  // clock-based implementation would now consider the sleep finished. With
  // event-driven completion, a resume that is NOT preceded by a
  // workflow.sleep.completed event must keep the workflow suspended.
  await delay(40)
  let events = await store.read({ aggregateId: started.workflowId })
  assert.equal(
    events.some((e) => e.type === "workflow.sleep.completed"),
    false,
    "precondition: no completion event yet"
  )

  await engine.resume(started.workflowId)
  let state = await engine.getState(started.workflowId)
  assert.equal(state?.status, "suspended", "sleep must NOT finish on elapsed clock alone")
  assert.equal(afterSleep, 0)

  // Driving the scheduler appends workflow.sleep.completed, and only THEN does
  // the post-sleep code run.
  await scheduler.flushOnce()

  events = await store.read({ aggregateId: started.workflowId })
  assert.equal(
    events.some((e) => e.type === "workflow.sleep.completed"),
    true,
    "scheduler wakeup recorded the completion event"
  )
  state = await engine.getState(started.workflowId)
  assert.equal(state?.status, "completed")
  assert.equal(state?.result, "done")
  assert.equal(afterSleep, 1, "post-sleep code runs exactly once, after the completion event")
})

test("ctx.now() is recorded once and stable across replays", async () => {
  const store = new InMemoryEventStore()
  const engine = new WorkflowEngine({ store })

  engine.register("clock", async (ctx: WorkflowContext) => {
    const t = ctx.now()
    await ctx.waitForSignal("go")
    return t
  })

  const started = await engine.start("clock", null)
  assert.equal(started.status, "suspended")

  // Let real time advance so a re-read of the clock would differ.
  await delay(20)
  await engine.signal(started.workflowId, "go", null)

  const state = await engine.getState(started.workflowId)
  assert.equal(state?.status, "completed")

  const events = await store.read({ aggregateId: started.workflowId })
  const recorded = events.filter((e) => e.type === "workflow.now.recorded")
  assert.equal(recorded.length, 1, "now() recorded exactly once despite the replay")
  const recordedValue = (recorded[0].payload as { value: number }).value
  assert.equal(state?.result, recordedValue, "now() replayed the recorded logical time")
})
