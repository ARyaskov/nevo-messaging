import { test } from "node:test"
import assert from "node:assert/strict"
import { WorkflowEngine, Workflow, getWorkflowMethods, discoverAndRegisterWorkflows, type WorkflowContext } from "../src/common/workflow"
import { Scheduler } from "../src/common/scheduler"
import { InMemoryEventStore } from "../src/common/event-store"

test("workflow runs to completion in one tick when no awaits suspend", async () => {
  const engine = new WorkflowEngine()
  engine.register("greet", async (ctx: WorkflowContext<{ name: string }>) => {
    return `hello, ${ctx.input.name}`
  })
  const out = await engine.start("greet", { name: "world" })
  assert.equal(out.status, "completed")
  assert.equal(out.result, "hello, world")
})

test("ctx.step caches results across runs (replay-from-history)", async () => {
  const engine = new WorkflowEngine()
  let stepInvocations = 0
  engine.register("twoStep", async (ctx: WorkflowContext) => {
    const a = await ctx.step("a", async () => {
      stepInvocations++
      return 1
    })
    const b = await ctx.step("b", async () => {
      stepInvocations++
      return a + 1
    })
    return a + b
  })
  const first = await engine.start("twoStep", null)
  assert.equal(first.status, "completed")
  assert.equal(first.result, 3)
  assert.equal(stepInvocations, 2)

  // Manually trigger another run on the same workflowId — should short-circuit.
  await engine.resume(first.workflowId)
  assert.equal(stepInvocations, 2, "steps must not re-execute on replay")
})

test("ctx.sleep suspends the workflow then resumes via scheduler", async () => {
  const store = new InMemoryEventStore()
  const scheduler = new Scheduler({ pollIntervalMs: 10 })
  const engine = new WorkflowEngine({ store, scheduler })

  let after = 0
  engine.register("withSleep", async (ctx: WorkflowContext) => {
    await ctx.step("before", async () => "ready")
    await ctx.sleep(40)
    after++
    return "done"
  })

  const first = await engine.start("withSleep", null)
  assert.equal(first.status, "suspended")
  assert.equal(after, 0)

  // Let the scheduler fire the wake-up.
  await new Promise((r) => setTimeout(r, 60))
  await scheduler.flushOnce()

  const state = await engine.getState(first.workflowId)
  assert.equal(state?.status, "completed")
  assert.equal(after, 1, "post-sleep code should run exactly once after resume")
})

test("ctx.waitForSignal suspends and consumes signal on resume", async () => {
  const engine = new WorkflowEngine()
  engine.register("waiter", async (ctx: WorkflowContext) => {
    const payload = await ctx.waitForSignal<{ value: number }>("approval")
    return `got ${payload.value}`
  })

  const first = await engine.start("waiter", null)
  assert.equal(first.status, "suspended")

  await engine.signal(first.workflowId, "approval", { value: 42 })

  const state = await engine.getState(first.workflowId)
  assert.equal(state?.status, "completed")
  assert.equal(state?.result, "got 42")
})

test("workflow.failed is recorded when the function throws", async () => {
  const engine = new WorkflowEngine()
  engine.register("buggy", async () => {
    throw new Error("kaboom")
  })
  const out = await engine.start("buggy", null)
  assert.equal(out.status, "failed")
  const state = await engine.getState(out.workflowId)
  assert.equal(state?.error, "kaboom")
})

test("cancel marks workflow as cancelled and the engine respects it on subsequent reads", async () => {
  const engine = new WorkflowEngine()
  engine.register("idle", async (ctx: WorkflowContext) => {
    await ctx.waitForSignal("never")
    return "never reaches"
  })
  const out = await engine.start("idle", null)
  assert.equal(out.status, "suspended")
  await engine.cancel(out.workflowId)
  const state = await engine.getState(out.workflowId)
  assert.equal(state?.status, "cancelled")
})

test("step results survive engine instance restart", async () => {
  const store = new InMemoryEventStore()
  const scheduler = new Scheduler({ pollIntervalMs: 10 })

  // First engine instance executes one step.
  const engine1 = new WorkflowEngine({ store, scheduler })
  let firstRunInvocations = 0
  engine1.register("twoStage", async (ctx: WorkflowContext) => {
    const a = await ctx.step("a", async () => {
      firstRunInvocations++
      return 7
    })
    await ctx.sleep(40)
    const b = await ctx.step("b", async () => {
      firstRunInvocations++
      return 11
    })
    return a + b
  })
  const first = await engine1.start("twoStage", null)
  assert.equal(first.status, "suspended")
  assert.equal(firstRunInvocations, 1)

  // Simulate process restart: build a new engine, register the same workflow,
  // share the EventStore and Scheduler — the wake-up handler re-registers itself.
  const engine2 = new WorkflowEngine({ store, scheduler })
  let secondRunInvocations = 0
  engine2.register("twoStage", async (ctx: WorkflowContext) => {
    const a = await ctx.step("a", async () => {
      secondRunInvocations++
      return 7
    })
    await ctx.sleep(40)
    const b = await ctx.step("b", async () => {
      secondRunInvocations++
      return 11
    })
    return a + b
  })

  // Wait for sleep, then drain the scheduler.
  await new Promise((r) => setTimeout(r, 60))
  await scheduler.flushOnce()

  const state = await engine2.getState(first.workflowId)
  assert.equal(state?.status, "completed")
  assert.equal(state?.result, 18)
  // Step `a` was already in history — only step `b` should run on the second engine.
  assert.equal(secondRunInvocations, 1, "engine2 must skip step `a` from history")
})

test("@Workflow decorator metadata discoverable and registrable", async () => {
  class WorkflowService {
    async greet(ctx: WorkflowContext<{ name: string }>) {
      return `hi, ${ctx.input.name}`
    }
  }
  Workflow({ name: "svc.greet" })(WorkflowService.prototype, "greet", { value: WorkflowService.prototype.greet })

  const svc = new WorkflowService()
  const meta = getWorkflowMethods(svc)
  assert.equal(meta.length, 1)
  assert.equal(meta[0].name, "svc.greet")

  const engine = new WorkflowEngine()
  const registered = discoverAndRegisterWorkflows(engine, [svc])
  assert.deepEqual(registered, [{ name: "svc.greet" }])

  const out = await engine.start("svc.greet", { name: "test" })
  assert.equal(out.result, "hi, test")
})
