# Durable workflows

`WorkflowEngine` is a Temporal-style durable execution engine built on top of the [event store](./cqrs.md). A workflow is a plain async function that may run to completion in a single tick, or **suspend** part-way through (waiting on a timer or an external signal) and **resume** minutes, hours, or days later — surviving process restarts because the entire execution history lives in the store.

Source: `src/common/workflow.ts`.

## Why durable workflows

A normal async function loses all of its state the moment the process dies. A workflow does not: every step result, every sleep, and every received signal is appended to the event store as it happens. On resume the engine re-runs the function from the top and **short-circuits past anything already recorded**, so side effects only happen once and the function appears to "pick up where it left off".

This is the same replay model Temporal and AWS Step Functions use. It buys you:

- **Crash safety.** A worker can die mid-workflow; another worker replays the history and continues.
- **Long-lived orchestration.** A workflow can `sleep(7 days)` without holding a thread or a connection.
- **Human-in-the-loop.** A workflow can block on `waitForSignal("approved")` until an operator (or another service) sends the signal.

## Quick start

```ts
import { WorkflowEngine, Scheduler, PgEventStore } from "@riaskov/nevo-messaging"

const engine = new WorkflowEngine({
  store: new PgEventStore({ client }),   // durable history (defaults to in-memory)
  scheduler                              // required for ctx.sleep / signal timeouts
})

engine.register("order.fulfil", async (ctx) => {
  const order = ctx.input as { orderId: string }

  // Every effect goes inside ctx.step — recorded once, replayed from history after.
  const charge = await ctx.step("charge", () => payments.charge(order.orderId))

  await ctx.sleep(24 * 60 * 60_000)      // suspend for a day, then resume

  const approved = await ctx.waitForSignal<boolean>("manualReview")
  if (!approved) {
    await ctx.step("refund", () => payments.refund(charge.id))
    return { status: "refunded" }
  }

  await ctx.step("ship", () => shipping.dispatch(order.orderId))
  return { status: "shipped" }
})

const { workflowId } = await engine.start("order.fulfil", { orderId: "o-123" })
```

To unblock the `waitForSignal` later — from an HTTP handler, a message handler, anywhere:

```ts
await engine.signal(workflowId, "manualReview", true)
```

## The `@Workflow` decorator + discovery

Instead of calling `engine.register` by hand, annotate methods on an injectable and let discovery wire them up.

```ts
import { Injectable } from "@nestjs/common"
import { Workflow, type WorkflowContext } from "@riaskov/nevo-messaging"

@Injectable()
export class OrderWorkflows {
  @Workflow()                              // name defaults to "OrderWorkflows#fulfil"
  async fulfil(ctx: WorkflowContext<{ orderId: string }>) {
    const charge = await ctx.step("charge", () => this.payments.charge(ctx.input.orderId))
    await ctx.sleep(60_000)
    return { charged: charge.id }
  }

  @Workflow({ name: "order.cancel" })      // explicit logical name
  async cancel(ctx: WorkflowContext) { /* … */ }
}
```

At bootstrap, hand the instances to `discoverAndRegisterWorkflows`:

```ts
import { discoverAndRegisterWorkflows } from "@riaskov/nevo-messaging"

const registered = discoverAndRegisterWorkflows(engine, [orderWorkflows])
// registered === [{ name: "OrderWorkflows#fulfil" }, { name: "order.cancel" }]
```

`@Workflow(options?)` accepts a single option, `name` (the logical name; defaults to `Class#method`). The method receives the `WorkflowContext` as its only argument, with `this` bound to the instance.

## The workflow context

The function you register receives a `WorkflowContext<C>`, where `C` is the input type:

| Member | Description |
|---|---|
| `ctx.workflowId` | The unique id of this run (UUIDv7). |
| `ctx.input` | The `input` passed to `engine.start`. |
| `ctx.step(name, fn)` | Run `fn` **exactly once**, record its result, and return the recorded result on every replay. The `name` must be stable and unique within the workflow. |
| `ctx.sleep(ms)` | Suspend the workflow for `ms` milliseconds. Requires a `Scheduler`. |
| `ctx.waitForSignal(name, { timeoutMs? })` | Suspend until a signal of `name` arrives; returns its payload. Signals are FIFO-queued per name. With `timeoutMs`, throws `WorkflowSignalTimeout` if no signal arrives in time (requires a `Scheduler`). |
| `ctx.now()` | The engine-provided clock. **Use this instead of `Date.now()`** (see the determinism contract). |

## Suspension & replay model

The engine is an append-only state machine over these event types in the store:

- `workflow.started` — `{ name, input }`
- `workflow.step.completed` — `{ name, result }`
- `workflow.sleep.started` — `{ ordinal, ms, wakeAt }`
- `workflow.sleep.completed` — `{ ordinal }`
- `workflow.signal.received` — `{ name, value }`
- `workflow.signal.consumed` — `{ name, index, ordinal }`
- `workflow.signal.timeout` — `{ ordinal, name }`
- `workflow.now.recorded` — `{ ordinal, value }`
- `workflow.suspended` — `{ reason }`
- `workflow.completed` / `workflow.failed` / `workflow.cancelled`

On every `execute()` the engine:

1. **Reads the full history** for the `workflowId` and folds it into a replay state — a map of completed step results, the set of *completed* sleep ordinals, the FIFO queue of received signals per name, the wait ordinals that already consumed a signal (and which one) or already timed out, and the recorded `ctx.now()` values keyed by ordinal.
2. **Re-runs the workflow function from the top.** Each `ctx.*` call consults the replay state first. Positional `ctx.*` calls are numbered by **ordinal** (a per-kind counter that increments on each call), so the same call lines up across replays:
   - `ctx.step(name, fn)` — if `name` is already in the step map, returns the recorded result *without* calling `fn`. Otherwise it runs `fn`, appends `workflow.step.completed`, and returns.
   - `ctx.sleep(ms)` — sleeps are numbered by ordinal (`sleep#1`, `sleep#2`, …). A sleep is finished **only when its `workflow.sleep.completed` event exists** — derived from history, never from comparing the wall clock to `wakeAt`. If the completion event is present it's a no-op; otherwise it appends `workflow.sleep.started`, enqueues a scheduler wake-up tagged `{ kind: "sleep", ordinal }` at `now + ms`, and **throws to suspend**.
   - `ctx.waitForSignal(name, { timeoutMs? })` — if this wait's ordinal already recorded a consumption, it returns the *same* payload (by recorded queue index); if it already recorded a timeout, it re-throws `WorkflowSignalTimeout`. Otherwise, on the first arrival at this wait, it takes the oldest received payload of `name` that no other wait has claimed, appends `workflow.signal.consumed`, and returns it. With no unclaimed payload available it (optionally, when `timeoutMs` is set and a scheduler is present) enqueues a `{ kind: "signal-timeout", ordinal, name }` wake-up and **throws to suspend**.
   - `ctx.now()` — returns the value recorded for this ordinal if present; otherwise reads `Date.now()` once, records it as `workflow.now.recorded`, and returns it so every later replay observes the same logical time.
3. **On normal return**, appends `workflow.completed` with the result.
4. **On a suspension throw**, appends `workflow.suspended` and returns `{ status: "suspended" }`.
5. **On any other throw**, appends `workflow.failed` with the error message.

The suspension throw is a private sentinel; `isWorkflowSuspended(err)` (and the exported `WORKFLOW_SUSPEND` symbol) distinguish it from genuine errors. **Never catch-all around `ctx.sleep`/`ctx.waitForSignal`** — swallowing the suspension sentinel breaks the engine. If you must wrap them in a `try`, rethrow when `isWorkflowSuspended(err)` is true.

Wake-ups are driven by the `Scheduler`: the engine registers a handler for the internal `nevo.workflow.wake` task. That task carries a **discriminated payload** — `{ workflowId, kind: "sleep" | "signal-timeout", ordinal, name? }` — telling the handler which durable outcome to record; legacy `{ workflowId }`-only payloads (enqueued before event-driven sleeps existed) still resume the run, they just record nothing first. Crucially, the handler **records the durable outcome of whatever the workflow was waiting on — `workflow.sleep.completed` or `workflow.signal.timeout` — BEFORE re-entering the function**, then calls `engine.resume(workflowId)`. So when the resume replays, the elapsed sleep (or fired timeout) is already a recorded fact and the workflow advances past it deterministically rather than re-checking the wall clock. Recording is idempotent, and a wake-up that races a signal that already satisfied the wait (or a workflow that already finished) is a no-op — the wait stays single-valued.

> **Sleeps and signal timeouts are recorded as their own events** (`workflow.sleep.completed`, `workflow.signal.timeout`), written by the wake-up handler before the replay. A sleep is "done" because that event exists, **not** because `wakeAt` is now in the past — so replay is independent of the wall clock you replay against. This is the core of the determinism contract below.

## Signals & timeouts

`engine.signal(workflowId, name, payload)` appends a `workflow.signal.received` event and resumes the run. Signals are **queued FIFO per name** and delivered to waits in arrival order: each received payload is handed to **exactly one** `waitForSignal` of that name, ever — even across resumes. A wait that has already consumed a payload replays the *same* payload (by recorded queue index), so re-delivering or double-resuming never duplicates a signal. A signal sent before any wait reaches it simply sits in the queue until a wait claims it.

`waitForSignal(name, { timeoutMs })` arms a timeout: if no unclaimed signal arrives within `timeoutMs`, the scheduler fires a `signal-timeout` wake-up that records `workflow.signal.timeout` and the wait throws `WorkflowSignalTimeout` (carrying `.signalName`). Catch it to take a fallback path:

```ts
import { WorkflowSignalTimeout } from "@riaskov/nevo-messaging"

engine.register("approval", async (ctx) => {
  try {
    const approved = await ctx.waitForSignal<boolean>("review", { timeoutMs: 24 * 60 * 60_000 })
    return approved ? "approved" : "rejected"
  } catch (err) {
    if (err instanceof WorkflowSignalTimeout) {
      await ctx.step("escalate", () => escalateToManager(ctx.workflowId))
      return "escalated"
    }
    throw err
  }
})
```

A timeout and a late-arriving signal race is resolved in favour of the signal: if the signal was consumed first, the timeout wake-up is a no-op. Once recorded, the wait's outcome (consumed payload *or* timeout) is single-valued and replays identically. `timeoutMs` requires a `Scheduler`; without one the wait suspends indefinitely until a signal arrives.

## The determinism contract

Replay only produces the same decisions as the original run if the workflow function is **deterministic** between `ctx.*` calls. Everything non-deterministic — clocks, randomness, IO, network calls — must be funnelled through the context so its outcome is recorded and replayed, not recomputed. Concretely:

- **No wall-clock reads.** Do not call `Date.now()`, `new Date()`, `performance.now()`, or `process.hrtime()` directly in workflow body logic. Use `ctx.now()` so the value is consistent across replays.
- **No randomness.** No `Math.random()`, no `crypto.randomUUID()`, no nondeterministic id generation in the body. If you need a random value or id, compute it **inside a `ctx.step`** so the result is recorded once and replayed thereafter.
- **No IO outside `ctx.step`.** Database writes, HTTP calls, message sends, file writes — every side effect must live inside `ctx.step(name, fn)`. Anything you do directly in the body runs again on every replay.
- **Effects only inside `ctx.step`.** `ctx.step` is the *only* place side effects are allowed, and it guarantees they happen exactly once. The body around steps should be pure orchestration: branching, looping, shaping data.
- **Stable, unique step names.** Step results are keyed by name. Re-using a name returns the first recorded result; deriving names from non-deterministic values (e.g. `ctx.step(Date.now() + "", …)`) breaks replay. Prefer stable literals like `"charge"`, `"notify-customer"`.
- **Deterministic control flow.** Branches and loops must depend only on `ctx.input`, recorded step results, and received signals — never on ambient state that can change between runs.

A handy mental model: imagine the function is run twice back-to-back with an identical history. If the second run takes a different branch or calls a different step, you've violated the contract.

## Lifecycle API

```ts
const { workflowId, status, result } = await engine.start("name", input, { workflowId? })
await engine.resume(workflowId)                  // re-enter a suspended workflow (also used internally)
await engine.signal(workflowId, "name", payload) // deliver a signal and resume
await engine.cancel(workflowId)                   // append workflow.cancelled
const state = await engine.getState(workflowId)   // WorkflowState | null
```

`engine.start` runs the workflow synchronously to its first suspension (or completion) and returns the resulting `status` and `result`. Pass `{ workflowId }` to make starts idempotent: the engine de-dups on the id via the store, so re-issuing the same id won't start a second run.

`getState` returns a `WorkflowState`:

```ts
interface WorkflowState {
  workflowId: string
  name: string
  status: "running" | "completed" | "failed" | "cancelled" | "suspended"
  input: unknown
  result?: unknown
  error?: string
  startedAt: number
  completedAt?: number
}
```

## Choosing a store

The engine defaults to `InMemoryEventStore`, which is perfect for tests but loses all history on restart. For anything durable, pass a persistent `EventStore`:

```ts
import { PgEventStore } from "@riaskov/nevo-messaging"
const engine = new WorkflowEngine({ store: new PgEventStore({ client }), scheduler })
```

See the [storage matrix](./storage-matrix.md) for the full backend comparison.

## See also

- [scheduler.md](./scheduler.md) — the `Scheduler` that drives `ctx.sleep` wake-ups
- [cqrs.md](./cqrs.md) — the event store the engine persists to
- [saga.md](./saga.md) — a lighter-weight orchestrator for compensating transactions
- [storage-matrix.md](./storage-matrix.md) — picking a durable backend
