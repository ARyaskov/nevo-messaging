# Saga orchestrator

A saga is a sequence of local transactions where each step has a **compensating action** that semantically undoes it. Use sagas when one global transaction across services is impossible.

Classic example: an order saga reserves inventory, charges the card, books shipping. If shipping fails, the saga refunds the card and releases the hold.

## API

```ts
import { createSaga, type SagaResult } from "@riaskov/nevo-messaging"

interface OrderCtx {
  userId: bigint
  items: Item[]
  total: number
  reservationId?: string
  chargeId?: string
  shipmentId?: string
}

const orderSaga = createSaga<OrderCtx>()
  .step({
    name: "reserveStock",
    execute: async (ctx) => {
      ctx.reservationId = await this.query("inventory", "item.reserve", { items: ctx.items })
    },
    compensate: async (ctx) => {
      if (ctx.reservationId) {
        await this.emit("inventory", "item.release", { reservationId: ctx.reservationId })
      }
    }
  })
  .step({
    name: "chargeCard",
    execute: async (ctx) => {
      ctx.chargeId = await this.query("payment", "card.charge", {
        userId: ctx.userId, amount: ctx.total
      })
    },
    compensate: async (ctx) => {
      if (ctx.chargeId) {
        await this.emit("payment", "card.refund", { chargeId: ctx.chargeId })
      }
    }
  })
  .step({
    name: "bookShipping",
    execute: async (ctx) => {
      ctx.shipmentId = await this.query("shipping", "shipment.book", { items: ctx.items })
    }
    // last step → no compensation needed
  })

const result: SagaResult = await orderSaga.run({ userId, items, total })
// result.status: "completed" | "failed" | "compensated" | "compensate-failed"
// result.executed: which steps ran
// result.compensated: which compensations ran
// result.error: the failure that triggered compensation
```

## Step options

Each step accepts retry and timeout knobs for both the execute and the compensate phase:

```ts
{
  name: "chargeCard",
  execute,
  compensate,

  // Execute phase
  retries: 3,
  timeoutMs: 5_000,
  backoff: { baseMs: 200, maxMs: 2_000, jitter: true },

  // Compensate phase (independent settings)
  compensateRetries: 5,
  compensateTimeoutMs: 10_000,
  compensateBackoff: { baseMs: 500, maxMs: 5_000, jitter: true }
}
```

If a step's `execute` exhausts retries the saga begins compensation; if a compensation exhausts its retries the saga ends with `status: "compensate-failed"` and the failing entry is recorded in `result.compensated`.

## Compensation ordering & ctx isolation

If step `N` fails, the orchestrator runs compensations for steps `N-1`, `N-2`, … `0` in reverse order. Each compensation receives a **deep clone** of the context (via `structuredClone`) — later step mutations cannot retroactively break earlier compensations.

## Persistence — resume after restart

In-memory sagas die with the process. To survive restarts, attach a store:

```ts
import { InMemorySagaStore, type SagaStore } from "@riaskov/nevo-messaging"

const store: SagaStore = new InMemorySagaStore()  // swap for your own implementation
const sagaId = "order:" + crypto.randomUUID()

const saga = createSaga<OrderCtx>()
  .withStore(store, sagaId)
  .step({ name: "reserveStock", execute, compensate })
  .step({ name: "chargeCard",   execute, compensate })

const result = await saga.run(initialCtx)
```

On boot, scan the store for pending sagas and resume them:

```ts
import { Saga } from "@riaskov/nevo-messaging"

const pending = await store.listPending()
for (const snap of pending) {
  const steps = rebuildSteps()
  const result = await Saga.resume<OrderCtx>(store, snap.sagaId, steps)
  console.log(`Resumed ${snap.sagaId}: ${result.status}`)
}
```

The `SagaStore` interface is small (4 methods: `save`, `load`, `listPending`, `delete`). Implement it over Postgres / Redis / SQLite as you need.

## Idempotent steps

Each step should be idempotent on the downstream side — the orchestrator may invoke `execute` twice if the process crashes mid-step or if you `resume` after restart. Combine with the [idempotency cache](./idempotency.md) on the called service.

## Result shape

```ts
interface SagaResult {
  status: "completed" | "failed" | "compensated" | "compensate-failed"
  error?: string
  executed: string[]                                          // step names that ran
  compensated: Array<{ step: string; ok: boolean; error?: string }>
  sagaId: string
}
```

Inspect `compensated` to see whether each compensation succeeded. A `compensate-failed` saga needs human attention — usually routed to the [DLQ](./dlq.md) or an ops dashboard.

## When not to use a saga

- **Single-service workflow** — use a local DB transaction.
- **Best-effort fire-and-forget** — emit and trust the world; sagas are for "must compensate on failure".
- **Long-running (hours / days)** — use a real workflow engine (Temporal, Cadence). Sagas are for sub-second to minutes-scale orchestration.
