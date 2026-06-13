# Scheduler

`Scheduler` is a durable, cluster-safe job scheduler: run a task once at a point in time, once after a delay, or repeatedly on a cron expression. Tasks are persisted through a pluggable `ScheduledTaskStore`, claimed with a lease so multiple replicas cooperate without double-firing, and reaped if a worker dies mid-run.

Sources: `src/common/scheduler.ts`, `src/common/cron.ts`.

## Quick start

```ts
import { Scheduler, PgScheduledTaskStore } from "@riaskov/nevo-messaging"

const scheduler = new Scheduler({
  store: new PgScheduledTaskStore({ client })   // defaults to in-memory
})

scheduler.registerHandler("send-digest", async (payload) => {
  await mailer.sendDigest(payload)
})

await scheduler.enqueueIn("send-digest", { userId: "u-1" }, 5 * 60_000)   // in 5 minutes
await scheduler.enqueueAt("send-digest", { userId: "u-2" }, Date.now() + 60_000)
await scheduler.enqueueCron("send-digest", { all: true }, "0 9 * * *")    // daily at 09:00

scheduler.start()   // begin polling; call scheduler.stop() on shutdown
```

`start()` begins a self-rescheduling poll loop (the timer is `unref`-ed so it never keeps the process alive on its own). For tests, skip `start()` and drive a single pass with `await scheduler.flushOnce()`.

## The `@Scheduled` decorator + discovery

Annotate methods and let discovery register the handler and enqueue the first run.

```ts
import { Injectable } from "@nestjs/common"
import { Scheduled } from "@riaskov/nevo-messaging"

@Injectable()
export class Reports {
  @Scheduled({ cron: "0 9 * * *", timezone: "America/New_York" })
  async dailyDigest() { /* … */ }

  @Scheduled({ in: 10_000 })           // one-shot, 10s after registration
  async warmCaches() { /* … */ }

  @Scheduled({ at: Date.parse("2026-01-01T00:00:00Z"), name: "new-year" })
  async newYear() { /* … */ }
}
```

```ts
import { discoverAndRegisterScheduled } from "@riaskov/nevo-messaging"

await discoverAndRegisterScheduled(scheduler, [reports])
scheduler.start()
```

`@Scheduled(options?)` options:

| Option | Description |
|---|---|
| `name` | Logical handler name. Defaults to `Class#method`. |
| `cron` | Cron expression for repeatable runs. |
| `at` | One-shot run at this epoch (ms) or `Date`. |
| `in` | One-shot run this many ms after registration. |
| `maxAttempts` | Override the scheduler-wide retry cap (reserved on the decorator metadata). |
| `timezone` | IANA timezone for cron evaluation, e.g. `"America/New_York"`. |
| `utc` | Evaluate cron in UTC. Shorthand for `timezone: "UTC"`. |

The decorated method is invoked with the task payload as its single argument.

## Enqueue API

```ts
scheduler.enqueueAt(name, payload, runAt, { id?, maxAttempts? }) // epoch ms or Date → task id
scheduler.enqueueIn(name, payload, ms, { id?, maxAttempts? })    // delay from now → task id
scheduler.enqueueCron(name, payload, cron, opts?)                // recurring → task id
scheduler.cancel(id)                                 // cancel a pending task (no-op if already ran)
scheduler.list({ status?, limit? })                  // inspect tasks
scheduler.registerHandler(name, handler)             // (name) → handler
scheduler.unregisterHandler(name)
scheduler.hasHandler(name)
```

## Cron syntax

The bundled parser is a standard **5-field POSIX cron**: `minute hour day month weekday`.

| Field | Range |
|---|---|
| minute | 0–59 |
| hour | 0–23 |
| day of month | 1–31 |
| month | 1–12 |
| weekday | 0–7 (Sunday = 0 or 7) |

Supported operators: `*` (wildcard), `,` (list), `-` (range), `/` (step). Quartz extensions (`L`, `#`, `?`) are **not** supported. Examples:

| Expression | Meaning |
|---|---|
| `* * * * *` | every minute |
| `0 9 * * *` | 09:00 every day |
| `*/15 * * * *` | every 15 minutes |
| `0 0 1 * *` | midnight on the 1st of each month |
| `0 0 13 * 5` | midnight on the 13th **or** any Friday |

> **POSIX day-of-month / day-of-week OR rule.** When *both* the day-of-month and weekday fields are restricted (neither is `*`), the day matches if **either** matches — hence `0 0 13 * 5` fires on the 13th OR on Fridays. When only one is restricted, only that one applies. Note that `*/2` is **not** a wildcard — a step restricts the field, so it does not trigger the OR rule.

`isValidCron(expr)` returns a boolean; `enqueueCron` throws on an invalid expression. The lower-level `nextCronTick(expr, from, opts)` returns the next firing at-or-after `from` in epoch ms (or `-1` if none in the next ~4 years), and `parseCron(expr)` exposes the parsed fields.

## Timezone & UTC semantics

By default cron is evaluated in the **server's local time** (matching `Date` getters). Override per task:

- `{ utc: true }` — evaluate in UTC.
- `{ timezone: "Area/City" }` — evaluate in any IANA zone, resolved via `Intl.DateTimeFormat`.

The chosen zone is **persisted on the task row** so every reschedule uses the same rule, even if the worker that reschedules it lives in a different region. DST is handled correctly: wall-clock times are mapped back to real epoch instants, and a wall-clock time that doesn't exist on a spring-forward day is skipped.

## Cluster safety

### Deterministic per-cluster cron id

`enqueueCron` derives the task id from the logical name (`cron:<name>`). Every replica that runs discovery enqueues *the same row*, and the store de-dups by id. The result: **a cron fires once per cluster per tick, not once per replica.** Re-registering an unchanged definition is a no-op. Changing the expression, timezone, or logical name updates the existing row and resets it to the new schedule.

> Use **distinct names** for distinct schedules. One-shot `at`/`in` tasks get a fresh UUID by default. Pass `{ id }` when the caller needs a deterministic, idempotent enqueue; workflow wake-ups use this to avoid duplicate timers during replay.

### Lease claiming & reaping

`flushOnce` claims due tasks via `claimDue(workerId, now, batchSize, claimTtlMs)`, which stamps `claimed_by`/`claimed_at` and flips the row to `running` (Postgres uses `FOR UPDATE SKIP LOCKED` so replicas don't race). A task is claimable when it is `pending`, **or** when it is stuck in `running` past its lease (`claimTtlMs`, default 60s) — the worker that held it likely crashed. Without this reaper a stuck task (and any cron behind it) would be stranded forever.

Finalizers (`markCompleted`/`markFailed`/`reschedule`) are **fenced**: they only mutate a row that is still `running` and still owned by the calling `workerId`. So if the reaper hands a stalled worker's task to a second worker, the original worker — should it finally finish — cannot double-bump `attempts` or re-reschedule the row the reaper now owns.

### Retries & missed-run policy

A failing one-shot handler increments `attempts`; the row goes back to `pending` until `maxAttempts` (default 5) is reached, after which it is marked `failed`. A handler with no registered handler name fails immediately.

For cron tasks, reaching `maxAttempts` records the last error and advances to the next cron tick instead of terminally deleting the recurring schedule. The next tick is computed from the task's **scheduled** `runAt`, not the wall clock at completion, so a slow handler doesn't drift the cadence. The missed-run policy is **SKIP**: if the worker was down or the handler ran past one or more ticks, the scheduler jumps to the next tick after *now* rather than replaying every missed occurrence.

## Scheduler options

```ts
new Scheduler({
  store,                  // ScheduledTaskStore (default: InMemoryScheduledTaskStore)
  pollIntervalMs: 1000,   // min 50
  batchSize: 20,          // tasks claimed per tick, min 1
  claimTtlMs: 60_000,     // lease length before a stuck task is reclaimable, min 1000
  maxAttempts: 5,         // retries before a task is marked failed, min 1
  workerId,               // defaults to a unique "worker-<uuid>" id
  logger
})
```

## Choosing a store

`InMemoryScheduledTaskStore` (default) is fine for tests and single-pod CLIs but loses everything on restart. It prunes terminal tasks after one hour and caps retained terminal rows at 10,000 by default; override with `{ terminalRetentionMs, maxTerminalTasks }`. For production use `PgScheduledTaskStore`, which gives durable rows, DB-clock leases shared across replicas, and the deterministic-id de-dup described above. Run its `migrate()` in your deploy pipeline (or `migrateAllPgStores`). See the [storage matrix](./storage-matrix.md).

## See also

- [workflow.md](./workflow.md) — the workflow engine uses the scheduler to drive `ctx.sleep`
- [storage-matrix.md](./storage-matrix.md) — `PgScheduledTaskStore` and the rest of the backends
- [outbox.md](./outbox.md) — the same lease/fence pattern, for the transactional outbox
