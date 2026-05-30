# Transactional outbox

The outbox pattern guarantees that a message is eventually published **after** your local state change, even when the broker is unreachable at write time.

The naïve approach (`db.commit(); broker.publish()`) is broken — the process can crash between the two lines. The outbox writes the intended publish to a persistent store at the same time you change state, then a background loop drains the store to the broker with retry.

## API

```ts
import {
  Outbox, InMemoryOutboxStore, SqliteOutboxStore,
  type OutboxPublisher
} from "@riaskov/nevo-messaging"

// Any object with `emit()` works — typically a Nevo client base
const publisher: OutboxPublisher = {
  emit: (service, method, params) => natsClient.emit(service, method, params)
}

const store = new SqliteOutboxStore({ path: "./outbox.db" })  // or InMemoryOutboxStore
const outbox = new Outbox(store, publisher, {
  intervalMs: 200,   // how often to poll for pending records
  batch: 100,        // max records per drain
  maxAttempts: 10    // record moves to "failed" after this many tries
})

outbox.start()
```

### Enqueueing

> [!WARNING]
> **Enqueueing outside the business transaction defeats the entire pattern.**
> `enqueue(serviceName, method, params)` / `store.save(record)` with no transaction
> handle writes the outbox row on the store's *own* connection, decoupled from your
> business commit. A crash *after* the business commit but *before* the outbox write
> **loses the event**; writing the outbox row *first* and then crashing before the
> business commit **publishes a phantom event** for a change that never happened.
> Always enqueue inside the same transaction as the state change.

The safe way is to pass a transaction handle so the business row and the outbox row
commit (or roll back) together. Use `withOutboxTransaction(client, fn)` and forward
the `tx` it hands you to `enqueue`/`save`:

```ts
import { withOutboxTransaction } from "@riaskov/nevo-messaging"

// `client` is your live connection (a pooled Postgres client, a node:sqlite
// DatabaseSync, or — in tests — an InMemoryOutboxStore).
await withOutboxTransaction(pgClient, async (tx) => {
  await tx.query("INSERT INTO orders (id, total) VALUES ($1, $2)", [id, total])  // business state
  await outbox.enqueue("notifications", "order.placed", { id }, { tx })          // outbox row, same tx
})
// Both committed, or — if anything throws — both rolled back. No lost or phantom events.
```

`withOutboxTransaction` issues `BEGIN` / `COMMIT` / `ROLLBACK` for you and rolls back
automatically if `fn` throws. You can equally call `store.save(record, tx)` directly.
`PgOutboxStore` accepts a `PgClient` as `tx`; `SqliteOutboxStore` accepts a `DatabaseSync`
(share it via `new SqliteOutboxStore({ db })` so the outbox and your tables live on one
connection).

Pass a `partitionKey` to keep related events ordered on the broker (see
[Delivery guarantees](#delivery-guarantees)):

```ts
await outbox.enqueue("orders", "order.shipped", { id }, { tx, partitionKey: `order:${id}` })
```

### Background drain

`outbox.start()` runs an interval timer that calls `flushOnce()`:

```ts
const { published, failed } = await outbox.flushOnce()
```

You can call `flushOnce()` directly (e.g. from a request handler that wants synchronous delivery on the happy path).

### Shutdown

```ts
await outbox.stop()
```

Pair with [graceful shutdown](./graceful-shutdown.md) so the loop drains before the process exits.

## Record lifecycle

Each record has a `status`:

| Status | Meaning |
|---|---|
| `pending` | Not yet attempted |
| `published` | Successfully delivered to the broker |
| `failed` | Reached `maxAttempts`; will not be retried automatically |

The `maxAttempts` you configure is enforced **by the store**: `markFailed` increments
`attempts`, flips the row to `failed` once `attempts >= maxAttempts`, and reports the
resulting status back to the relay (it is not recomputed client-side). A row stays
`pending` and is retried on the next drain until it is parked.

Failed records are kept in the store for inspection. Replay manually by resetting their status, or route them to the [DLQ](./dlq.md).

## Delivery guarantees

- **At-least-once** to the broker. A relay can still re-deliver a message (e.g. it
  publishes, then crashes before recording success, or its claim TTL lapses and a
  second worker re-publishes). Combine with the consumer's
  [idempotency cache](./idempotency.md) or [inbox](./inbox.md) for effectively-exactly-once.
- **No double *finalization*.** With `PgOutboxStore`, `listPending` leases rows with
  `FOR UPDATE SKIP LOCKED` and stamps `claimed_by`. `markPublished`/`markFailed` only
  mutate a row still owned by *this* worker and still `pending`; if the claim was stolen
  the update is a no-op (logged, not counted), so the outbox state is never advanced twice.
- **Per-partition ordering.** Records sharing a `partitionKey` are relayed strictly in
  `createdAt` order, and a partition **halts at its first failure** — a later event never
  overtakes a stuck earlier one, on the first pass or on retry. Records *without* a
  `partitionKey` are independent and relayed together (a failure of one does not block others).
- **Partial-batch safe.** A publisher's `emitBatch` may return a per-item result array
  (`{ ok, error }[]`); items the broker accepted are marked published and never re-sent.
  Returning `void` keeps the legacy all-or-nothing contract (resolve = all accepted, throw = none).

If you need global ordering, give every related record the same `partitionKey`.

## SQLite store

`SqliteOutboxStore` uses Node's built-in `node:sqlite` (Node ≥ 24). The table is provisioned on first use:

```sql
CREATE TABLE IF NOT EXISTS nevo_outbox (
  id            TEXT PRIMARY KEY,
  service_name  TEXT NOT NULL,
  method        TEXT NOT NULL,
  params_json   TEXT NOT NULL,
  partition_key TEXT,
  created_at    INTEGER NOT NULL,
  published_at  INTEGER,
  attempts      INTEGER NOT NULL DEFAULT 0,
  status        TEXT NOT NULL DEFAULT 'pending',
  last_error    TEXT
);
```

Options:

```ts
new SqliteOutboxStore({
  path: "./outbox.db",     // default ":memory:"
  tableName: "nevo_outbox",
  pragma: ["journal_mode = WAL", "synchronous = NORMAL"],
  // Optional: share an existing DatabaseSync so the outbox write commits in the
  // SAME transaction as your business tables (see "Enqueueing" above).
  db: myDatabaseSync
})
```

For Postgres or MySQL, implement the small `OutboxStore` interface:

```ts
interface OutboxStore {
  // `tx` is a caller-owned transaction handle — write the row in the business tx.
  save(record: OutboxRecord, tx?: unknown): Promise<void>
  // Returns { owned, status, attempts }; owned=false means the claim was stolen.
  markPublished(id: string): Promise<OutboxMarkResult>
  // Honors maxAttempts and returns the resulting status; owned=false if stolen.
  markFailed(id: string, error: string, maxAttempts: number): Promise<OutboxMarkResult>
  listPending(limit: number): Promise<OutboxRecord[]>
}
```

A multi-worker SQL store should lease rows (`SELECT … FOR UPDATE SKIP LOCKED`, stamping
a `claimed_by`) and guard `markPublished`/`markFailed` with `AND claimed_by = $worker AND
status = 'pending'`, returning `owned: false` when no row matches — exactly what
`PgOutboxStore` does.

## Limitations of the in-process design

- The outbox runs in the same process as the producer; nothing forwards records if the producer dies and never restarts.
- Multiple producer instances should each have their own store, or use a SQL store with `SELECT … FOR UPDATE SKIP LOCKED` semantics to lease records (you implement this in the store interface).

## Monitoring

- Number of records with `status = "pending"` — should stay near zero
- Time-since-`created_at` of the oldest pending record — surfaces broker outages
- Records with `status = "failed"` — needs operator attention

Surface these as gauges via your [metrics](./metrics.md) registry.

## See also

- [inbox.md](./inbox.md) — consumer-side counterpart
- [dlq.md](./dlq.md) — where exhausted records can go
- [graceful-shutdown.md](./graceful-shutdown.md) — draining the outbox on SIGTERM
