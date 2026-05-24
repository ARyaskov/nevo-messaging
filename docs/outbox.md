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

```ts
const recordId = await outbox.enqueue("notifications", "user.created", { userId: 123n })
```

`enqueue(serviceName, method, params)` writes the record synchronously to the store. With `SqliteOutboxStore`, that write is durable across crashes.

To make the write atomic with your business state, perform both inside the same DB transaction. `SqliteOutboxStore` opens its own connection, so you would typically share a single connection or use a different DB. The simplest pattern: use a Postgres/MySQL implementation that lets you pass an existing transaction handle.

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

Failed records are kept in the store for inspection. Replay manually by resetting their status, or route them to the [DLQ](./dlq.md).

## Delivery guarantees

- **At-least-once** to the broker
- Combine with the consumer's [idempotency cache](./idempotency.md) or [inbox](./inbox.md) for effectively-exactly-once
- Ordering is **not** preserved across records — `flushOnce()` drains a batch and publishes in parallel

If you need ordering, group related records and drain serially in your custom publisher.

## SQLite store

`SqliteOutboxStore` uses Node's built-in `node:sqlite` (Node ≥ 24). The table is provisioned on first use:

```sql
CREATE TABLE IF NOT EXISTS outbox (
  id            TEXT PRIMARY KEY,
  service_name  TEXT NOT NULL,
  method        TEXT NOT NULL,
  params        BLOB NOT NULL,
  created_at    INTEGER NOT NULL,
  published_at  INTEGER,
  attempts      INTEGER DEFAULT 0,
  status        TEXT NOT NULL,
  last_error    TEXT
);
```

Options:

```ts
new SqliteOutboxStore({
  path: "./outbox.db",     // default ":memory:"
  tableName: "outbox",
  pragma: { journal_mode: "WAL", synchronous: "NORMAL" }
})
```

For Postgres or MySQL, implement the small `OutboxStore` interface (4 methods: `save`, `markPublished`, `markFailed`, `listPending`).

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
