# Inbox (consumer-side deduplication)

The inbox is a small wrapper that records every successfully processed message UUID. When a duplicate arrives — because the producer retried, the broker redelivered, or you replayed the DLQ — the inbox short-circuits the handler and returns the cached result.

Pair the inbox with the [outbox](./outbox.md) on the producer side to get effectively exactly-once semantics across the wire.

## Real API

```ts
import { Inbox, InMemoryInboxStore, type InboxStore } from "@riaskov/nevo-messaging"

const store: InboxStore = new InMemoryInboxStore()
const inbox = new Inbox({ enabled: true, store })

// Inside a handler:
async function onOrderPlaced(envelope: { uuid: string; payload: OrderPayload }) {
  return inbox.dedupe(envelope.uuid, async () => {
    // your real work — runs at most once per uuid
    await applyProjection(envelope.payload)
    return { ok: true }
  })
}
```

`Inbox.dedupe(uuid, handler, opts?)`:

- If `store.hasSeen(uuid)` returns `true`, the handler is skipped and the previous result is returned from `store.getResult(uuid)`.
- Otherwise, the handler runs and `store.markSeen(uuid, result)` is called with the outcome.
- The `opts.tx` field is forwarded to the store unchanged — useful for storing the inbox entry in the same DB transaction as the projection.

## The `InboxStore` interface

```ts
interface InboxStore {
  hasSeen(uuid: string): Promise<boolean>
  markSeen(uuid: string, result: unknown, opts?: { tx?: unknown }): Promise<void>
  getResult(uuid: string): Promise<unknown>
}
```

The built-in `InMemoryInboxStore` is fine for tests and short retry windows. For durability, implement the interface over your database. Doing the `markSeen` call inside the same DB transaction as your projection update is what makes the pair exactly-once.

## Sketch: Postgres-backed inbox

```ts
import { Pool, PoolClient } from "pg"

class PgInboxStore implements InboxStore {
  constructor(private pool: Pool) {}

  async hasSeen(uuid: string) {
    const { rowCount } = await this.pool.query(
      "SELECT 1 FROM inbox WHERE uuid = $1", [uuid]
    )
    return rowCount > 0
  }

  async markSeen(uuid: string, result: unknown, opts?: { tx?: PoolClient }) {
    const exec = opts?.tx ?? this.pool
    await exec.query(
      "INSERT INTO inbox(uuid, result, seen_at) VALUES ($1, $2, NOW()) ON CONFLICT DO NOTHING",
      [uuid, JSON.stringify(result)]
    )
  }

  async getResult(uuid: string) {
    const { rows } = await this.pool.query(
      "SELECT result FROM inbox WHERE uuid = $1", [uuid]
    )
    return rows[0] ? JSON.parse(rows[0].result) : null
  }
}
```

Wrap your projection in a transaction and pass the connection through `opts.tx`:

```ts
await pool.query("BEGIN")
try {
  const result = await inbox.dedupe(env.uuid, async () => {
    await applyProjection(client, env.payload)
    return { ok: true }
  }, { tx: client })
  await pool.query("COMMIT")
} catch (e) {
  await pool.query("ROLLBACK")
  throw e
}
```

The framework does NOT ship a SQL store — implement the interface above for your DB.

## Difference from the idempotency cache

[Idempotency cache](./idempotency.md) is in-memory, per-process, short-lived. It is the right tool for retries within a few seconds.

The inbox is intended to be durable, shared, and to survive restarts and operator replays. It is appropriate when the projection must be exactly-once for correctness (financial postings, immutable history).

## Pruning

`InMemoryInboxStore` keeps everything in a map. For long-running services, prune entries older than the producer's retry window. The interface doesn't ship a `prune()` method — add one to your DB-backed implementation:

```ts
async pruneOlderThan(olderThanMs: number) {
  await this.pool.query(
    "DELETE FROM inbox WHERE seen_at < NOW() - $1::interval",
    [`${olderThanMs} milliseconds`]
  )
}
```

A daily prune job is usually plenty. Don't prune more aggressively than the producer's retry window.
