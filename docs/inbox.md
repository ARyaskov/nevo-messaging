# Inbox (consumer-side deduplication)

The inbox is a small wrapper that records every successfully processed message UUID. When a duplicate arrives — because the producer retried, the broker redelivered, or you replayed the DLQ — the inbox short-circuits the handler and returns the cached result.

Pair the inbox with the [outbox](./outbox.md) on the producer side to get **at-least-once** delivery with consumer-side dedup. The delivery guarantee is at-least-once; the projection is exactly-once only when the projection and `markSeen` share one transaction and the store's claim cannot be stolen while the handler runs.

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

- Concurrent calls for the same `uuid` are collapsed by an **in-process leader election**: the first caller leads and runs the handler; the rest await its result instead of re-running.
- For cross-process dedup, if the store implements the optional `claim(uuid)` the leader atomically reserves the key (claim-before-execute) so duplicates on _other_ replicas also wait for the winner's result. Stores without `claim()` fall back to the racy `hasSeen` → `getResult` / `markSeen` check-then-act.
- On a hit the stored result is returned and the handler is skipped; otherwise the handler runs and `store.markSeen(uuid, result)` records the outcome.
- `opts.tx` is a **commit wrapper**, not a connection handle: its type is `(commit: (tx?: unknown) => Promise<void>) => Promise<void>`. The inbox calls `opts.tx(commit)` and, inside your wrapper, you run your projection and invoke `commit(tx)` (which performs `markSeen`) — pass your transaction connection as `tx` so stores that support it (e.g. `PgInboxStore`) write the dedup record inside the same transaction as the projection. (This differs from the older `{ tx: connection }` shape.)

## The `InboxStore` interface

```ts
interface InboxStore {
  hasSeen(uuid: string): Promise<boolean>
  markSeen(uuid: string, result?: unknown): Promise<void>
  getResult(uuid: string): Promise<unknown | undefined>
  // Optional claim-before-execute primitive. The single winner gets
  // `{ acquired: true }` and must run the handler then `markSeen`; losers get the
  // finished result if present, else `{ acquired: false }`. Stores without it fall
  // back to the racy `hasSeen` / `markSeen` check-then-act.
  claim?(uuid: string, opts?: { ttlMs?: number }): Promise<{ acquired: boolean; existing?: unknown }>
}
```

Note that `markSeen` takes no `opts.tx` itself — transactional commit is wired through `Inbox.dedupe(uuid, handler, { tx })` (see above); `PgInboxStore.markSeen(uuid, result, { tx })` accepts an optional store-native transaction handle that the commit wrapper forwards. The built-in `InMemoryInboxStore` is fine for tests and short retry windows; for durability use a shipped store (below) or implement the interface over your database.

## Shipped durable stores

The framework **does** ship SQL/Redis inbox stores — you do not have to hand-roll one:

- **`PgInboxStore`** (`src/common/pg-stores.ts`) — Postgres-backed, with `migrate()` and a `prune()` for TTL cleanup. Best for transactional projections where the projection is exactly-once when you commit the dedup record in the same transaction as the projection (and the claim is not stolen mid-run).
- **`RedisInboxStore`** (`src/common/inbox-redis.ts`) — Redis-backed, implements the optional `claim()` for cross-process leader election, with a `readErrorPolicy` (`"open"` | `"closed"`). Best for high-throughput consumers.

```ts
import { Inbox, PgInboxStore, RedisInboxStore } from "@riaskov/nevo-messaging"

// Postgres
const pgStore = new PgInboxStore({ client: pg /* { query } */, schema: "public" })
await pgStore.migrate()
const inbox = new Inbox({ enabled: true, store: pgStore })

// or Redis
const redisStore = new RedisInboxStore({ client: redisAdapter, ttlMs: 24 * 60 * 60_000 })
const redisInbox = new Inbox({ enabled: true, store: redisStore })
```

See [storage-matrix.md](./storage-matrix.md) for when to pick each.

## Transactional, exactly-once projections

Prefer the shipped `PgInboxStore` over a hand-rolled one. Even then the delivery contract is **at-least-once**; the projection becomes exactly-once only when the projection and `markSeen` commit in one transaction and the claim is not stolen mid-run. Pass `opts.tx` to `dedupe` — it is a **commit wrapper** `(commit) => Promise<void>`: you own BEGIN/COMMIT, run the projection, call `commit()` (which runs the store's `markSeen`) and resolve the wrapper **only after your COMMIT resolves** — the inbox settles its local lease (waking followers) only after the wrapper resolves:

```ts
const client = await pool.connect()
try {
  await client.query("BEGIN")
  const result = await inbox.dedupe(
    env.uuid,
    async () => {
      await applyProjection(client, env.payload) // your write, on the txn connection
      return { ok: true }
    },
    {
      // The inbox invokes this with `commit` on the executing path only;
      // everything — projection, markSeen and COMMIT — happens inside your
      // BEGIN, and the wrapper resolves only after COMMIT so local followers
      // never observe an uncommitted result.
      tx: async (commit) => {
        await commit(client)
        await client.query("COMMIT")
      }
    }
  )
  return result
} catch (e) {
  // A retryable busy/pending claim (thrown as a MessagingError) or a failed
  // COMMIT propagates here; rethrow so the message is redelivered.
  throw e
} finally {
  // Duplicate/cached and follower calls return before opts.tx runs, so the
  // BEGIN above can still be open when we get here. ROLLBACK is a no-op after
  // a successful COMMIT, and this guarantees no client is released into the
  // pool with an open transaction.
  await client.query("ROLLBACK").catch(() => {})
  client.release()
}
```

If the wrapper rejects (COMMIT never happened), `ROLLBACK` still applies in the `finally` and the duplicate will be retried — the state remains at-least-once with a rolled-back transaction. The `finally` `ROLLBACK` also covers the calls that return before `opts.tx` runs (duplicate hit, cached result, follower, or a thrown retryable busy/pending error): their `BEGIN` would otherwise be released into the pool still open. Note that a mid-run claim steal by another worker (claim TTL expiry) breaks exactly-once; size `claimTtlMs` above your worst-case handler duration.

If you need a store that writes `markSeen` on the _very same connection_ as your projection (rather than the store's own connection), implement `InboxStore` over that connection — the shape is small. But for the common case the shipped `PgInboxStore` plus the commit-wrapper above is enough — with the guarantee limited to at-least-once unless the projection and `markSeen` share one transaction and the claim is never stolen mid-run.

## Difference from the idempotency cache

[Idempotency cache](./idempotency.md) is in-memory, per-process, short-lived. It is the right tool for retries within a few seconds.

The inbox is intended to be durable, shared, and to survive restarts and operator replays. It is appropriate when the projection must be exactly-once for correctness (financial postings, immutable history) — that exactly-once is an application-level requirement you satisfy via the shared-transaction pattern above, not a guarantee the Inbox primitive itself provides.

## Pruning

`InMemoryInboxStore` keeps everything in a map and self-evicts via its LRU/TTL. For durable stores, prune entries older than the producer's retry window.

`PgInboxStore` ships a `prune()` that deletes rows older than its configured `ttlMs` (default 24h) — call it from a daily cron:

```ts
const store = new PgInboxStore({ client: pg, ttlMs: 24 * 60 * 60_000 })
// …later, on a schedule…
const removed = await store.prune()
```

`RedisInboxStore` needs no prune job — each entry carries a Redis TTL (`ttlMs`, default 24h) and expires on its own. If you implement a custom DB-backed store, add an equivalent `DELETE ... WHERE seen_at < now() - retentionWindow`.

A daily prune job is usually plenty. Don't prune more aggressively than the producer's retry window.
