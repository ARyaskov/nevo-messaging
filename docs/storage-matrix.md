# Production storage matrix

`nevo-messaging` separates **primitives** (saga, outbox, inbox, event-store, DLQ, idempotency, rate-limit, audit) from **storage** behind narrow interfaces. Picking the right backend per primitive is the difference between a working single-pod demo and a production fleet.

This page is the cheat sheet.

## At a glance

| Primitive | In-memory | SQLite | Redis | Postgres | Right choice for prod |
|---|---|---|---|---|---|
| **Idempotency** | ✅ | — | ✅ `RedisIdempotencyStore` | — | Redis (with in-proc L1) |
| **Inbox** | ✅ | — | ✅ `RedisInboxStore` | ✅ `PgInboxStore` | Postgres if you already have it; Redis otherwise |
| **Outbox** | ✅ | ✅ `SqliteOutboxStore` | — | ✅ `PgOutboxStore` | Postgres (`FOR UPDATE SKIP LOCKED`) |
| **Saga** | ✅ | — | — | ✅ `PgSagaStore` | Postgres |
| **EventStore** | ✅ | — | — | ✅ `PgEventStore` | Postgres |
| **DLQ** | ✅ | — | — | ✅ `PgDlqStore` | Postgres (queryable JSONB) |
| **Rate-limit** | ✅ | — | ✅ `RedisRateLimiter` | — | Redis if you scale horizontally |
| **Audit log** | ✅ | — | — | ✅ `PgAuditSink` + `FileAuditSink` | Postgres + file tee for compliance |

## Decision rules

### Idempotency

- **Single replica** → `LruIdempotencyCache` (default). Process-local, sub-µs.
- **Multi-replica** → `RedisIdempotencyStore`. The store keeps an in-proc L1 LRU automatically, so repeated hits on the same replica never round-trip to Redis.
- **Why no Postgres** → idempotency hits are on the hot path. Redis hash ops are an order of magnitude faster than even a hot Postgres TCP connection.

### Inbox

The inbox dedupes messages on the **consumer** side after a broker redeliver. Two distinct profiles:

- **Latency-sensitive consumers (Kafka >50k msg/s)** → `RedisInboxStore`. ~1ms per `markSeen`, hot working set fits in memory.
- **Transactional projections** → `PgInboxStore`. Pair `markSeen` with the projection write in the same transaction (`Inbox.dedupe(uuid, handler, { tx })`). Slower but gives true exactly-once.

### Outbox

`PgOutboxStore` is the *only* production-grade option:

- Writes the outbox row in the same transaction that mutates business state — atomic.
- `listPending()` uses `SELECT … FOR UPDATE SKIP LOCKED` so multiple workers don't race.
- Stale claims (default 60s) are reclaimable so a dead worker doesn't strand rows.

`SqliteOutboxStore` is fine for single-pod CLIs and for tests; not for multi-replica.

### Saga / EventStore

These are inherently durable concepts — anything in-memory after a crash is useless. Use Postgres unless you're prototyping. Both come with `migrate()` helpers; run them in your deploy pipeline.

`PgEventStore` includes a polling `subscribe(from, handler)` — fine for catch-up projections. For low-latency dispatch wire `LISTEN/NOTIFY` separately.

### DLQ

In-memory DLQ is a ring buffer (default cap 1 000) and is fine if you've already routed failures elsewhere (Sentry, OpsGenie). For replay + ops-tooling, use `PgDlqStore` — queryable by topic/method/error_code/time window, JSONB payload preserved.

### Rate-limit

- **No horizontal scaling** → in-process `RateLimiter`. Zero RTT.
- **Horizontal scaling** → `RedisRateLimiter`. Atomic Lua script, ~1ms per check.
- **Bursty traffic with both per-pod and fleet caps** → `RedisRateLimiter.withLocalShield(local, remote)`. Local protects against single-pod blow-ups; Redis enforces the cluster ceiling.

### Audit log

For compliance (SOC2, HIPAA, PCI), it's typical to write to **two** sinks:

- `PgAuditSink` — queryable.
- `FileAuditSink` with `fsync: true` — survives DB outages, append-only by file system.

Wire them through `TeeAuditSink([pg, file])`.

## Per-store reference

Every store implements a narrow interface (`OutboxStore`, `InboxStore`, etc.) so the primitive that uses it never depends on the backend. The table below is the full set of concrete stores that ship today.

### Postgres stores (`pg-stores.ts`)

All Postgres stores take a `PgClient` and an optional `schema`/`table`, and expose a `migrate()` that creates the table + indexes idempotently. `migrateAllPgStores(client, schema?)` runs every one.

| Store | Interface | Table (default) | Notes |
|---|---|---|---|
| `PgOutboxStore` | `OutboxStore` | `nevo_outbox` | `save(record, tx?)` writes the row in **your** business transaction. `listPending` claims with `FOR UPDATE SKIP LOCKED`; `markPublished`/`markFailed` are fenced by `claimed_by` + `status='pending'`. Stale claims (default 60s) are reclaimable. |
| `PgInboxStore` | `InboxStore` | `nevo_inbox` | `hasSeen`/`markSeen` for consumer-side dedup; `markSeen` is `ON CONFLICT DO NOTHING`. `prune()` deletes rows older than `ttlMs` (default 24h) — call from a daily cron. |
| `PgSagaStore` | `SagaStore` | `nevo_saga` | `save` upserts the snapshot; `listPending()` returns `pending`/`compensating` sagas for crash recovery. `type` column backfilled for older tables. |
| `PgEventStore` | `EventStore` | `nevo_events` | `BIGSERIAL` sequence. `append` returns the assigned `sequence`/`ts`; `read(range)` filters by sequence/type/aggregate; `subscribe(from, handler)` is a polling catch-up loop (wire `LISTEN/NOTIFY` separately for low latency). |
| `PgDlqStore` | `DlqStore` | `nevo_dlq` | Queryable by `topic`/`method`/`reason`/`error_code`/time window; full entry preserved as JSONB. `stats()` aggregates by reason/code/method. |
| `PgScheduledTaskStore` | `ScheduledTaskStore` | `nevo_scheduled` | `claimDue` claims pending tasks **and** reaps `running` tasks past their lease (DB-clock `NOW()`). Finalizers fenced by `claimed_by` + `status='running'`. Cron `timezone` persisted per row. |

> The audit Postgres sink, `PgAuditSink` (`audit-log.ts`), is a separate `AuditSink` (table `nevo_audit`) rather than one of the `pg-stores.ts` stores, and is **not** covered by `migrateAllPgStores` — create its table yourself (DDL is in [audit-log.md](./audit-log.md)).

### Redis stores

Each takes a minimal client shape (a handful of lines over `ioredis` / `node-redis` / `upstash`):

| Store | Interface | Client shape | Notes |
|---|---|---|---|
| `RedisIdempotencyStore` | `IdempotencyStore` | `IdempotencyRedisLike` | In-proc **L1 LRU** in front of Redis. Atomic `claim` (`SET NX PX` of an in-progress sentinel) → single winner executes, losers `awaitResult`. `readErrorPolicy: "open"` (default) or `"closed"`. |
| `RedisInboxStore` | `InboxStore` | `InboxRedisClient` | Same claim-before-execute model for consumer dedup. `hasSeen`/`claim`/`markSeen`/`getResult`. `readErrorPolicy` controls fail-open vs fail-closed on read errors. TTL default 24h. |
| `RedisRateLimiter` | (rate limiter) | `RateLimitRedisClient` | Atomic token-bucket **Lua** script (refill + consume in one round-trip). `failOpen` (default true). `withLocalShield(local, remote)` composes a per-pod limiter in front. |

The two claim-based stores share an `IdempotencyClaim` contract: the unique `claim` winner runs the handler then overwrites its in-progress sentinel with the real result; everyone else gets the finished result or waits for it. A NUL-wrapped sentinel guarantees it can never collide with an encoded payload.

### SQLite & in-memory

- `SqliteOutboxStore` (`sqlite-outbox.ts`) — durable single-process outbox using the built-in `node:sqlite` module (Node 23+ with `--experimental-sqlite`, or Node 24+ where it is stable). Good for single-pod CLIs and tests; not for multi-replica.
- In-memory stores (`InMemoryEventStore`, `InMemoryScheduledTaskStore`, `LruIdempotencyCache`, the in-memory outbox/inbox/saga/DLQ) — zero-dependency, lost on restart. The default for tests and the [in-memory transport](./testing.md).

### BigInt in stores

Postgres `JSONB` and Redis strings are **text** formats with no native BigInt. The framework's wire codec encodes `bigint` values with a sentinel string (`@@nevo:bigint:<digits>`; the legacy `"<digits>n"` form is still decoded) so they survive a JSON round-trip — see [bigint.md](./bigint.md).

What this means for the stores:

- **Payloads you hand to a store** (outbox `params`, event `payload`, saga `ctx`, DLQ entries, scheduled-task `payload`) are serialized with `JSON.stringify`. A raw `bigint` would throw (`TypeError: Do not know how to serialize a BigInt`). Encode BigInts first with `serializeBigInt(obj)` / `stringifyWithBigInt(obj)` (from `bigint.utils`), or keep numeric ids within `Number.MAX_SAFE_INTEGER`. When the value travels through the normal client→handler path it is already sentinel-encoded for you.
- **Postgres `BIGSERIAL` columns** (`nevo_events.sequence`) come back from the driver as a JS `string` for large values; the stores normalise these to `number` on read. Sequences therefore lose precision above `2^53` — fine for event ordering at realistic volumes, but don't treat the sequence as an arbitrary-precision integer.

## Schema cheat sheet

Every Pg store ships a `migrate()` that creates the table if missing. To run them all at once during deploy:

```ts
import { migrateAllPgStores } from "@riaskov/nevo-messaging"

await migrateAllPgStores(client) // creates nevo_outbox, nevo_inbox, nevo_saga, nevo_events, nevo_dlq
```

Or pass the schema you want them in:

```ts
await migrateAllPgStores(client, "platform_messaging")
```

If you use a separate migration tool (Flyway, Sqitch, Prisma migrations, …), copy the `CREATE TABLE` blocks from the doc comments in `src/common/pg-stores.ts` and own the schema yourself.

## Recommended index strategy

Each store creates the indexes it strictly needs at migrate time. For higher-throughput workloads, consider:

| Table | Extra index | Reason |
|---|---|---|
| `nevo_outbox` | `(claimed_by, claimed_at)` | Recover stale claims faster |
| `nevo_inbox` | `(seen_at)` BRIN | Cheap age-based eviction at scale |
| `nevo_events` | `(type, sequence)` partial | Type-specific projections |
| `nevo_dlq` | `(reason, ts)` | Reason-grouped dashboards |

## Pluggable client adapters

All Postgres stores accept a `PgClient` interface:

```ts
interface PgClient {
  query<T>(text: string, values?: unknown[]): Promise<{ rows: T[]; rowCount?: number }>
}
```

Wrappers for the three common libraries are 4 lines each:

### `pg`

```ts
import { Pool } from "pg"
const pool = new Pool({ connectionString: process.env.DATABASE_URL })
const client: PgClient = { query: (text, values) => pool.query(text, values) }
```

### `postgres`

```ts
import postgres from "postgres"
const sql = postgres(process.env.DATABASE_URL!)
const client: PgClient = {
  async query(text, values) {
    const rows = await sql.unsafe(text, values as any[])
    return { rows, rowCount: rows.length }
  }
}
```

### `pg-promise`

```ts
import pgp from "pg-promise"
const db = pgp()(process.env.DATABASE_URL)
const client: PgClient = {
  async query(text, values) {
    const rows = await db.any(text, values)
    return { rows, rowCount: rows.length }
  }
}
```

The same approach applies to Redis stores (`InboxRedisClient`, `RateLimitRedisClient`, `IdempotencyRedisLike`) — adapt to `ioredis`, `node-redis`, or `upstash/redis` in a handful of lines.

## What ships in 2.3

| Pkg | Stores added |
|---|---|
| `@riaskov/nevo-messaging` | `RedisRateLimiter`, `RedisInboxStore`, `RedisIdempotencyStore` (already in 2.2), `PgOutboxStore`, `PgInboxStore`, `PgSagaStore`, `PgEventStore`, `PgDlqStore`, `PgAuditSink`, `FileAuditSink`, `TeeAuditSink`, plus the in-memory transport for tests |

No new hard dependencies — every database adapter is a 4-line wrapper around your library of choice.

## See also

- [outbox.md](./outbox.md) / [inbox.md](./inbox.md) / [saga.md](./saga.md) — the primitives
- [idempotency.md](./idempotency.md) — distributed idempotency story
- [rate-limiting.md](./rate-limiting.md) — distributed rate limit story
- [testing.md](./testing.md) — `createMemoryTransport()` for unit tests
- [audit-log.md](./audit-log.md) *(when present)* — compliance use cases
