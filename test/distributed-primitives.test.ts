import { test } from "node:test"
import assert from "node:assert/strict"
import { RedisRateLimiter, type RateLimitRedisClient } from "../src/common/rate-limit-redis"
import { RedisInboxStore, type InboxRedisClient } from "../src/common/inbox-redis"
import { PgOutboxStore, PgInboxStore, PgSagaStore, PgEventStore, PgDlqStore, type PgClient } from "../src/common/pg-stores"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"

// ---------------------------------------------------------------------------
// Fake Redis: emulates the small subset our stores need.
// ---------------------------------------------------------------------------

function fakeRedisForRateLimit(): RateLimitRedisClient & { state: Map<string, { tokens: number; lastRefill: number }> } {
  const state = new Map<string, { tokens: number; lastRefill: number }>()
  return {
    state,
    async eval({ keys, args }) {
      const key = keys[0]
      const capacity = Number(args[0])
      const refill = Number(args[1])
      const now = Number(args[2])
      let entry = state.get(key)
      if (!entry) entry = { tokens: capacity, lastRefill: now }
      const elapsedMs = now - entry.lastRefill
      if (elapsedMs > 0) {
        entry.tokens = Math.min(capacity, entry.tokens + (elapsedMs / 1000) * refill)
      }
      if (entry.tokens < 1) {
        const retryAfterMs = Math.ceil(((1 - entry.tokens) / refill) * 1000)
        entry.lastRefill = now
        state.set(key, entry)
        return [0, retryAfterMs, entry.tokens]
      }
      entry.tokens -= 1
      entry.lastRefill = now
      state.set(key, entry)
      return [1, 0, entry.tokens]
    }
  }
}

function fakeRedisForInbox(): InboxRedisClient & { storage: Map<string, string> } {
  const storage = new Map<string, string>()
  return {
    storage,
    async get(key) { return storage.get(key) ?? null },
    async set(key, value, opts) {
      if (opts.ifNotExists && storage.has(key)) return null
      storage.set(key, value)
      return "OK"
    },
    async del(key) { return storage.delete(key) ? 1 : 0 },
    async exists(key) { return storage.has(key) ? 1 : 0 }
  } as InboxRedisClient & { storage: Map<string, string> }
}

// ---------------------------------------------------------------------------
// Fake Postgres: minimal SQL-aware in-memory backend, just enough to test.
// ---------------------------------------------------------------------------

function fakePg(): PgClient & { sql: string[]; params: unknown[][]; rows: Map<string, any[]> } {
  const sql: string[] = []
  const params: unknown[][] = []
  const rows = new Map<string, any[]>() // table → rows
  return {
    sql, params, rows,
    async query<T = unknown>(text: string, values: unknown[] = []): Promise<{ rows: T[]; rowCount?: number }> {
      sql.push(text)
      params.push(values)
      // We don't actually execute SQL — the tests only assert that the right
      // queries were issued. Selective stubs return canned rows.
      const t = text.toLowerCase()
      if (t.includes("returning")) return { rows: [{ sequence: 1, ts: new Date() }] as any, rowCount: 1 }
      if (t.includes("select exists")) return { rows: [{ exists: false } as any], rowCount: 1 }
      if (t.includes("select count")) return { rows: [{ count: "0" } as any], rowCount: 1 }
      if (t.startsWith("with cte")) return { rows: [] as T[], rowCount: 0 }
      return { rows: [] as T[], rowCount: 0 }
    }
  }
}

// ---------------------------------------------------------------------------
// RedisRateLimiter
// ---------------------------------------------------------------------------

test("RedisRateLimiter allows up to capacity then throws RATE_LIMITED", async () => {
  const rl = new RedisRateLimiter({
    client: fakeRedisForRateLimit(),
    enabled: true,
    capacity: 3,
    refillPerSec: 0
  })
  await rl.check({ topic: "t", method: "m", callerService: "frontend" })
  await rl.check({ topic: "t", method: "m", callerService: "frontend" })
  await rl.check({ topic: "t", method: "m", callerService: "frontend" })
  await assert.rejects(
    () => rl.check({ topic: "t", method: "m", callerService: "frontend" }),
    (err) => {
      assert.ok(err instanceof MessagingError)
      assert.equal((err as MessagingError).code, ErrorCode.RATE_LIMITED)
      assert.ok(((err as MessagingError).details as any)?.retryAfterMs > 0)
      return true
    }
  )
})

test("RedisRateLimiter different callers get independent buckets", async () => {
  const rl = new RedisRateLimiter({
    client: fakeRedisForRateLimit(),
    enabled: true,
    capacity: 1,
    refillPerSec: 0
  })
  await rl.check({ topic: "t", method: "m", callerService: "a" })
  await rl.check({ topic: "t", method: "m", callerService: "b" })
  await assert.rejects(() => rl.check({ topic: "t", method: "m", callerService: "a" }))
})

test("RedisRateLimiter fails open on Redis errors when failOpen=true", async () => {
  const rl = new RedisRateLimiter({
    client: { async eval() { throw new Error("ECONNREFUSED") } },
    enabled: true,
    capacity: 1,
    refillPerSec: 0,
    failOpen: true
  })
  // Should not throw — the bucket is unavailable, but the request proceeds.
  await rl.check({ topic: "t", method: "m" })
})

test("RedisRateLimiter fails closed when failOpen=false", async () => {
  const rl = new RedisRateLimiter({
    client: { async eval() { throw new Error("ECONNREFUSED") } },
    enabled: true,
    capacity: 1,
    refillPerSec: 0,
    failOpen: false
  })
  await assert.rejects(() => rl.check({ topic: "t", method: "m" }))
})

// ---------------------------------------------------------------------------
// RedisInboxStore
// ---------------------------------------------------------------------------

test("RedisInboxStore markSeen → hasSeen round-trips", async () => {
  const inbox = new RedisInboxStore({ client: fakeRedisForInbox() })
  assert.equal(await inbox.hasSeen("u-1"), false)
  await inbox.markSeen("u-1", { ok: true })
  assert.equal(await inbox.hasSeen("u-1"), true)
  assert.deepEqual(await inbox.getResult("u-1"), { ok: true })
})

test("RedisInboxStore swallows write errors without throwing", async () => {
  const failing: InboxRedisClient = {
    async get() { return null },
    async set() { throw new Error("ECONNREFUSED") }
  }
  const inbox = new RedisInboxStore({ client: failing })
  // markSeen should NOT throw — at-least-once delivery still works; the
  // handler may simply re-run on the next attempt.
  await inbox.markSeen("u-1", { ok: true })
})

// ---------------------------------------------------------------------------
// PgOutboxStore
// ---------------------------------------------------------------------------

test("PgOutboxStore.migrate creates the table + pending index", async () => {
  const pg = fakePg()
  await new PgOutboxStore({ client: pg }).migrate()
  const text = pg.sql.join(" ").toLowerCase()
  assert.match(text, /create table if not exists/)
  assert.match(text, /nevo_outbox/)
  assert.match(text, /nevo_outbox_pending_idx/)
})

test("PgOutboxStore.listPending issues FOR UPDATE SKIP LOCKED", async () => {
  const pg = fakePg()
  const store = new PgOutboxStore({ client: pg })
  await store.listPending(10)
  const text = pg.sql.join(" ").toLowerCase()
  assert.match(text, /for update skip locked/)
})

test("PgOutboxStore.markPublished sets status=published", async () => {
  const pg = fakePg()
  const store = new PgOutboxStore({ client: pg })
  await store.markPublished("id-1")
  const text = pg.sql.join(" ").toLowerCase()
  assert.match(text, /set status = 'published'/)
})

// ---------------------------------------------------------------------------
// PgInboxStore
// ---------------------------------------------------------------------------

test("PgInboxStore.markSeen uses ON CONFLICT DO NOTHING", async () => {
  const pg = fakePg()
  const inbox = new PgInboxStore({ client: pg })
  await inbox.markSeen("u-1", { ok: true })
  const text = pg.sql.join(" ").toLowerCase()
  assert.match(text, /on conflict \(uuid\) do nothing/)
})

test("PgInboxStore.prune deletes by ttl interval", async () => {
  const pg = fakePg()
  const inbox = new PgInboxStore({ client: pg })
  await inbox.prune()
  const text = pg.sql.join(" ").toLowerCase()
  assert.match(text, /delete from .*nevo_inbox/)
  assert.match(text, /milliseconds/)
})

// ---------------------------------------------------------------------------
// PgSagaStore
// ---------------------------------------------------------------------------

test("PgSagaStore.save upserts saga_id", async () => {
  const pg = fakePg()
  const store = new PgSagaStore({ client: pg })
  await store.save({
    sagaId: "s-1", steps: ["a", "b"], executed: ["a"], ctx: { foo: 1 },
    status: "pending", updatedAt: Date.now()
  })
  const text = pg.sql.join(" ").toLowerCase()
  assert.match(text, /on conflict \(saga_id\) do update/)
})

test("PgSagaStore.listPending only returns pending+compensating", async () => {
  const pg = fakePg()
  const store = new PgSagaStore({ client: pg })
  await store.listPending()
  const text = pg.sql.join(" ").toLowerCase()
  assert.match(text, /status in \('pending', 'compensating'\)/)
})

// ---------------------------------------------------------------------------
// PgEventStore
// ---------------------------------------------------------------------------

test("PgEventStore.append uses RETURNING sequence, ts", async () => {
  const pg = fakePg()
  const store = new PgEventStore({ client: pg })
  const ev = await store.append({ type: "user.created", aggregateId: "u-1", payload: { id: 1 } })
  assert.equal(ev.type, "user.created")
  assert.ok(ev.id.length > 0)
  const text = pg.sql.join(" ").toLowerCase()
  assert.match(text, /returning sequence, ts/)
})

test("PgEventStore.read builds dynamic WHERE clauses", async () => {
  const pg = fakePg()
  const store = new PgEventStore({ client: pg })
  await store.read({ from: 10, to: 100, type: "x", aggregateId: "a-1", limit: 50 })
  const text = pg.sql.join(" ").toLowerCase()
  assert.match(text, /sequence >=/)
  assert.match(text, /sequence <=/)
  assert.match(text, /type =/)
  assert.match(text, /aggregate_id =/)
  assert.match(text, /limit/)
})

// ---------------------------------------------------------------------------
// PgDlqStore
// ---------------------------------------------------------------------------

test("PgDlqStore.push stores entry as JSONB", async () => {
  const pg = fakePg()
  const store = new PgDlqStore({ client: pg })
  await store.push({
    topic: "user-events",
    reason: "handler-error",
    error: { code: 500, message: "boom" },
    meta: { method: "user.delete" } as any,
    ts: Date.now()
  })
  const text = pg.sql.join(" ").toLowerCase()
  assert.match(text, /insert into .*nevo_dlq/)
  assert.match(text, /\$7::jsonb/)
})

test("PgDlqStore.stats aggregates by reason/method/code", async () => {
  const pg = fakePg()
  const store = new PgDlqStore({ client: pg })
  await store.stats()
  const text = pg.sql.join(" ").toLowerCase()
  assert.match(text, /group by reason/)
  assert.match(text, /group by error_code/)
  assert.match(text, /group by method/)
})
