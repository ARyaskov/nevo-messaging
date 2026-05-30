import { test } from "node:test"
import assert from "node:assert/strict"
import { InMemoryOutboxStore, Outbox, withOutboxTransaction, type OutboxRecord } from "../src/common/outbox"
import { PgOutboxStore, type PgClient } from "../src/common/pg-stores"
import { SqliteOutboxStore } from "../src/common/sqlite-outbox"

test("outbox publishes pending records", async () => {
  const store = new InMemoryOutboxStore()
  const calls: string[] = []
  const ob = new Outbox(store, { emit: async (svc, m) => { calls.push(`${svc}:${m}`) } }, { batch: 10, maxAttempts: 3 })
  await ob.enqueue("user", "user.created", { id: 1 })
  await ob.enqueue("user", "user.deleted", { id: 2 })
  const { published, failed } = await ob.flushOnce()
  assert.equal(published, 2)
  assert.equal(failed, 0)
  assert.deepEqual(calls, ["user:user.created", "user:user.deleted"])
})

test("outbox records failed on persistent error", async () => {
  const store = new InMemoryOutboxStore()
  const ob = new Outbox(store, { emit: async () => { throw new Error("nope") } }, { batch: 1, maxAttempts: 1 })
  await ob.enqueue("user", "x", {})
  const { failed } = await ob.flushOnce()
  assert.equal(failed, 1)
})

function record(id: string, partitionKey?: string): OutboxRecord {
  return { id, serviceName: "orders", method: "order.placed", params: { id }, createdAt: 1, attempts: 0, status: "pending", partitionKey }
}

test("withOutboxTransaction commits the outbox row with the business tx", async () => {
  const store = new InMemoryOutboxStore()
  await withOutboxTransaction(store, async (tx) => {
    await store.save(record("evt-1"), tx)
  })
  const pending = await store.listPending(10)
  assert.equal(pending.length, 1)
  assert.equal(pending[0].id, "evt-1")
})

test("withOutboxTransaction rolls back the outbox row when the business tx fails", async () => {
  const store = new InMemoryOutboxStore()
  await assert.rejects(
    withOutboxTransaction(store, async (tx) => {
      // Stage the outbox row first, then let the surrounding business write fail.
      await store.save(record("evt-1"), tx)
      throw new Error("business write failed")
    }),
    /business write failed/
  )
  // The outbox row must NOT survive a rolled-back business transaction.
  assert.deepEqual(await store.listPending(10), [])
})

test("enqueue inside withOutboxTransaction rolls back on business failure", async () => {
  const store = new InMemoryOutboxStore()
  const ob = new Outbox(store, { emit: async () => {} })
  await assert.rejects(
    withOutboxTransaction(store, async (tx) => {
      await ob.enqueue("orders", "order.placed", { id: 1 }, { tx })
      throw new Error("rollback please")
    })
  )
  assert.deepEqual(await store.listPending(10), [])
})

test("withOutboxTransaction rolls back the outbox row with business state (sqlite, shared db)", async () => {
  const { DatabaseSync } = await import("node:sqlite")
  const db = new DatabaseSync(":memory:")
  db.exec("CREATE TABLE orders (id TEXT PRIMARY KEY)")
  const store = new SqliteOutboxStore({ db })
  const orderCount = () => (db.prepare("SELECT COUNT(*) AS n FROM orders").get() as { n: number }).n

  // Business INSERT + outbox save in one tx, then fail: BOTH must roll back.
  await assert.rejects(
    withOutboxTransaction(db, async (tx: any) => {
      tx.prepare("INSERT INTO orders (id) VALUES (?)").run("o-1")
      await store.save(record("evt-1"), tx)
      throw new Error("business failed")
    }),
    /business failed/
  )
  assert.equal(orderCount(), 0)
  assert.deepEqual(await store.listPending(10), [])

  // The happy path commits both atomically.
  await withOutboxTransaction(db, async (tx: any) => {
    tx.prepare("INSERT INTO orders (id) VALUES (?)").run("o-2")
    await store.save(record("evt-2"), tx)
  })
  assert.equal(orderCount(), 1)
  assert.equal((await store.listPending(10)).length, 1)

  store.close()   // borrowed connection -> no-op
  db.close()
})

test("withOutboxTransaction throws on a handle it cannot drive", async () => {
  await assert.rejects(
    withOutboxTransaction({} as unknown, async () => {}),
    /must expose query\(sql\).*exec\(sql\).*beginTx\(\)/s
  )
})

const silentLogger: any = { trace() {}, debug() {}, info() {}, warn() {}, error() {}, fatal() {}, child() { return silentLogger } }

/**
 * A stateful in-memory stand-in for a Postgres connection that executes the
 * small, fixed set of statements PgOutboxStore issues — enough to exercise the
 * real ownership guard. With `claimTtlMs: 0` every claim is immediately
 * re-claimable, which reproduces the "slow worker's claim TTL expired" race
 * deterministically and without a real database.
 */
function statefulFakePg(): PgClient & { rows: any[] } {
  const rows: any[] = []
  const find = (id: string) => rows.find((r) => r.id === id)
  return {
    rows,
    async query<T = unknown>(text: string, values: unknown[] = []): Promise<{ rows: T[]; rowCount?: number }> {
      const t = text.toLowerCase()
      if (t.includes("skip locked")) {
        const limit = Number(values[0])
        const ttlMs = Number(values[1])
        const workerId = String(values[2])
        const claimable = rows.filter((r) => r.status === "pending" && (r.claimed_by == null || ttlMs === 0)).slice(0, limit)
        for (const r of claimable) r.claimed_by = workerId
        return {
          rows: claimable.map((r) => ({
            id: r.id, service_name: r.service_name, method: r.method, params: r.params,
            partition_key: r.partition_key ?? null, attempts: r.attempts, status: r.status,
            last_error: r.last_error ?? null, created_at: new Date(r.created_at)
          })) as T[],
          rowCount: claimable.length
        }
      }
      if (t.includes("insert into") && t.includes("on conflict")) {
        const [id, service_name, method, params, partition_key, status, attempts, created_at] = values as any[]
        if (!find(String(id))) rows.push({ id, service_name, method, params, partition_key, status, attempts, created_at, claimed_by: null, last_error: null })
        return { rows: [] as T[], rowCount: 1 }
      }
      if (t.includes("'published'")) {
        const [id, workerId] = values as any[]
        const r = find(String(id))
        if (r && r.status === "pending" && r.claimed_by === workerId) {
          r.status = "published"
          return { rows: [{ status: r.status, attempts: r.attempts }] as T[], rowCount: 1 }
        }
        return { rows: [] as T[], rowCount: 0 }
      }
      if (t.includes("last_error =")) {
        const [id, error, maxAttempts, workerId] = values as any[]
        const r = find(String(id))
        if (r && r.status === "pending" && r.claimed_by === workerId) {
          r.attempts += 1
          r.last_error = error
          r.status = r.attempts >= Number(maxAttempts) ? "failed" : "pending"
          r.claimed_by = null
          return { rows: [{ status: r.status, attempts: r.attempts }] as T[], rowCount: 1 }
        }
        return { rows: [] as T[], rowCount: 0 }
      }
      return { rows: [] as T[], rowCount: 0 }
    }
  }
}

test("stolen claim does not double-publish (pg ownership guard)", async () => {
  const pg = statefulFakePg()
  // Two workers share one table; claimTtlMs:0 makes a claim instantly stealable.
  const slow = new PgOutboxStore({ client: pg, claimTtlMs: 0, logger: silentLogger })
  const fast = new PgOutboxStore({ client: pg, claimTtlMs: 0, logger: silentLogger })
  await slow.save(record("evt-1"))

  // `slow` claims the row, then its TTL lapses and `fast` re-claims the same row.
  assert.equal((await slow.listPending(10)).length, 1)
  assert.equal((await fast.listPending(10)).length, 1)

  // `fast` publishes and legitimately owns the finalization.
  const markFast = await fast.markPublished("evt-1")
  assert.equal(markFast.owned, true)
  assert.equal(markFast.status, "published")

  // `slow` finishes late: its claim was stolen, so it must NOT re-publish.
  const markSlow = await slow.markPublished("evt-1")
  assert.equal(markSlow.owned, false)

  // The row was published exactly once.
  assert.equal(pg.rows.filter((r) => r.status === "published").length, 1)
})

test("markFailed on a stolen claim is ignored (pg ownership guard)", async () => {
  const pg = statefulFakePg()
  const slow = new PgOutboxStore({ client: pg, claimTtlMs: 0, logger: silentLogger })
  const fast = new PgOutboxStore({ client: pg, claimTtlMs: 0, logger: silentLogger })
  await slow.save(record("evt-1"))
  await slow.listPending(10)
  await fast.listPending(10)
  await fast.markPublished("evt-1")

  const markSlow = await slow.markFailed("evt-1", "boom", 5)
  assert.equal(markSlow.owned, false)
  // The row stays published; the stolen-claim failure did not flip it back to pending.
  assert.equal(pg.rows.find((r) => r.id === "evt-1").status, "published")
})

test("configured maxAttempts parks a poison row after exactly N attempts (sqlite)", async () => {
  const store = new SqliteOutboxStore()
  const ob = new Outbox(store, { emit: async () => { throw new Error("broker down") } }, { batch: 10, maxAttempts: 3 })
  await ob.enqueue("user", "user.created", { id: 1 })

  // Attempts 1 and 2 leave the row pending (no park yet).
  assert.equal((await ob.flushOnce()).failed, 0)
  assert.equal((await store.listPending(10)).length, 1)
  assert.equal((await ob.flushOnce()).failed, 0)
  assert.equal((await store.listPending(10)).length, 1)

  // Attempt 3 reaches maxAttempts and parks the row as failed.
  assert.equal((await ob.flushOnce()).failed, 1)
  assert.equal((await store.listPending(10)).length, 0)

  // A parked row is not retried, so further flushes are no-ops.
  const after = await ob.flushOnce()
  assert.equal(after.published + after.failed, 0)
  store.close()
})

test("maxAttempts threading works through the in-memory store", async () => {
  const store = new InMemoryOutboxStore()
  const ob = new Outbox(store, { emit: async () => { throw new Error("down") } }, { batch: 10, maxAttempts: 2 })
  await ob.enqueue("user", "x", {})
  assert.equal((await ob.flushOnce()).failed, 0)   // attempt 1 -> pending
  assert.equal((await ob.flushOnce()).failed, 1)   // attempt 2 -> failed
  assert.equal((await store.listPending(10)).length, 0)
})
