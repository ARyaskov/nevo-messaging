import { test } from "node:test"
import assert from "node:assert/strict"
import { InMemoryEventStore, type DomainEvent } from "../src/common/event-store"
import { PgEventStore, type PgClient } from "../src/common/pg-stores"
import { SqliteOutboxStore } from "../src/common/sqlite-outbox"
import type { OutboxRecord } from "../src/common/outbox"

const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

// ---------------------------------------------------------------------------
// Fix 1: PgEventStore.subscribe must NOT advance the cursor past an event whose
// handler threw — it has to retry the SAME event next tick (at-least-once).
// ---------------------------------------------------------------------------

/**
 * Minimal stateful stand-in for a Postgres connection that backs the small,
 * fixed set of statements PgEventStore issues: the append INSERT ... RETURNING
 * and the read SELECT ... WHERE sequence >= $n ORDER BY sequence ASC. Enough to
 * exercise the real poll loop without a database.
 */
function fakeEventPg(): PgClient & { rows: any[] } {
  const rows: any[] = []
  let seq = 0
  return {
    rows,
    async query<T = unknown>(text: string, values: unknown[] = []): Promise<{ rows: T[]; rowCount?: number }> {
      const t = text.toLowerCase()
      if (t.includes("insert into") && t.includes("returning sequence")) {
        const [id, type, aggregate_id, payload, meta] = values as any[]
        seq += 1
        const ts = new Date()
        rows.push({ sequence: seq, id, type, aggregate_id, payload, meta, ts })
        return { rows: [{ sequence: seq, ts }] as T[], rowCount: 1 }
      }
      if (t.includes("select") && t.includes("from") && t.includes("order by sequence")) {
        // The only filter the subscribe loop uses is `sequence >= $1`.
        const from = values.length > 0 ? Number(values[0]) : 0
        const out = rows
          .filter((r) => r.sequence >= from)
          .sort((a, b) => a.sequence - b.sequence)
          .map((r) => ({
            sequence: r.sequence,
            id: r.id,
            type: r.type,
            aggregate_id: r.aggregate_id,
            payload: r.payload,
            meta: r.meta,
            ts: r.ts
          }))
        return { rows: out as T[], rowCount: out.length }
      }
      return { rows: [] as T[], rowCount: 0 }
    }
  }
}

test("pg event-store: a failing handler is retried and the cursor does not advance past it", async () => {
  const pg = fakeEventPg()
  const store = new PgEventStore({ client: pg })
  await store.append({ type: "a", payload: { n: 1 } })
  await store.append({ type: "b", payload: { n: 2 } })
  await store.append({ type: "c", payload: { n: 3 } })

  const delivered: number[] = []
  let failuresLeft = 2 // fail the FIRST event twice, then let it through
  const sub = await store.subscribe(
    1,
    (e: DomainEvent) => {
      if (e.sequence === 1 && failuresLeft > 0) {
        failuresLeft--
        throw new Error("handler boom")
      }
      delivered.push(e.sequence)
    },
    { pollIntervalMs: 10 }
  )

  // Give the poll loop several ticks (interval is clamped to >= 50ms).
  await delay(400)
  await sub.unsubscribe()

  // Event 1 must be delivered exactly once it finally succeeds, and the later
  // events must NOT have been delivered ahead of it (no skipping past a failure).
  assert.deepEqual(delivered, [1, 2, 3])
  // Sanity: event 1 was attempted more than once before succeeding.
  assert.equal(failuresLeft, 0)
})

test("pg event-store: a permanently failing handler blocks the cursor (no data loss)", async () => {
  const pg = fakeEventPg()
  const store = new PgEventStore({ client: pg })
  await store.append({ type: "a", payload: { n: 1 } })
  await store.append({ type: "b", payload: { n: 2 } })

  const seen: number[] = []
  const sub = await store.subscribe(
    1,
    (e: DomainEvent) => {
      seen.push(e.sequence)
      if (e.sequence === 1) throw new Error("always fails")
    },
    { pollIntervalMs: 10 }
  )
  await delay(300)
  await sub.unsubscribe()

  // Every delivery attempt must be for event 1 — the loop never advances past
  // the poison event to event 2, so seq 2 is never observed.
  assert.ok(seen.length >= 2, "expected event 1 to be retried at least once")
  assert.ok(
    seen.every((s) => s === 1),
    `expected only seq 1 retries, saw ${seen.join(",")}`
  )
})

// ---------------------------------------------------------------------------
// Fix 2: a bigint payload must round-trip through the sqlite outbox. JSON.stringify
// throws on bigint; the BigInt-safe codec persists and restores it.
// ---------------------------------------------------------------------------

test("sqlite outbox: a bigint payload round-trips through save/listPending", async () => {
  const store = new SqliteOutboxStore()
  const big = 9007199254740993n // > Number.MAX_SAFE_INTEGER
  const record: OutboxRecord = {
    id: "evt-big",
    serviceName: "ledger",
    method: "amount.posted",
    params: { amount: big, nested: { ids: [1n, 2n] }, note: "ok" },
    createdAt: Date.now(),
    attempts: 0,
    status: "pending"
  }

  // The bug: this save would THROW ("Do not know how to serialize a BigInt")
  // and fail the surrounding business transaction. It must now succeed.
  await store.save(record)

  const pending = await store.listPending(10)
  assert.equal(pending.length, 1)
  const params = pending[0].params as { amount: bigint; nested: { ids: bigint[] }; note: string }
  assert.equal(typeof params.amount, "bigint")
  assert.equal(params.amount, big)
  assert.deepEqual(params.nested.ids, [1n, 2n])
  assert.equal(params.note, "ok")
  store.close()
})

test("sqlite outbox: a failed ordered predecessor blocks newer partition records", async () => {
  const store = new SqliteOutboxStore()
  await store.save({
    id: "ordered-1",
    serviceName: "ledger",
    method: "entry.post",
    params: { n: 1 },
    partitionKey: "account-7",
    createdAt: 1,
    attempts: 0,
    status: "pending"
  })
  await store.save({
    id: "ordered-2",
    serviceName: "ledger",
    method: "entry.post",
    params: { n: 2 },
    partitionKey: "account-7",
    createdAt: 2,
    attempts: 0,
    status: "pending"
  })
  await store.markFailed("ordered-1", "poison", 1)

  assert.deepEqual(await store.listPending(10), [])
  store.close()
})

// ---------------------------------------------------------------------------
// Fix 3: InMemoryEventStore.append must persist synchronously but must NOT block
// on a slow subscriber.
// ---------------------------------------------------------------------------

test("in-memory event-store: a slow subscriber does not block append, yet the event is durable immediately", async () => {
  const store = new InMemoryEventStore()

  let handlerStarted = false
  let releaseHandler!: () => void
  const handlerGate = new Promise<void>((resolve) => {
    releaseHandler = resolve
  })

  await store.subscribe(0, async () => {
    handlerStarted = true
    await handlerGate // a deliberately slow subscriber that never resolves on its own
  })

  const start = Date.now()
  const event = await store.append({ type: "x", payload: { v: 1 } })
  const elapsed = Date.now() - start

  // append resolved promptly even though the subscriber is wedged forever on the
  // gate — proof it did not await handler completion.
  assert.ok(elapsed < 100, `append took ${elapsed}ms; it should not block on the subscriber`)

  // The event is durable the instant append() returns (workflow relies on this).
  const events = await store.read({})
  assert.equal(events.length, 1)
  assert.equal(events[0].id, event.id)

  // Delivery still happens — the handler fires on a later turn, not inline.
  await delay(10)
  assert.equal(handlerStarted, true)

  releaseHandler()
})

test("in-memory event-store: a handler that reentrantly appends does not corrupt delivery", async () => {
  const store = new InMemoryEventStore()
  const seen: string[] = []
  let reentered = false

  await store.subscribe(0, async (e) => {
    seen.push(e.type)
    // Reentrant append from within a handler must not throw or cause an
    // unbounded replay loop.
    if (e.type === "first" && !reentered) {
      reentered = true
      await store.append({ type: "second", payload: {} })
    }
  })

  await store.append({ type: "first", payload: {} })
  await delay(20)

  assert.equal(store.size(), 2)
  assert.ok(seen.includes("first"))
  assert.ok(seen.includes("second"))
  // No runaway duplication.
  assert.equal(seen.filter((t) => t === "first").length, 1)
  assert.equal(seen.filter((t) => t === "second").length, 1)
})
