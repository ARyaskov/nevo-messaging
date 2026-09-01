import "reflect-metadata"
import { test } from "node:test"
import assert from "node:assert/strict"
import { PgEventStore, type PgClient } from "../src/common/pg-stores"
import type { DomainEvent } from "../src/common/event-store"

/**
 * A Postgres stand-in that models commit visibility: a row inserted by a still-open
 * transaction is present in the table but its `txid` is not yet below the snapshot
 * xmin, so `committedOnly` reads must not return it — nor step over it.
 */
function fakeVisibilityPg() {
  const rows: any[] = []
  let seq = 0
  let nextTxid = 100
  // Transactions that have been assigned an id but have not finished.
  const openTxids = new Set<number>()
  const statements: string[] = []

  const snapshotXmin = () => {
    // Postgres semantics: xmin is the lowest still-running xid, else the next one.
    let min = nextTxid
    for (const t of openTxids) min = Math.min(min, t)
    return min
  }

  const client: PgClient = {
    async query<T = unknown>(text: string, values: unknown[] = []): Promise<{ rows: T[]; rowCount?: number }> {
      statements.push(text)
      const t = text.toLowerCase()
      if (t.includes("insert into") && t.includes("returning sequence")) {
        const [id, type, aggregate_id, payload, meta] = values as any[]
        seq += 1
        const txid = nextTxid++
        const ts = new Date()
        rows.push({ sequence: seq, id, type, aggregate_id, payload, meta, ts, txid })
        return { rows: [{ sequence: seq, ts }] as T[], rowCount: 1 }
      }
      if (t.includes("select") && t.includes("order by sequence")) {
        const committedOnly = t.includes("pg_snapshot_xmin")
        const from = values.length > 0 ? Number(values[0]) : 0
        const xmin = snapshotXmin()
        const out = rows
          .filter((r) => r.sequence >= from)
          .filter((r) => (committedOnly ? r.txid < xmin : true))
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

  return {
    client,
    statements,
    /** Inserts a row whose transaction stays open until `commit()` is called. */
    async appendUncommitted(type: string) {
      seq += 1
      const txid = nextTxid++
      openTxids.add(txid)
      rows.push({ sequence: seq, id: `u-${seq}`, type, aggregate_id: null, payload: {}, meta: null, ts: new Date(), txid })
      return { sequence: seq, commit: () => openTxids.delete(txid) }
    }
  }
}

test("append no longer serialises writers behind an advisory lock", async () => {
  const pg = fakeVisibilityPg()
  const store = new PgEventStore({ client: pg.client })
  await store.append({ type: "a", payload: {} })

  const insert = pg.statements.find((s) => s.toLowerCase().includes("insert into"))!
  assert.doesNotMatch(insert, /pg_advisory/i, "a global append lock caps the whole store at one writer")
  assert.match(insert, /VALUES/i)
})

test("migrate declares the visibility column and its index", async () => {
  const pg = fakeVisibilityPg()
  await new PgEventStore({ client: pg.client }).migrate()
  const all = pg.statements.join("\n")
  assert.match(all, /txid\s+xid8 NOT NULL DEFAULT pg_current_xact_id\(\)/)
  assert.match(all, /ADD COLUMN IF NOT EXISTS txid xid8/)
  assert.match(all, /nevo_events_txid_idx/)
})

test("committedOnly reads filter on the snapshot xmin", async () => {
  const pg = fakeVisibilityPg()
  const store = new PgEventStore({ client: pg.client })
  await store.append({ type: "a", payload: {} })

  await store.read({ from: 1 })
  const plain = pg.statements[pg.statements.length - 1]
  assert.doesNotMatch(plain, /pg_snapshot_xmin/, "a plain read must see everything the transaction can")

  await store.read({ from: 1, committedOnly: true })
  const guarded = pg.statements[pg.statements.length - 1]
  assert.match(guarded, /txid < pg_snapshot_xmin\(pg_current_snapshot\(\)\)/)
})

test("subscribe does not step over an event that commits later with a lower sequence", async () => {
  const pg = fakeVisibilityPg()
  const store = new PgEventStore({ client: pg.client })

  // seq 1 is inserted by a transaction that stays open…
  const pending = await pg.appendUncommitted("slow")
  assert.equal(pending.sequence, 1)
  // …while seq 2 commits immediately.
  await store.append({ type: "fast", payload: {} })

  const delivered: number[] = []
  const sub = await store.subscribe(1, (e: DomainEvent) => void delivered.push(e.sequence), { pollIntervalMs: 10 })

  await new Promise((res) => setTimeout(res, 80))
  assert.deepEqual(delivered, [], "neither event is deliverable while seq 1 is still in flight")

  pending.commit()
  await new Promise((res) => setTimeout(res, 120))
  await sub.unsubscribe()

  assert.deepEqual(delivered, [1, 2], "both events arrive in sequence order once the slow writer commits")
})

test("a committed tail is still delivered while an older writer is in flight only after it settles", async () => {
  const pg = fakeVisibilityPg()
  const store = new PgEventStore({ client: pg.client })

  await store.append({ type: "first", payload: {} })
  const pending = await pg.appendUncommitted("middle")
  await store.append({ type: "last", payload: {} })

  const delivered: number[] = []
  const sub = await store.subscribe(1, (e: DomainEvent) => void delivered.push(e.sequence), { pollIntervalMs: 10 })

  await new Promise((res) => setTimeout(res, 80))
  assert.deepEqual(delivered, [1], "seq 1 is safe; seq 3 must wait behind the in-flight seq 2")

  pending.commit()
  await new Promise((res) => setTimeout(res, 120))
  await sub.unsubscribe()

  assert.deepEqual(delivered, [1, 2, 3])
})
