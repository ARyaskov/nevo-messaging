import { test } from "node:test"
import assert from "node:assert/strict"
import { Inbox, InMemoryInboxStore } from "../src/common/inbox"
import { RedisInboxStore, type InboxRedisClient } from "../src/common/inbox-redis"
import { PgInboxStore, type PgClient } from "../src/common/pg-stores"
import type { InboxStore } from "../src/common/inbox"
import { MessagingError } from "../src/common/errors"
import { ErrorCode, isRetryable } from "../src/common/error-code"

function fakeRedis(): InboxRedisClient & { storage: Map<string, string> } {
  const storage = new Map<string, string>()
  return {
    storage,
    async get(key) {
      return storage.get(key) ?? null
    },
    async set(key, value, opts) {
      if (opts.ifNotExists && storage.has(key)) return null
      storage.set(key, value)
      return "OK"
    },
    async del(key) {
      return storage.delete(key) ? 1 : 0
    },
    async exists(key) {
      return storage.has(key) ? 1 : 0
    }
  } as InboxRedisClient & { storage: Map<string, string> }
}

test("Inbox.dedupe runs the handler exactly once under same-uuid concurrency", async () => {
  let calls = 0
  let release!: () => void
  const gate = new Promise<void>((r) => {
    release = r
  })
  const inbox = new Inbox({ store: new InMemoryInboxStore() })
  const run = () =>
    inbox.dedupe("evt-1", async () => {
      calls++
      await gate
      return { v: calls }
    })

  const p1 = run()
  const p2 = run()
  await new Promise((r) => setTimeout(r, 20))
  release()
  const [r1, r2] = await Promise.all([p1, p2])

  assert.equal(calls, 1, "in-process leader election must dedup concurrent same-uuid calls")
  assert.deepEqual(r1, r2)
  assert.deepEqual(r1, { v: 1 })
})

test("Inbox.dedupe with RedisInboxStore.claim dedups across two inbox instances (replicas)", async () => {
  let calls = 0
  const shared = fakeRedis()
  const inboxA = new Inbox({ store: new RedisInboxStore({ client: shared }) })
  const inboxB = new Inbox({ store: new RedisInboxStore({ client: shared }) })

  const r1 = await inboxA.dedupe("evt-2", async () => {
    calls++
    return { by: "A" }
  })
  const r2 = await inboxB.dedupe("evt-2", async () => {
    calls++
    return { by: "B" }
  })

  assert.equal(calls, 1, "only one replica runs the handler")
  assert.deepEqual(r1, { by: "A" })
  assert.deepEqual(r2, { by: "A" }, "replica B returns replica A's stored result")
})

test("RedisInboxStore.claim acquires once; markSeen overwrites the sentinel", async () => {
  const store = new RedisInboxStore({ client: fakeRedis() })
  const c1 = await store.claim("u1")
  assert.equal(c1.acquired, true)
  const c2 = await store.claim("u1")
  assert.equal(c2.acquired, false)
  assert.equal(c2.existing, undefined) // sentinel only, no result yet
  await store.markSeen("u1", { ok: true })
  assert.equal(await store.hasSeen("u1"), true)
  assert.deepEqual(await store.getResult("u1"), { ok: true })
  const c3 = await store.claim("u1")
  assert.equal(c3.acquired, false)
  assert.deepEqual(c3.existing, { ok: true })
})

test("RedisInboxStore.hasSeen: fail-open returns false, fail-closed returns true on read error", async () => {
  const boom: InboxRedisClient = {
    async get() {
      throw new Error("down")
    },
    async set() {
      return "OK"
    },
    async exists() {
      throw new Error("down")
    }
  }
  const open = new RedisInboxStore({ client: boom, readErrorPolicy: "open" })
  assert.equal(await open.hasSeen("u1"), false)
  const closed = new RedisInboxStore({ client: boom, readErrorPolicy: "closed" })
  assert.equal(await closed.hasSeen("u1"), true)
})

test("RedisInboxStore.claim: fail-closed rethrows when the store is down", async () => {
  const boom: InboxRedisClient = {
    async get() {
      return null
    },
    async set() {
      throw new Error("down")
    }
  }
  const closed = new RedisInboxStore({ client: boom, readErrorPolicy: "closed" })
  await assert.rejects(() => closed.claim("u1"))
})

test("dedupe's commit wrapper threads the caller's transaction into markSeen", async () => {
  const seen: unknown[] = []
  const store = {
    hasSeen: async () => false,
    markSeen: async (_uuid: string, _result: unknown, opts?: { tx?: unknown }) => {
      seen.push(opts?.tx)
    },
    getResult: async () => undefined
  }
  const inbox = new Inbox({ store: store as InboxStore })
  const txClient = { id: "txn-1" }
  await inbox.dedupe("evt-tx", async () => "ok", {
    // The user's commit wrapper receives the tx handle and forwards it, so the
    // store writes the dedup record on the business-transaction connection.
    tx: async (commit) => commit(txClient)
  })
  assert.equal(seen.length, 1)
  assert.deepEqual(seen[0], txClient)
})

test("PgInboxStore.markSeen writes through the supplied transaction client", async () => {
  const queries: { client: PgClient | undefined; sql: string }[] = []
  const tx: PgClient = { query: (sql: string) => (queries.push({ client: tx, sql }), Promise.resolve({ rows: [] })) }
  const store = new PgInboxStore({
    client: { query: (sql: string) => (queries.push({ client: undefined, sql }), Promise.resolve({ rows: [] })) }
  })
  await store.markSeen("u-tx", { ok: true }, { tx })
  assert.equal(queries.length, 1)
  assert.equal(queries[0].client, tx, "the record lands on the caller's transaction connection")
  assert.match(queries[0].sql, /INSERT INTO/)
})

test("a lost claim with no surfaced result is re-claimed, not raced (retryable, no double-execute)", async () => {
  let claims = 0
  // A "live peer" holds the claim for the whole window: the loser must never
  // fall through to the handler (the old behaviour).
  const heldStore = {
    hasSeen: async () => false,
    markSeen: async () => {},
    getResult: async () => undefined,
    isDone: async () => false,
    claim: async () => {
      claims++
      return { acquired: false }
    }
  }
  const inbox = new Inbox({ store: heldStore as InboxStore, awaitTimeoutMs: 60 })
  let runs = 0
  // A live peer holding the claim is "not ready yet": the loser must get a
  // retryable error, never a fake empty success (which would lose the
  // delivery under a false-success ACK).
  await assert.rejects(
    () =>
      inbox.dedupe("live-peer", async () => {
        runs++
        return "ran"
      }),
    (err: unknown) => err instanceof MessagingError && err.code === ErrorCode.SERVICE_UNAVAILABLE && err.retryable === true && isRetryable(err.code)
  )
  assert.equal(runs, 0, "the handler never runs while the peer holds the claim")
  assert.equal(claims, 2, "the loser awaits once, then re-claims once before reporting not-ready")
})

test("an expired claim is re-claimed successfully and the handler runs exactly once", async () => {
  let claims = 0
  // First claim is held (crashed peer); by the re-claim the TTL has expired.
  const store = {
    hasSeen: async () => false,
    markSeen: async () => {},
    getResult: async () => undefined,
    isDone: async () => false,
    claim: async () => {
      return ++claims === 1 ? { acquired: false } : { acquired: true }
    }
  }
  const inbox = new Inbox({ store: store as InboxStore, awaitTimeoutMs: 60 })
  let runs = 0
  const r = await inbox.dedupe("crashed-peer", async () => {
    runs++
    return "ran"
  })
  assert.equal(r, "ran")
  assert.equal(runs, 1, "the re-claim winner runs the handler exactly once")
})

test("a follower settles only after the leader's commit wrapper resolves", async () => {
  let release!: () => void
  const gate = new Promise<void>((r) => {
    release = r
  })
  const events: string[] = []
  const store = {
    hasSeen: async () => false,
    markSeen: async () => {
      events.push("markSeen")
    },
    getResult: async () => undefined
  }
  const inbox = new Inbox({ store: store as InboxStore })
  const leader = inbox.dedupe(
    "tx-order",
    async () => "ok",
    // The wrapper resolves only after the external COMMIT; the local lease must
    // not settle (waking followers) before that.
    {
      tx: async (commit) => {
        await commit()
        await gate
        events.push("commit")
      }
    }
  )
  const follower = inbox.dedupe("tx-order", async () => {
    events.push("follower-ran")
    return "bad"
  })
  await new Promise((r) => setTimeout(r, 20))
  release()
  await Promise.all([leader, follower])
  assert.equal(events.indexOf("follower-ran"), -1, "followers must not re-run the handler")
  assert.ok(events.indexOf("markSeen") < events.indexOf("commit"), "markSeen precedes the commit wrapper's resolution")
})
