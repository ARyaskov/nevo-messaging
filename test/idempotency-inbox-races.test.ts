import { test } from "node:test"
import assert from "node:assert/strict"
import { setTimeout as sleep } from "node:timers/promises"
import { LruIdempotencyCache } from "../src/common/idempotency"
import { RedisIdempotencyStore, type IdempotencyRedisLike } from "../src/common/idempotency-store"
import { TwoTierIdempotency } from "../src/common/idempotency-runtime"
import { Inbox } from "../src/common/inbox"
import { RedisInboxStore, type InboxRedisClient } from "../src/common/inbox-redis"

// ---------------------------------------------------------------------------
// Shared fakes
// ---------------------------------------------------------------------------

/**
 * In-memory Redis-ish backend with real `SET NX` semantics plus a gauge of how
 * many `claim`/`set` operations are in flight AT ONCE — so a test can prove the
 * leader-failure path promotes a single new leader instead of stampeding every
 * waiter into a concurrent distributed claim.
 */
function gaugedRedis() {
  const storage = new Map<string, string>()
  let inFlight = 0
  let maxInFlight = 0
  const enter = () => {
    inFlight++
    if (inFlight > maxInFlight) maxInFlight = inFlight
  }
  const leave = () => {
    inFlight--
  }
  const client: IdempotencyRedisLike & InboxRedisClient = {
    async get(key: string) {
      return storage.get(key) ?? null
    },
    async set(key: string, value: string, opts: { ttlMs: number; ifNotExists?: boolean }) {
      enter()
      try {
        await sleep(5) // widen the window so a herd would actually overlap
        if (opts.ifNotExists && storage.has(key)) return null
        storage.set(key, value)
        return "OK"
      } finally {
        leave()
      }
    },
    async del(key: string) {
      return storage.delete(key) ? 1 : 0
    },
    async exists(key: string) {
      return storage.has(key) ? 1 : 0
    }
  }
  return {
    client,
    storage,
    get maxInFlight() {
      return maxInFlight
    }
  }
}

// ===========================================================================
// Fix 1 — TwoTierIdempotency: a failing leader must not strand waiters or fire
// a herd of concurrent distributed claims.
// ===========================================================================

test("TwoTier: failing leader promotes ONE new leader, no claim herd, no strand", async () => {
  const redis = gaugedRedis()
  const distributed = new RedisIdempotencyStore<{ n: number }>({
    client: redis.client,
    enabled: true,
    ttlMs: 60_000
  })
  const idem = new TwoTierIdempotency<{ n: number }>({ distributed, awaitTimeoutMs: 1_000 })

  let executes = 0
  let failNext = true
  // Realistic begin→(execute? run+commit : hit) round-trip. The first `execute`
  // simulates a handler failure (release); later ones succeed (commit).
  const runOnce = async (): Promise<{ n: number }> => {
    const b = await idem.begin("k")
    if (b.status === "hit") return b.value
    executes++
    if (failNext) {
      failNext = false
      await idem.release("k", new Error("leader boom"))
      // Re-run the whole flow: a fresh begin re-elects a leader / awaits the winner.
      return runOnce()
    }
    const value = { n: executes }
    await idem.commit("k", value)
    return value
  }

  // Four concurrent callers for the same cold key. One leads, fails, releases;
  // the rest must NOT all stampede the distributed claim.
  const results = await Promise.all([runOnce(), runOnce(), runOnce(), runOnce()])

  // No herd: distributed SET/claim never overlapped.
  assert.equal(redis.maxInFlight, 1, "distributed claims must not stampede after a leader failure")
  // No strand: all four settled to the same committed value.
  for (const r of results) assert.deepEqual(r, { n: 2 })
  // The handler ran exactly twice: the failed leader + the one promoted leader.
  assert.equal(executes, 2, "exactly one failed + one successful execution; no herd of re-executes")

  // A later caller sees the committed value (pure hit).
  assert.deepEqual(await idem.begin("k"), { status: "hit", value: { n: 2 } })
})

test("TwoTier (no distributed store): failing leader lets exactly one waiter re-run", async () => {
  const idem = new TwoTierIdempotency<number>({ l1Options: { enabled: true, ttlMs: 60_000 } })

  let executes = 0
  let failNext = true
  const runOnce = async (): Promise<number> => {
    const b = await idem.begin("local")
    if (b.status === "hit") return b.value
    executes++
    if (failNext) {
      failNext = false
      await idem.release("local", new Error("boom"))
      return runOnce()
    }
    await idem.commit("local", executes)
    return executes
  }

  const results = await Promise.all([runOnce(), runOnce(), runOnce()])
  for (const r of results) assert.equal(r, 2)
  assert.equal(executes, 2, "one failed leader + exactly one promoted re-run, the rest are hits")
  assert.deepEqual(await idem.begin("local"), { status: "hit", value: 2 })
})

test("TwoTier: concurrent first-callers elect a single leader (no herd on the happy path)", async () => {
  const redis = gaugedRedis()
  const distributed = new RedisIdempotencyStore<number>({ client: redis.client, enabled: true, ttlMs: 60_000 })
  const idem = new TwoTierIdempotency<number>({ distributed, awaitTimeoutMs: 1_000 })

  let executes = 0
  const runOnce = async (): Promise<number> => {
    const b = await idem.begin("cold")
    if (b.status === "hit") return b.value
    executes++
    await idem.commit("cold", executes)
    return executes
  }

  // Fire many begins at once for a cold key — exactly one should execute.
  const results = await Promise.all(Array.from({ length: 6 }, () => runOnce()))
  assert.equal(executes, 1, "only one concurrent caller leads a cold key")
  for (const r of results) assert.equal(r, 1)
  assert.equal(redis.maxInFlight, 1, "no concurrent claim herd on the happy path")
})

// ===========================================================================
// Fix 2 — RedisInboxStore: a void/null-returning handler must dedupe (the
// loser must see it as seen and NOT re-run).
// ===========================================================================

function fakeInboxRedis(): InboxRedisClient & { storage: Map<string, string> } {
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

test("RedisInboxStore: a void completion is reported done; getResult stays undefined", async () => {
  const store = new RedisInboxStore({ client: fakeInboxRedis() })
  const c1 = await store.claim("v")
  assert.equal(c1.acquired, true)
  await store.markSeen("v", undefined) // handler returned nothing

  // Distinguishable from "absent": seen + done are true, but there is no value.
  assert.equal(await store.hasSeen("v"), true)
  assert.equal(await store.isDone("v"), true)
  assert.equal(await store.getResult("v"), undefined)

  // A loser claiming now is told it is NOT acquired (key exists / finished).
  const c2 = await store.claim("v")
  assert.equal(c2.acquired, false)
})

test("RedisInboxStore: an in-progress claim is NOT yet done", async () => {
  const store = new RedisInboxStore({ client: fakeInboxRedis() })
  await store.claim("p") // sentinel only
  assert.equal(await store.isDone("p"), false, "the bare in-progress sentinel is not a finished result")
  assert.equal(await store.getResult("p"), undefined)
})

test("Inbox.dedupe with RedisInboxStore: void handler runs ONCE across replicas", async () => {
  let calls = 0
  const shared = fakeInboxRedis()
  const inboxA = new Inbox({ store: new RedisInboxStore({ client: shared }), awaitTimeoutMs: 500 })
  const inboxB = new Inbox({ store: new RedisInboxStore({ client: shared }), awaitTimeoutMs: 500 })

  const r1 = await inboxA.dedupe("void-evt", async () => {
    calls++ /* returns undefined */
  })
  const r2 = await inboxB.dedupe("void-evt", async () => {
    calls++
    return { should: "not run" }
  })

  assert.equal(calls, 1, "the second replica must see the void completion and skip the handler")
  assert.equal(r1, undefined)
  assert.equal(r2, undefined, "replica B returns replica A's (void) result, not its own")
})

test("Inbox.dedupe with RedisInboxStore: void handler deduped under same-instance concurrency", async () => {
  let calls = 0
  let release!: () => void
  const gate = new Promise<void>((r) => {
    release = r
  })
  const inbox = new Inbox({ store: new RedisInboxStore({ client: fakeInboxRedis() }), awaitTimeoutMs: 500 })
  const run = () =>
    inbox.dedupe("void-cc", async () => {
      calls++
      await gate /* returns undefined */
    })

  const p1 = run()
  const p2 = run()
  await sleep(20)
  release()
  const [a, b] = await Promise.all([p1, p2])

  assert.equal(calls, 1, "in-process leader election dedups the void handler")
  assert.equal(a, undefined)
  assert.equal(b, undefined)
})

// ===========================================================================
// Fix 3 — delete must actually remove the entry: has() is false afterward.
// ===========================================================================

test("LruIdempotencyCache.delete removes the entry (has/get report absent)", () => {
  const c = new LruIdempotencyCache<number>({ enabled: true, ttlMs: 60_000 })
  c.set("a", 1)
  assert.equal(c.has("a"), true)
  assert.equal(c.delete("a"), true)
  assert.equal(c.has("a"), false, "deleted key must not be present")
  assert.equal(c.get("a"), undefined)
  assert.equal(c.size(), 0, "no tombstone node left behind")
  assert.equal(c.delete("a"), false, "deleting a missing key returns false")
})

test("LruIdempotencyCache.delete unlinks the LRU node (list stays consistent)", () => {
  const c = new LruIdempotencyCache<number>({ enabled: true, maxEntries: 3, ttlMs: 60_000 })
  c.set("a", 1)
  c.set("b", 2)
  c.set("c", 3)
  c.delete("b") // remove a middle node
  assert.equal(c.has("b"), false)
  // The remaining entries are intact and still tracked.
  assert.equal(c.get("a"), 1)
  assert.equal(c.get("c"), 3)
  assert.equal(c.size(), 2)
  // Re-inserting after delete works and respects capacity.
  c.set("d", 4)
  c.set("e", 5)
  assert.equal(c.size(), 3, "capacity still enforced after delete + reinsert")
})

test("RedisIdempotencyStore.delete makes has() false (no live tombstone)", async () => {
  const store = new RedisIdempotencyStore<{ ok: boolean }>({ client: fakeInboxRedis(), enabled: true, ttlMs: 60_000 })
  await store.set("k1", { ok: true })
  assert.equal(await store.has("k1"), true)
  await store.delete("k1")
  assert.equal(await store.has("k1"), false, "after delete the key must read as absent, not a tombstone")
  assert.equal(await store.get("k1"), undefined)
})
