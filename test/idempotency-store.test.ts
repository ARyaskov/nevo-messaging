import { test } from "node:test"
import assert from "node:assert/strict"
import { RedisIdempotencyStore, type IdempotencyRedisLike } from "../src/common/idempotency-store"

function fakeRedis(): IdempotencyRedisLike & { storage: Map<string, string>; getCalls: number; setCalls: number } {
  const storage = new Map<string, string>()
  let getCalls = 0
  let setCalls = 0
  return {
    storage,
    get getCalls() { return getCalls },
    get setCalls() { return setCalls },
    async get(key: string) { getCalls++; return storage.get(key) ?? null },
    async set(key, value, opts) {
      setCalls++
      if (opts.ifNotExists && storage.has(key)) return null
      storage.set(key, value)
      return "OK"
    },
    async del(key) { return storage.delete(key) ? 1 : 0 }
  } as any
}

test("RedisIdempotencyStore set + get round-trips through fake redis", async () => {
  const redis = fakeRedis()
  const store = new RedisIdempotencyStore<{ ok: boolean }>({ client: redis, enabled: true, ttlMs: 1_000 })
  assert.equal(await store.has("k1"), false)
  await store.set("k1", { ok: true })
  assert.equal(await store.has("k1"), true)
  assert.deepEqual(await store.get("k1"), { ok: true })
})

test("RedisIdempotencyStore L1 caches reads after first hit", async () => {
  const redis = fakeRedis() as any
  const store = new RedisIdempotencyStore<number>({ client: redis, enabled: true, ttlMs: 60_000 })
  await store.set("hot", 7)
  const before = redis.getCalls
  await store.get("hot") // L1 absorbs after `set`
  await store.get("hot")
  await store.get("hot")
  // No additional redis.get calls past the L1 warm-up.
  assert.ok(redis.getCalls - before <= 1)
})

test("RedisIdempotencyStore disabled returns nothing", async () => {
  const store = new RedisIdempotencyStore({ client: fakeRedis(), enabled: false })
  await store.set("x", 1)
  assert.equal(await store.has("x"), false)
  assert.equal(await store.get("x"), undefined)
})

// ---------------------------------------------------------------------------
// Claim-before-execute
// ---------------------------------------------------------------------------

test("claim acquires once; second claim sees in-progress, then the committed result", async () => {
  const store = new RedisIdempotencyStore<{ n: number }>({ client: fakeRedis(), enabled: true, ttlMs: 60_000 })
  const c1 = await store.claim("k")
  assert.equal(c1.acquired, true)
  // While the sentinel is held: not acquired, and no real result yet.
  const c2 = await store.claim("k")
  assert.equal(c2.acquired, false)
  assert.equal(c2.existing, undefined)
  // The winner commits — overwriting its own sentinel.
  await store.set("k", { n: 7 })
  const c3 = await store.claim("k")
  assert.equal(c3.acquired, false)
  assert.deepEqual(c3.existing, { n: 7 })
})

test("set overwrites the in-progress sentinel (no NX strand)", async () => {
  const store = new RedisIdempotencyStore<number>({ client: fakeRedis(), enabled: true, ttlMs: 60_000 })
  await store.claim("k")  // writes the sentinel with SET NX
  await store.set("k", 5) // must replace it even though the key already exists
  assert.equal(await store.get("k"), 5)
})

test("awaitResult resolves once a peer commits the real result", async () => {
  const store = new RedisIdempotencyStore<number>({ client: fakeRedis(), enabled: true, ttlMs: 60_000 })
  await store.claim("k") // in-progress sentinel
  const waiter = store.awaitResult("k", { timeoutMs: 1_000, pollMs: 5 })
  setTimeout(() => { void store.set("k", 99) }, 20)
  assert.equal(await waiter, 99)
})

test("awaitResult returns undefined when no result appears before the deadline", async () => {
  const store = new RedisIdempotencyStore<number>({ client: fakeRedis(), enabled: true, ttlMs: 60_000 })
  await store.claim("k")
  assert.equal(await store.awaitResult("k", { timeoutMs: 40, pollMs: 5 }), undefined)
})

// ---------------------------------------------------------------------------
// Fail-open / fail-closed read-error policy
// ---------------------------------------------------------------------------

test("readErrorPolicy=closed rethrows on read failure; open treats it as a miss", async () => {
  const boom: IdempotencyRedisLike = {
    async get() { throw new Error("ECONNREFUSED") },
    async set() { return "OK" }
  }
  const closed = new RedisIdempotencyStore({ client: boom, enabled: true, readErrorPolicy: "closed" })
  await assert.rejects(() => Promise.resolve(closed.get("k")))
  const open = new RedisIdempotencyStore({ client: boom, enabled: true, readErrorPolicy: "open" })
  assert.equal(await open.get("k"), undefined)
})

test("readErrorPolicy=open claim falls open to acquired (execute) when the store is down", async () => {
  const boom: IdempotencyRedisLike = {
    async get() { return null },
    async set() { throw new Error("ECONNREFUSED") }
  }
  const open = new RedisIdempotencyStore({ client: boom, enabled: true, readErrorPolicy: "open" })
  const c = await open.claim("k")
  assert.equal(c.acquired, true)
})
