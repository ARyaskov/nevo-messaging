import { test } from "node:test"
import assert from "node:assert/strict"
import { Inbox, InMemoryInboxStore } from "../src/common/inbox"
import { RedisInboxStore, type InboxRedisClient } from "../src/common/inbox-redis"

function fakeRedis(): InboxRedisClient & { storage: Map<string, string> } {
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

test("Inbox.dedupe runs the handler exactly once under same-uuid concurrency", async () => {
  let calls = 0
  let release!: () => void
  const gate = new Promise<void>((r) => { release = r })
  const inbox = new Inbox({ store: new InMemoryInboxStore() })
  const run = () => inbox.dedupe("evt-1", async () => { calls++; await gate; return { v: calls } })

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

  const r1 = await inboxA.dedupe("evt-2", async () => { calls++; return { by: "A" } })
  const r2 = await inboxB.dedupe("evt-2", async () => { calls++; return { by: "B" } })

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
    async get() { throw new Error("down") },
    async set() { return "OK" },
    async exists() { throw new Error("down") }
  }
  const open = new RedisInboxStore({ client: boom, readErrorPolicy: "open" })
  assert.equal(await open.hasSeen("u1"), false)
  const closed = new RedisInboxStore({ client: boom, readErrorPolicy: "closed" })
  assert.equal(await closed.hasSeen("u1"), true)
})

test("RedisInboxStore.claim: fail-closed rethrows when the store is down", async () => {
  const boom: InboxRedisClient = {
    async get() { return null },
    async set() { throw new Error("down") }
  }
  const closed = new RedisInboxStore({ client: boom, readErrorPolicy: "closed" })
  await assert.rejects(() => closed.claim("u1"))
})