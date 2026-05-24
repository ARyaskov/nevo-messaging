import { test } from "node:test"
import assert from "node:assert/strict"
import { LruIdempotencyCache } from "../src/common/idempotency"

test("LRU evicts oldest", () => {
  const c = new LruIdempotencyCache<number>({ enabled: true, maxEntries: 2, ttlMs: 60_000 })
  c.set("a", 1); c.set("b", 2); c.set("c", 3)
  assert.equal(c.has("a"), false)
  assert.equal(c.get("b"), 2)
  assert.equal(c.get("c"), 3)
})

test("TTL expiration", async () => {
  const c = new LruIdempotencyCache<number>({ enabled: true, maxEntries: 10, ttlMs: 30 })
  c.set("a", 1)
  await new Promise((r) => setTimeout(r, 60))
  assert.equal(c.has("a"), false)
})

test("disabled cache does nothing", () => {
  const c = new LruIdempotencyCache<number>({ enabled: false })
  c.set("a", 1)
  assert.equal(c.has("a"), false)
})
