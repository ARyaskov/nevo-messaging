import { test } from "node:test"
import assert from "node:assert/strict"
import { RateLimiter } from "../src/common/rate-limit"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"

test("disabled when constructed with no options", () => {
  const rl = new RateLimiter()
  assert.equal(rl.isEnabled(), false)
  rl.check({ topic: "t", method: "m" })
})

test("token bucket exhausts after capacity is spent", () => {
  const rl = new RateLimiter({ enabled: true, capacity: 3, refillPerSec: 0 })
  rl.check({ topic: "t", method: "m", callerService: "front" })
  rl.check({ topic: "t", method: "m", callerService: "front" })
  rl.check({ topic: "t", method: "m", callerService: "front" })
  try {
    rl.check({ topic: "t", method: "m", callerService: "front" })
    assert.fail("should have thrown")
  } catch (err) {
    assert.ok(err instanceof MessagingError)
    assert.equal((err as MessagingError).code, ErrorCode.RATE_LIMITED)
    assert.equal((err as MessagingError).retryable, true)
  }
})

test("different callers have independent buckets", () => {
  const rl = new RateLimiter({ enabled: true, capacity: 1, refillPerSec: 0 })
  rl.check({ topic: "t", method: "m", callerService: "a" })
  rl.check({ topic: "t", method: "m", callerService: "b" })
  assert.throws(() => rl.check({ topic: "t", method: "m", callerService: "a" }), MessagingError)
})

test("refills tokens over time", async () => {
  const rl = new RateLimiter({ enabled: true, capacity: 1, refillPerSec: 50 })
  rl.check({ topic: "t", method: "m" })
  assert.throws(() => rl.check({ topic: "t", method: "m" }), MessagingError)
  await new Promise((r) => setTimeout(r, 60))
  rl.check({ topic: "t", method: "m" })
})

test("scopes match by predicate", () => {
  const rl = new RateLimiter({
    enabled: true,
    capacity: 100,
    refillPerSec: 100,
    scopes: [
      {
        capacity: 1,
        refillPerSec: 0,
        match: (ctx) => ctx.method === "expensive",
        keyExtractor: (ctx) => `${ctx.callerService ?? "anon"}:${ctx.method}`
      }
    ]
  })
  rl.check({ topic: "t", method: "expensive", callerService: "x" })
  assert.throws(() => rl.check({ topic: "t", method: "expensive", callerService: "x" }), MessagingError)
  for (let i = 0; i < 10; i++) rl.check({ topic: "t", method: "cheap" })
})
