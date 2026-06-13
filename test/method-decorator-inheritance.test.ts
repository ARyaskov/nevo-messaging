import { test } from "node:test"
import assert from "node:assert/strict"
import { Cacheable, RateLimit, getMethodCacheable, getMethodRateLimit } from "../src/common/method-decorators"

test("@RateLimit and @Cacheable subclass metadata does not leak into the parent", () => {
  class Parent {
    parent() {}
  }
  class Child extends Parent {
    child() {}
  }
  RateLimit({ capacity: 2, refillPerSec: 1 })(Parent.prototype, "parent", { value: Parent.prototype.parent })
  Cacheable({ ttlMs: 1000 })(Child.prototype, "child", { value: Child.prototype.child })

  assert.equal(getMethodCacheable(new Parent(), "child"), undefined)
  assert.equal(getMethodRateLimit(new Child(), "parent")?.capacity, 2)
  assert.equal(getMethodCacheable(new Child(), "child")?.ttlMs, 1000)
})
