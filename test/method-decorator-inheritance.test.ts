import { test } from "node:test"
import assert from "node:assert/strict"
import { Cacheable, RateLimit, getMethodCacheable, getMethodRateLimit } from "../src/common/method-decorators"
import { Scheduled, getScheduledMethods } from "../src/common/scheduler"
import { Workflow, getWorkflowMethods } from "../src/common/workflow"

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

test("@Scheduled subclass metadata does not leak into the parent", () => {
  class Parent {
    parentTick() {}
  }
  class Child extends Parent {
    childTick() {}
  }
  Scheduled({ cron: "* * * * *" })(Parent.prototype, "parentTick", { value: Parent.prototype.parentTick })
  Scheduled({ cron: "0 * * * *" })(Child.prototype, "childTick", { value: Child.prototype.childTick })

  assert.deepEqual(
    getScheduledMethods(new Parent()).map((m) => m.propertyKey),
    ["parentTick"]
  )
  assert.deepEqual(
    getScheduledMethods(new Child())
      .map((m) => m.propertyKey)
      .sort(),
    ["childTick", "parentTick"]
  )
})

test("@Workflow subclass metadata does not leak into the parent", () => {
  class Parent {
    parentFlow() {}
  }
  class Child extends Parent {
    childFlow() {}
  }
  Workflow({ name: "parent-flow" })(Parent.prototype, "parentFlow", { value: Parent.prototype.parentFlow })
  Workflow({ name: "child-flow" })(Child.prototype, "childFlow", { value: Child.prototype.childFlow })

  assert.deepEqual(
    getWorkflowMethods(new Parent()).map((m) => m.name),
    ["parent-flow"]
  )
  assert.equal(getWorkflowMethods(new Child()).length, 2)
})

test("a subclass overriding a scheduled method replaces the parent's entry", () => {
  class Parent {
    tick() {}
  }
  class Child extends Parent {
    override tick() {}
  }
  Scheduled({ cron: "* * * * *", name: "from-parent" })(Parent.prototype, "tick", { value: Parent.prototype.tick })
  Scheduled({ cron: "0 0 * * *", name: "from-child" })(Child.prototype, "tick", { value: Child.prototype.tick })

  const child = getScheduledMethods(new Child())
  assert.equal(child.length, 1, "one entry per method, not one per decorator application up the chain")
  assert.equal(child[0].name, "from-child")
  assert.equal(getScheduledMethods(new Parent())[0].name, "from-parent")
})
