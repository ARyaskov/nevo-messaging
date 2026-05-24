import { test } from "node:test"
import assert from "node:assert/strict"
import { isAccessAllowed } from "../src/common/access-control"
import type { AccessControlConfig } from "../src/common/types"

test("undefined ACL allows all", () => {
  assert.equal(isAccessAllowed(undefined, "any-topic", "any.method", "frontend"), true)
})

test("matching allow grants access", () => {
  const cfg: AccessControlConfig = {
    rules: [{ topic: "user-events", method: "*", allow: ["frontend"] }]
  }
  assert.equal(isAccessAllowed(cfg, "user-events", "user.getById", "frontend"), true)
})

test("matching allow rejects unknown caller", () => {
  const cfg: AccessControlConfig = {
    rules: [{ topic: "user-events", method: "*", allow: ["frontend"] }]
  }
  assert.equal(isAccessAllowed(cfg, "user-events", "user.getById", "evil"), false)
})

test("deny overrides allow on same rule", () => {
  const cfg: AccessControlConfig = {
    rules: [
      { topic: "user-events", method: "*", allow: ["*"], deny: ["evil"] }
    ]
  }
  assert.equal(isAccessAllowed(cfg, "user-events", "any", "evil"), false)
  assert.equal(isAccessAllowed(cfg, "user-events", "any", "frontend"), true)
})

test("topic/method not matched falls back to allowAllByDefault", () => {
  const cfg: AccessControlConfig = {
    rules: [{ topic: "other", method: "*", allow: ["frontend"] }],
    allowAllByDefault: false
  }
  assert.equal(isAccessAllowed(cfg, "user-events", "user.getById", "frontend"), false)

  const cfg2: AccessControlConfig = {
    rules: [{ topic: "other", method: "*", allow: ["frontend"] }],
    allowAllByDefault: true
  }
  assert.equal(isAccessAllowed(cfg2, "user-events", "user.getById", "frontend"), true)
})

test("matched rule without allow falls through to allow", () => {
  const cfg: AccessControlConfig = {
    rules: [{ topic: "user-events", method: "ping", deny: ["evil"] }]
  }
  assert.equal(isAccessAllowed(cfg, "user-events", "ping", "frontend"), true)
  assert.equal(isAccessAllowed(cfg, "user-events", "ping", "evil"), false)
})
