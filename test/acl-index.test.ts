import { test } from "node:test"
import assert from "node:assert/strict"
import { isAccessAllowed } from "../src/common/access-control"
import type { AccessControlConfig } from "../src/common/types"

test("compiled ACL still matches simple allow/deny", () => {
  const cfg: AccessControlConfig = {
    rules: [
      { topic: "user-events", method: "user.getById", allow: ["frontend"] },
      { topic: "user-events", method: "*", deny: ["evil"] },
      { topic: "*", method: "system.*", allow: ["*"] }
    ]
  }
  assert.equal(isAccessAllowed(cfg, "user-events", "user.getById", "frontend"), true)
  assert.equal(isAccessAllowed(cfg, "user-events", "user.getById", "evil"), false)
  assert.equal(isAccessAllowed(cfg, "user-events", "user.delete", "evil"), false)
})

test("compiled ACL falls back to allowAllByDefault when nothing matches", () => {
  const cfg: AccessControlConfig = {
    rules: [{ topic: "other", method: "x", allow: ["x"] }],
    allowAllByDefault: false
  }
  assert.equal(isAccessAllowed(cfg, "user-events", "ping", "anyone"), false)
})

test("compiled ACL is cached across calls (large rule sets)", () => {
  const rules = []
  for (let i = 0; i < 200; i++) rules.push({ topic: `t${i}`, method: `m${i}`, allow: ["svc"] })
  const cfg: AccessControlConfig = { rules }
  for (let i = 0; i < 100; i++) {
    assert.equal(isAccessAllowed(cfg, "t50", "m50", "svc"), true)
    assert.equal(isAccessAllowed(cfg, "t50", "m50", "evil"), false)
  }
})
