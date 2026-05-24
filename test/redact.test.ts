import { test } from "node:test"
import assert from "node:assert/strict"
import { redactObject } from "../src/common/redact"

test("redacts known secret keys", () => {
  const x = { username: "alice", password: "p@ss", auth: { token: "abc" }, nested: { secret: "s" } }
  const r = redactObject(x) as any
  assert.equal(r.username, "alice")
  assert.equal(r.password, "[REDACTED]")
  assert.equal(r.auth.token, "[REDACTED]")
  assert.equal(r.nested.secret, "[REDACTED]")
})

test("handles cycles", () => {
  const obj: any = { a: 1 }
  obj.self = obj
  const r = redactObject(obj) as any
  assert.equal(r.a, 1)
  assert.equal(r.self, "[Circular]")
})

test("preserves arrays", () => {
  const r = redactObject([{ token: "x" }, { ok: 1 }]) as any
  assert.equal(r[0].token, "[REDACTED]")
  assert.equal(r[1].ok, 1)
})
