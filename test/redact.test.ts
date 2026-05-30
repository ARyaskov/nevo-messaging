import { test } from "node:test"
import assert from "node:assert/strict"
import { redactObject, jsonByteSize } from "../src/common/redact"

test("redacts known secret keys", () => {
  const x = { username: "alice", password: "p@ss", auth: { token: "abc" }, nested: { secret: "s" } }
  const r = redactObject(x) as any
  assert.equal(r.username, "alice")
  assert.equal(r.password, "[REDACTED]")
  // "auth" matches the sensitive-substring set, so the whole blob is redacted.
  assert.equal(r.auth, "[REDACTED]")
  assert.equal(r.nested.secret, "[REDACTED]")
})

test("handles cycles", () => {
  const obj: any = { a: 1 }
  obj.self = obj
  const r = redactObject(obj) as any
  assert.equal(r.a, 1)
  assert.equal(r.self, "[Circular]")
})

test("flags only genuine cycles, not shared references", () => {
  const a: any = { name: "a" }
  const b: any = { name: "b", a }
  a.b = b // real cycle: a -> b -> a
  const r = redactObject(a) as any
  assert.equal(r.name, "a")
  assert.equal(r.b.name, "b")
  assert.equal(r.b.a, "[Circular]")
})

test("does not mark a shared (non-circular) reference as circular", () => {
  const shared = { id: 7, label: "cfg" }
  const root = { left: shared, right: shared, list: [shared, shared] }
  const r = redactObject(root) as any
  assert.deepEqual(r.left, { id: 7, label: "cfg" })
  assert.deepEqual(r.right, { id: 7, label: "cfg" })
  assert.deepEqual(r.list[0], { id: 7, label: "cfg" })
  assert.deepEqual(r.list[1], { id: 7, label: "cfg" })
  assert.notEqual(r.right, "[Circular]")
})

test("preserves arrays", () => {
  const r = redactObject([{ token: "x" }, { ok: 1 }]) as any
  assert.equal(r[0].token, "[REDACTED]")
  assert.equal(r[1].ok, 1)
})

test("redacts expanded and substring secret keys", () => {
  const r = redactObject({
    authorization: "Bearer abc",
    "x-api-key": "k-123",
    userPassword: "p",
    db_password_enc: "q",
    refresh_token: "r",
    client_secret: "s",
    pwd: "t",
    cvv: "123",
    bearer: "tok",
    username: "keep-me"
  }) as any
  assert.equal(r.authorization, "[REDACTED]")
  assert.equal(r["x-api-key"], "[REDACTED]")
  assert.equal(r.userPassword, "[REDACTED]")
  assert.equal(r.db_password_enc, "[REDACTED]")
  assert.equal(r.refresh_token, "[REDACTED]")
  assert.equal(r.client_secret, "[REDACTED]")
  assert.equal(r.pwd, "[REDACTED]")
  assert.equal(r.cvv, "[REDACTED]")
  assert.equal(r.bearer, "[REDACTED]")
  assert.equal(r.username, "keep-me")
})

test("summarizes binary data instead of expanding it", () => {
  const r = redactObject({ blob: Buffer.from("hello world"), arr: new Uint8Array([1, 2, 3]) }) as any
  assert.equal(r.blob, "[Buffer 11B]")
  assert.equal(r.arr, "[Buffer 3B]")
})

test("unfolds Map/Set and passes Date/RegExp through", () => {
  const when = new Date("2020-01-02T03:04:05.000Z")
  const r = redactObject({
    map: new Map<string, unknown>([["password", "x"], ["name", "alice"]]),
    set: new Set([1, 2, 2]),
    when,
    re: /abc/g
  }) as any
  assert.equal(r.map.password, "[REDACTED]")
  assert.equal(r.map.name, "alice")
  assert.deepEqual(r.set, [1, 2])
  assert.ok(r.when instanceof Date)
  assert.equal(r.when.getTime(), when.getTime())
  assert.ok(r.re instanceof RegExp)
})

test("jsonByteSize estimates redacted size and short-circuits", () => {
  assert.ok(jsonByteSize({ a: "hi" }) > 0)
  // A sensitive value collapses to "[REDACTED]" in the estimate — the 10k blob
  // behind it is never counted, so the entry stays well under the limit.
  const big = "x".repeat(10_000)
  assert.ok(jsonByteSize({ password: big }, 100) < 100)
  // A plain oversized value bails out past the limit.
  assert.ok(jsonByteSize({ blob: big }, 100) > 100)
  // Binaries are summarized, not expanded, so they don't blow the budget.
  assert.ok(jsonByteSize({ buf: Buffer.alloc(10_000) }, 100) < 100)
  // Cycles must not hang the estimator.
  const c: any = {}
  c.self = c
  assert.ok(jsonByteSize(c) > 0)
})
