import { test } from "node:test"
import assert from "node:assert/strict"
import { redactObject } from "../src/common/redact"

const REDACTED = "[REDACTED]"

test("ordinary fields that merely contain 'key' or 'auth' survive", () => {
  const out = redactObject({
    authorId: "u-1",
    author: "ann",
    authority: "eu-west",
    sortKey: "created_at",
    keyword: "sale",
    monkey: "george",
    keyboard: "qwerty",
    donkey: 1,
    publicKey: "not-secret-by-name"
  }) as any

  // These all used to come back as [REDACTED], which made logs and DLQ entries useless.
  assert.equal(out.authorId, "u-1")
  assert.equal(out.author, "ann")
  assert.equal(out.authority, "eu-west")
  assert.equal(out.sortKey, "created_at")
  assert.equal(out.keyword, "sale")
  assert.equal(out.monkey, "george")
  assert.equal(out.keyboard, "qwerty")
  assert.equal(out.donkey, 1)
  assert.equal(out.publicKey, "not-secret-by-name")
})

test("actual secrets are still redacted", () => {
  const out = redactObject({
    password: "p",
    passwd: "p",
    secret: "s",
    client_secret: "s",
    token: "t",
    accessToken: "t",
    access_token: "t",
    refreshToken: "t",
    authToken: "t",
    authorization: "Bearer x",
    bearer: "x",
    apiKey: "k",
    api_key: "k",
    "x-api-key": "k",
    privateKey: "k",
    private_key: "k",
    accessKey: "k",
    secretKey: "k",
    signingKey: "k",
    encryptionKey: "k",
    cookie: "c",
    "set-cookie": "c",
    credentials: "c",
    ssn: "n",
    cvv: "n"
  }) as any

  for (const [field, value] of Object.entries(out)) {
    assert.equal(value, REDACTED, `${field} must be redacted`)
  }
})

test("custom keys extend the built-in list", () => {
  const out = redactObject({ internalRef: "x", keep: "y" }, ["internalRef"]) as any
  assert.equal(out.internalRef, REDACTED)
  assert.equal(out.keep, "y")
})

test("nested and collection shapes are redacted throughout", () => {
  const out = redactObject({
    user: { name: "ann", password: "p" },
    list: [{ token: "t" }, { id: 1 }],
    set: new Set([{ secret: "s" }]),
    map: new Map<string, unknown>([
      ["apiKey", "k"],
      ["sortKey", "ok"]
    ])
  }) as any

  assert.equal(out.user.name, "ann")
  assert.equal(out.user.password, REDACTED)
  assert.equal(out.list[0].token, REDACTED)
  assert.equal(out.list[1].id, 1)
  assert.equal(out.set[0].secret, REDACTED)
  assert.equal(out.map.apiKey, REDACTED)
  assert.equal(out.map.sortKey, "ok")
})

test("a pathologically deep payload is bounded instead of overflowing the stack", () => {
  let deep: any = { leaf: true }
  for (let i = 0; i < 5000; i++) deep = { next: deep }

  const out = redactObject(deep) as any
  let cursor = out
  let depth = 0
  while (cursor && typeof cursor === "object" && cursor.next !== undefined) {
    cursor = cursor.next
    depth++
  }
  assert.ok(depth < 5000, "traversal must stop before the input's real depth")
  assert.equal(cursor.next ?? cursor, "[TooDeep]")
})

test("circular references are marked, not followed", () => {
  const node: any = { name: "n", password: "p" }
  node.self = node
  const out = redactObject(node) as any
  assert.equal(out.name, "n")
  assert.equal(out.password, REDACTED)
  assert.equal(out.self, "[Circular]")
})
