import { test } from "node:test"
import assert from "node:assert/strict"
import { extractCallerService, isAccessAllowed } from "../src/common/access-control"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"
import type { AccessControlConfig, MessageMeta } from "../src/common/types"

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

// --- extractCallerService: identity extraction hardening ---------------------

// A signature-verifying stub standing in for a real JWKS verifier: only the
// exact token "valid-frontend" is accepted; everything else is rejected (null),
// as a real verifier would reject a forged or absent signature.
const frontendVerifier = async (token: string) => (token === "valid-frontend" ? { sub: "frontend" } : null)

// Build a well-formed but UNSIGNED JWT (alg:none) carrying attacker-chosen
// claims. The pre-fix code base64-decoded this and trusted its claims.
function makeUnsignedJwt(claims: Record<string, unknown>): string {
  const seg = (o: unknown) => Buffer.from(JSON.stringify(o)).toString("base64url")
  return `${seg({ alg: "none", typ: "JWT" })}.${seg(claims)}.`
}

test("verifier set: spoofed meta.service with no token is anonymous, not the spoofed service", async () => {
  const meta: MessageMeta = { service: "frontend" }
  assert.equal(await extractCallerService(meta, frontendVerifier), undefined)
})

test("verifier set: spoofed meta.service with an invalid token is anonymous, not the spoofed service", async () => {
  const meta: MessageMeta = { service: "frontend", auth: { token: "forged" } }
  assert.equal(await extractCallerService(meta, frontendVerifier), undefined)
})

test("verifier set: identity comes from the verified token", async () => {
  const meta: MessageMeta = { auth: { token: "valid-frontend" } }
  assert.equal(await extractCallerService(meta, frontendVerifier), "frontend")
})

test("verifier set: meta.service matching the verified identity is accepted", async () => {
  const meta: MessageMeta = { service: "frontend", auth: { token: "valid-frontend" } }
  assert.equal(await extractCallerService(meta, frontendVerifier), "frontend")
})

test("verifier set: meta.service disagreeing with the verified identity is rejected", async () => {
  const meta: MessageMeta = { service: "admin", auth: { token: "valid-frontend" } }
  await assert.rejects(
    () => extractCallerService(meta, frontendVerifier),
    (err: unknown) => err instanceof MessagingError && err.code === ErrorCode.UNAUTHORIZED
  )
})

test("no verifier: an unsigned token is ignored and yields anonymous identity", async () => {
  const forged = makeUnsignedJwt({ service: "admin", serviceName: "admin", sub: "admin" })
  const meta: MessageMeta = { auth: { token: forged } }
  // Pre-fix this returned "admin" by decoding the unsigned payload; with no
  // verifier configured it must now be anonymous since nothing verified the token.
  assert.equal(await extractCallerService(meta), undefined)
})

test("no verifier: meta.service is trusted as identity (trusted-network mode)", async () => {
  assert.equal(await extractCallerService({ service: "frontend" }), "frontend")
})

test("verifier set: a verified token is cached and not re-verified per call", async () => {
  let calls = 0
  const counting = async (token: string) => {
    calls++
    return token === "valid-frontend" ? { sub: "frontend", exp: Math.floor(Date.now() / 1000) + 300 } : null
  }
  const meta: MessageMeta = { auth: { token: "valid-frontend" } }
  assert.equal(await extractCallerService(meta, counting), "frontend")
  assert.equal(await extractCallerService(meta, counting), "frontend")
  assert.equal(calls, 1)
})
