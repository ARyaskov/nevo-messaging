import { test } from "node:test"
import assert from "node:assert/strict"
import { generateKeyPairSync, createSign, createHmac, constants, KeyObject } from "node:crypto"
import { createJwksVerifier, JwksKey } from "../src/common/jwt-verifier"

const nowSec = () => Math.floor(Date.now() / 1000)

function b64url(input: Buffer | string): string {
  const b = typeof input === "string" ? Buffer.from(input, "utf8") : input
  return b.toString("base64").replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "")
}

interface KeyPair {
  privateKey: KeyObject
  publicKey: KeyObject
  jwk: JwksKey
}

function rsaKeyPair(kid: string, jwkAlg?: string): KeyPair {
  const { publicKey, privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 })
  const jwk = { ...publicKey.export({ format: "jwk" }), kid } as JwksKey
  if (jwkAlg) jwk.alg = jwkAlg
  return { privateKey, publicKey, jwk }
}

function ecKeyPair(kid: string, namedCurve = "P-256"): KeyPair {
  const { publicKey, privateKey } = generateKeyPairSync("ec", { namedCurve })
  const jwk = { ...publicKey.export({ format: "jwk" }), kid } as JwksKey
  return { privateKey, publicKey, jwk }
}

const HASH: Record<string, string> = {
  RS256: "sha256",
  RS384: "sha384",
  RS512: "sha512",
  PS256: "sha256",
  PS384: "sha384",
  PS512: "sha512",
  ES256: "sha256",
  ES384: "sha384",
  ES512: "sha512"
}

function sign(alg: string, key: KeyObject, signingInput: string): Buffer {
  const signer = createSign(HASH[alg])
  signer.update(signingInput)
  signer.end()
  if (alg.startsWith("ES")) return signer.sign({ key, dsaEncoding: "ieee-p1363" })
  if (alg.startsWith("PS")) return signer.sign({ key, padding: constants.RSA_PKCS1_PSS_PADDING, saltLength: constants.RSA_PSS_SALTLEN_DIGEST })
  return signer.sign(key)
}

function makeToken(opts: {
  alg: string
  key?: KeyObject
  kid?: string
  header?: Record<string, unknown>
  payload?: Record<string, unknown>
}): string {
  const header = { alg: opts.alg, ...(opts.kid ? { kid: opts.kid } : {}), ...(opts.header ?? {}) }
  const h = b64url(JSON.stringify(header))
  const p = b64url(JSON.stringify(opts.payload ?? {}))
  const signingInput = `${h}.${p}`
  const sig = opts.key ? sign(opts.alg, opts.key, signingInput) : Buffer.alloc(0)
  return `${signingInput}.${b64url(sig)}`
}

// A fetch stand-in whose body can vary per call, with a call counter so tests can
// assert how many times the JWKS was fetched.
function countingFetch(responder: (call: number) => Promise<{ keys: JwksKey[] }>) {
  let calls = 0
  const fn = (async () => {
    calls++
    const body = await responder(calls)
    return { ok: true, status: 200, json: async () => body }
  }) as unknown as typeof fetch
  return { fn, calls: () => calls }
}

const RSA = rsaKeyPair("rsa-1")
const EC = ecKeyPair("ec-1")

function rsaVerifier(extra: Record<string, unknown> = {}) {
  const { fn } = countingFetch(async () => ({ keys: [RSA.jwk] }))
  return createJwksVerifier({ jwksUri: "https://issuer/jwks.json", fetchImpl: fn, ...extra })
}

test("verifies a valid RS256 token", async () => {
  const verify = rsaVerifier()
  const token = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "rsa-1", payload: { sub: "svc-a", exp: nowSec() + 3600 } })
  assert.equal((await verify(token))?.sub, "svc-a")
})

test("verifies a valid ES256 token", async () => {
  const { fn } = countingFetch(async () => ({ keys: [EC.jwk] }))
  const verify = createJwksVerifier({ jwksUri: "u", fetchImpl: fn })
  const token = makeToken({ alg: "ES256", key: EC.privateKey, kid: "ec-1", payload: { sub: "svc-a", exp: nowSec() + 3600 } })
  assert.equal((await verify(token))?.sub, "svc-a")
})

test("verifies a valid PS256 token", async () => {
  const verify = rsaVerifier()
  const token = makeToken({ alg: "PS256", key: RSA.privateKey, kid: "rsa-1", payload: { sub: "svc-a", exp: nowSec() + 3600 } })
  assert.equal((await verify(token))?.sub, "svc-a")
})

test("rejects alg:none", async () => {
  const verify = rsaVerifier()
  const token = makeToken({ alg: "none", kid: "rsa-1", payload: { sub: "attacker", exp: nowSec() + 3600 } })
  assert.equal(await verify(token), null)
})

test("rejects HS256 even when signed with the public key (alg confusion)", async () => {
  const verify = rsaVerifier()
  const pubPem = RSA.publicKey.export({ type: "spki", format: "pem" }) as string
  const header = b64url(JSON.stringify({ alg: "HS256", kid: "rsa-1" }))
  const payload = b64url(JSON.stringify({ sub: "attacker", exp: nowSec() + 3600 }))
  const signingInput = `${header}.${payload}`
  const sig = createHmac("sha256", pubPem).update(signingInput).digest()
  const token = `${signingInput}.${b64url(sig)}`
  assert.equal(await verify(token), null)
})

test("rejects an unknown kid without falling back to another key", async () => {
  // The token is genuinely signed by rsa-1's key, but its header names a kid that
  // is absent from the JWKS. The old code fell back to keys[0] (== rsa-1) and
  // wrongly accepted it; the hardened verifier must refuse.
  const verify = rsaVerifier()
  const token = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "ghost", payload: { sub: "x", exp: nowSec() + 3600 } })
  assert.equal(await verify(token), null)
})

test("fails closed (throws) when the JWKS fetch fails with nothing cached", async () => {
  const { fn } = countingFetch(async () => {
    throw new Error("network down")
  })
  const verify = createJwksVerifier({ jwksUri: "u", fetchImpl: fn })
  const token = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "rsa-1", payload: { sub: "x", exp: nowSec() + 3600 } })
  await assert.rejects(() => verify(token))
})

test("serves a stale cached key set when a later JWKS refetch fails", async () => {
  const { fn, calls } = countingFetch(async (call) => {
    if (call === 1) return { keys: [RSA.jwk] }
    throw new Error("network down")
  })
  // cacheTtlMs: 0 forces a refetch attempt on every verify.
  const verify = createJwksVerifier({ jwksUri: "u", fetchImpl: fn, cacheTtlMs: 0 })
  const token = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "rsa-1", payload: { sub: "svc-a", exp: nowSec() + 3600 } })
  assert.equal((await verify(token))?.sub, "svc-a") // call 1: fresh keys
  assert.equal((await verify(token))?.sub, "svc-a") // call 2: fetch throws -> stale cache served
  assert.equal(calls(), 2)
})

test("refetches once on a kid miss to pick up a rotated key", async () => {
  const oldKey = rsaKeyPair("old-1")
  const newKey = rsaKeyPair("new-1")
  const { fn, calls } = countingFetch(async (call) => (call === 1 ? { keys: [oldKey.jwk] } : { keys: [oldKey.jwk, newKey.jwk] }))
  const verify = createJwksVerifier({ jwksUri: "u", fetchImpl: fn })
  const token = makeToken({ alg: "RS256", key: newKey.privateKey, kid: "new-1", payload: { sub: "svc-a", exp: nowSec() + 3600 } })
  // call 1 fills the cache (old key only) -> kid miss -> call 2 refetch finds new-1.
  assert.equal((await verify(token))?.sub, "svc-a")
  assert.equal(calls(), 2)
  // The rotated key is now cached, so a second verify needs no further fetch.
  assert.equal((await verify(token))?.sub, "svc-a")
  assert.equal(calls(), 2)
})

test("rate-limits rotation refetches for a kid that never appears", async () => {
  const { fn, calls } = countingFetch(async () => ({ keys: [RSA.jwk] }))
  const verify = createJwksVerifier({ jwksUri: "u", fetchImpl: fn })
  const token = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "ghost", payload: { sub: "x", exp: nowSec() + 3600 } })
  assert.equal(await verify(token), null) // call 1 (fill) + call 2 (rotation refetch)
  assert.equal(await verify(token), null) // cache valid; rotation rate-limited -> no new fetch
  assert.equal(calls(), 2)
})

test("rejects a token with no exp when requireExp (default)", async () => {
  const verify = rsaVerifier()
  const token = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "rsa-1", payload: { sub: "x" } })
  assert.equal(await verify(token), null)
})

test("allows a token with no exp when requireExp is false", async () => {
  const verify = rsaVerifier({ requireExp: false })
  const token = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "rsa-1", payload: { sub: "x" } })
  assert.equal((await verify(token))?.sub, "x")
})

test("rejects an expired token but tolerates clock skew", async () => {
  const verify = rsaVerifier({ clockSkewSec: 30 })
  const expired = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "rsa-1", payload: { sub: "x", exp: nowSec() - 120 } })
  assert.equal(await verify(expired), null)
  const recent = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "rsa-1", payload: { sub: "x", exp: nowSec() - 5 } })
  assert.equal((await verify(recent))?.sub, "x")
})

test("rejects unknown crit header parameters", async () => {
  const verify = rsaVerifier()
  const token = makeToken({
    alg: "RS256",
    key: RSA.privateKey,
    kid: "rsa-1",
    header: { crit: ["http://example/exp"] },
    payload: { sub: "x", exp: nowSec() + 3600 }
  })
  assert.equal(await verify(token), null)
})

test("rejects when the JWK key type does not match the alg family", async () => {
  // JWKS advertises an EC key under kid "k1"; the token header claims RS256.
  const ec = ecKeyPair("k1")
  const rsa = rsaKeyPair("k1")
  const { fn } = countingFetch(async () => ({ keys: [ec.jwk] }))
  const verify = createJwksVerifier({ jwksUri: "u", fetchImpl: fn })
  const token = makeToken({ alg: "RS256", key: rsa.privateKey, kid: "k1", payload: { sub: "x", exp: nowSec() + 3600 } })
  assert.equal(await verify(token), null)
})

test("rejects when the JWK pins an alg different from the header alg", async () => {
  const kp = rsaKeyPair("pinned", "RS512")
  const { fn } = countingFetch(async () => ({ keys: [kp.jwk] }))
  const verify = createJwksVerifier({ jwksUri: "u", fetchImpl: fn })
  const token = makeToken({ alg: "RS256", key: kp.privateKey, kid: "pinned", payload: { sub: "x", exp: nowSec() + 3600 } })
  assert.equal(await verify(token), null)
})

test("rejects an alg outside a restricted allow-list", async () => {
  const verify = rsaVerifier({ allowedAlgorithms: ["ES256"] })
  const token = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "rsa-1", payload: { sub: "x", exp: nowSec() + 3600 } })
  assert.equal(await verify(token), null)
})

test("rejects a token whose signature does not verify", async () => {
  const other = rsaKeyPair("rsa-1") // different key under the same kid
  const { fn } = countingFetch(async () => ({ keys: [RSA.jwk] }))
  const verify = createJwksVerifier({ jwksUri: "u", fetchImpl: fn })
  const token = makeToken({ alg: "RS256", key: other.privateKey, kid: "rsa-1", payload: { sub: "x", exp: nowSec() + 3600 } })
  assert.equal(await verify(token), null)
})

test("enforces issuer and audience when configured", async () => {
  const verify = rsaVerifier({ issuer: "https://iss", audience: "svc-a" })
  const good = makeToken({
    alg: "RS256",
    key: RSA.privateKey,
    kid: "rsa-1",
    payload: { sub: "x", iss: "https://iss", aud: ["svc-a", "other"], exp: nowSec() + 3600 }
  })
  assert.equal((await verify(good))?.sub, "x")
  const badIss = makeToken({
    alg: "RS256",
    key: RSA.privateKey,
    kid: "rsa-1",
    payload: { sub: "x", iss: "https://evil", aud: "svc-a", exp: nowSec() + 3600 }
  })
  assert.equal(await verify(badIss), null)
  const badAud = makeToken({
    alg: "RS256",
    key: RSA.privateKey,
    kid: "rsa-1",
    payload: { sub: "x", iss: "https://iss", aud: "nope", exp: nowSec() + 3600 }
  })
  assert.equal(await verify(badAud), null)
})

test("ignores non-string aud entries when matching audience", async () => {
  const verify = rsaVerifier({ audience: "svc-a" })
  const mixed = makeToken({
    alg: "RS256",
    key: RSA.privateKey,
    kid: "rsa-1",
    payload: { sub: "x", aud: [123, { x: 1 }, "svc-a"], exp: nowSec() + 3600 }
  })
  assert.equal((await verify(mixed))?.sub, "x")
  const junk = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "rsa-1", payload: { sub: "x", aud: [123, null], exp: nowSec() + 3600 } })
  assert.equal(await verify(junk), null)
})

test("requireIss and requireAud reject tokens missing those claims", async () => {
  const verify = rsaVerifier({ requireIss: true, requireAud: true })
  const missing = makeToken({ alg: "RS256", key: RSA.privateKey, kid: "rsa-1", payload: { sub: "x", exp: nowSec() + 3600 } })
  assert.equal(await verify(missing), null)
  const present = makeToken({
    alg: "RS256",
    key: RSA.privateKey,
    kid: "rsa-1",
    payload: { sub: "x", iss: "whoever", aud: "anything", exp: nowSec() + 3600 }
  })
  assert.equal((await verify(present))?.sub, "x")
})

test("selects the only key when no kid is present, but refuses an ambiguous set", async () => {
  const a = rsaKeyPair("a")
  const b = rsaKeyPair("b")
  const single = countingFetch(async () => ({ keys: [a.jwk] }))
  const verifySingle = createJwksVerifier({ jwksUri: "u", fetchImpl: single.fn })
  const ok = makeToken({ alg: "RS256", key: a.privateKey, payload: { sub: "x", exp: nowSec() + 3600 } })
  assert.equal((await verifySingle(ok))?.sub, "x")

  const multi = countingFetch(async () => ({ keys: [a.jwk, b.jwk] }))
  const verifyMulti = createJwksVerifier({ jwksUri: "u", fetchImpl: multi.fn })
  const ambiguous = makeToken({ alg: "RS256", key: a.privateKey, payload: { sub: "x", exp: nowSec() + 3600 } })
  assert.equal(await verifyMulti(ambiguous), null)
})
