import { createPublicKey, createVerify } from "node:crypto"

export interface JwksKey {
  kty: "RSA" | "EC" | string
  kid?: string
  n?: string
  e?: string
  x?: string
  y?: string
  crv?: string
  alg?: string
  use?: string
}

export interface JwksKeySet {
  keys: JwksKey[]
}

export interface JwksVerifierOptions {
  jwksUri: string
  issuer?: string
  audience?: string | string[]
  cacheTtlMs?: number
  clockSkewSec?: number
  fetchImpl?: typeof fetch
}

export interface VerifiedClaims {
  iss?: string
  sub?: string
  aud?: string | string[]
  exp?: number
  nbf?: number
  iat?: number
  [k: string]: unknown
}

function base64UrlToBuffer(s: string): Buffer {
  const padded = s.replaceAll("-", "+").replaceAll("_", "/")
  const pad = padded.padEnd(padded.length + ((4 - (padded.length % 4)) % 4), "=")
  return Buffer.from(pad, "base64")
}

function decodeJwtHeader(token: string): { kid?: string; alg?: string } | null {
  const dot = token.indexOf(".")
  if (dot < 0) return null
  try {
    const headerJson = base64UrlToBuffer(token.slice(0, dot)).toString("utf8")
    return JSON.parse(headerJson)
  } catch { return null }
}

function decodeJwtPayloadRaw(token: string): { payload: VerifiedClaims | null; signedBytes: Buffer; signature: Buffer } | null {
  const parts = token.split(".")
  if (parts.length !== 3) return null
  try {
    const payload = JSON.parse(base64UrlToBuffer(parts[1]).toString("utf8")) as VerifiedClaims
    const signedBytes = Buffer.from(parts[0] + "." + parts[1], "utf8")
    const signature = base64UrlToBuffer(parts[2])
    return { payload, signedBytes, signature }
  } catch { return null }
}

function jwkToPublicKey(jwk: JwksKey): import("node:crypto").KeyObject {
  return createPublicKey({ key: jwk as any, format: "jwk" })
}

export function createJwksVerifier(opts: JwksVerifierOptions): (token: string) => Promise<VerifiedClaims | null> {
  const cacheTtlMs = opts.cacheTtlMs ?? 60 * 60_000
  const clockSkewSec = opts.clockSkewSec ?? 30
  const fetchImpl = opts.fetchImpl ?? globalThis.fetch
  let cached: { fetchedAt: number; keys: JwksKey[] } | null = null

  async function loadJwks(): Promise<JwksKey[]> {
    if (cached && Date.now() - cached.fetchedAt < cacheTtlMs) return cached.keys
    const res = await fetchImpl(opts.jwksUri)
    if (!res.ok) throw new Error(`JWKS fetch failed: ${res.status}`)
    const json = (await res.json()) as JwksKeySet
    cached = { fetchedAt: Date.now(), keys: json.keys ?? [] }
    return cached.keys
  }

  function findKey(keys: JwksKey[], kid?: string): JwksKey | undefined {
    if (kid) return keys.find((k) => k.kid === kid) ?? keys[0]
    return keys[0]
  }

  function algToVerifier(alg: string): { hash: string; verify: "rsa" | "ec" } {
    switch (alg) {
      case "RS256": return { hash: "sha256", verify: "rsa" }
      case "RS384": return { hash: "sha384", verify: "rsa" }
      case "RS512": return { hash: "sha512", verify: "rsa" }
      case "ES256": return { hash: "sha256", verify: "ec" }
      case "ES384": return { hash: "sha384", verify: "ec" }
      case "ES512": return { hash: "sha512", verify: "ec" }
      default: throw new Error(`Unsupported JWS alg: ${alg}`)
    }
  }

  return async function verify(token: string): Promise<VerifiedClaims | null> {
    const header = decodeJwtHeader(token)
    if (!header?.alg) return null
    const decoded = decodeJwtPayloadRaw(token)
    if (!decoded?.payload) return null

    const keys = await loadJwks().catch(() => [])
    const jwk = findKey(keys, header.kid)
    if (!jwk) return null

    const { hash, verify: kind } = algToVerifier(header.alg)
    const keyObj = jwkToPublicKey(jwk)
    const v = createVerify(hash)
    v.update(decoded.signedBytes)
    v.end()
    const ok = kind === "ec"
      ? v.verify({ key: keyObj, dsaEncoding: "ieee-p1363" }, decoded.signature)
      : v.verify(keyObj, decoded.signature)
    if (!ok) return null

    const now = Math.floor(Date.now() / 1000)
    if (typeof decoded.payload.exp === "number" && decoded.payload.exp + clockSkewSec < now) return null
    if (typeof decoded.payload.nbf === "number" && decoded.payload.nbf - clockSkewSec > now) return null
    if (opts.issuer && decoded.payload.iss !== opts.issuer) return null
    if (opts.audience) {
      const expected = Array.isArray(opts.audience) ? opts.audience : [opts.audience]
      const aud = decoded.payload.aud
      const audList = Array.isArray(aud) ? aud : aud ? [aud] : []
      if (!audList.some((a) => expected.includes(a))) return null
    }
    return decoded.payload
  }
}
