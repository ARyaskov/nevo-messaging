import { createPublicKey, createVerify, constants } from "node:crypto"

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
  // Accepted signature algorithms (allow-list). Never includes `none`/HS*.
  allowedAlgorithms?: string[]
  // Reject tokens that carry no `exp` claim (default true).
  requireExp?: boolean
  // Reject tokens with no `iss` claim (when `issuer` is set its value is always enforced too).
  requireIss?: boolean
  // Reject tokens with no usable `aud` claim (when `audience` is set its value is always enforced too).
  requireAud?: boolean
  // On a `kid` miss, throttle JWKS refetches to at most one per this interval.
  refetchMinIntervalMs?: number
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

interface JwtHeader {
  alg?: string
  kid?: string
  crit?: unknown
  [k: string]: unknown
}

type AlgFamily = "RSA" | "EC"

interface AlgSpec {
  hash: string
  family: AlgFamily
  pss: boolean
}

const DEFAULT_ALLOWED_ALGORITHMS = ["RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256"]

// Hard cap on encoded token size, enforced before any base64-decode/JSON.parse.
const MAX_TOKEN_BYTES = 8192

function isTokenOverCap(token: string): boolean {
  return typeof token !== "string" || token.length > MAX_TOKEN_BYTES
}

function base64UrlToBuffer(s: string): Buffer {
  const padded = s.replaceAll("-", "+").replaceAll("_", "/")
  const pad = padded.padEnd(padded.length + ((4 - (padded.length % 4)) % 4), "=")
  return Buffer.from(pad, "base64")
}

function decodeJwtHeader(token: string): JwtHeader | null {
  if (isTokenOverCap(token)) return null
  const dot = token.indexOf(".")
  if (dot < 0) return null
  try {
    const headerJson = base64UrlToBuffer(token.slice(0, dot)).toString("utf8")
    return JSON.parse(headerJson)
  } catch { return null }
}

function decodeJwtPayloadRaw(token: string): { payload: VerifiedClaims | null; signedBytes: Buffer; signature: Buffer } | null {
  if (isTokenOverCap(token)) return null
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

// Maps a JWS `alg` to verify primitives; returns null for unsupported algs (incl. `none`/HS*).
function algToSpec(alg: string): AlgSpec | null {
  switch (alg) {
    case "RS256": return { hash: "sha256", family: "RSA", pss: false }
    case "RS384": return { hash: "sha384", family: "RSA", pss: false }
    case "RS512": return { hash: "sha512", family: "RSA", pss: false }
    case "PS256": return { hash: "sha256", family: "RSA", pss: true }
    case "PS384": return { hash: "sha384", family: "RSA", pss: true }
    case "PS512": return { hash: "sha512", family: "RSA", pss: true }
    case "ES256": return { hash: "sha256", family: "EC", pss: false }
    case "ES384": return { hash: "sha384", family: "EC", pss: false }
    case "ES512": return { hash: "sha512", family: "EC", pss: false }
    default: return null
  }
}

function ktyMatchesFamily(kty: string | undefined, family: AlgFamily): boolean {
  return family === "RSA" ? kty === "RSA" : kty === "EC"
}

export function createJwksVerifier(opts: JwksVerifierOptions): (token: string) => Promise<VerifiedClaims | null> {
  const cacheTtlMs = opts.cacheTtlMs ?? 60 * 60_000
  const clockSkewSec = opts.clockSkewSec ?? 30
  const fetchImpl = opts.fetchImpl ?? globalThis.fetch
  const allowedAlgorithms = opts.allowedAlgorithms ?? DEFAULT_ALLOWED_ALGORITHMS
  const requireExp = opts.requireExp ?? true
  const requireIss = opts.requireIss ?? false
  const requireAud = opts.requireAud ?? false
  const refetchMinIntervalMs = opts.refetchMinIntervalMs ?? 60_000
  let cached: { fetchedAt: number; keys: JwksKey[] } | null = null
  let lastRotationRefetchAt = 0

  // Reject `none`/HS* outright; require the rest to be on the allow-list.
  function isAlgAllowed(alg: string): boolean {
    if (alg === "none") return false
    if (alg.startsWith("HS")) return false
    return allowedAlgorithms.includes(alg)
  }

  async function fetchJwks(): Promise<JwksKey[]> {
    const res = await fetchImpl(opts.jwksUri)
    if (!res.ok) throw new Error(`JWKS fetch failed: ${res.status}`)
    const json = (await res.json()) as JwksKeySet
    cached = { fetchedAt: Date.now(), keys: json.keys ?? [] }
    return cached.keys
  }

  async function loadJwks(): Promise<JwksKey[]> {
    if (cached && Date.now() - cached.fetchedAt < cacheTtlMs) return cached.keys
    try {
      return await fetchJwks()
    } catch (err) {
      // On fetch failure, serve the stale cache if present; otherwise fail closed by rethrowing.
      if (cached) return cached.keys
      throw err
    }
  }

  // Rate-limited, never-throwing refetch to pick up a rotated key after a `kid` miss.
  async function maybeRefetchForRotation(): Promise<JwksKey[]> {
    const now = Date.now()
    if (now - lastRotationRefetchAt < refetchMinIntervalMs) return cached?.keys ?? []
    lastRotationRefetchAt = now
    try {
      return await fetchJwks()
    } catch {
      return cached?.keys ?? []
    }
  }

  function findKey(keys: JwksKey[], kid?: string): JwksKey | undefined {
    // A present `kid` must match exactly (no fallback). Absent `kid`: only safe when the set is a singleton.
    if (kid) return keys.find((k) => k.kid === kid)
    return keys.length === 1 ? keys[0] : undefined
  }

  return async function verify(token: string): Promise<VerifiedClaims | null> {
    if (isTokenOverCap(token)) return null
    const header = decodeJwtHeader(token)
    if (!header?.alg) return null
    // Reject critical extensions we do not understand (RFC 7515 §4.1.11).
    if (header.crit !== undefined) return null
    // Pin the algorithm before any network/key selection so a forged `alg` cannot drive them.
    if (!isAlgAllowed(header.alg)) return null
    const spec = algToSpec(header.alg)
    if (!spec) return null

    const decoded = decodeJwtPayloadRaw(token)
    if (!decoded?.payload) return null

    const keys = await loadJwks()
    let jwk = findKey(keys, header.kid)
    if (!jwk && header.kid) {
      // `kid` unknown — may have just rotated in; try one rate-limited refetch.
      const refreshed = await maybeRefetchForRotation()
      jwk = findKey(refreshed, header.kid)
    }
    if (!jwk) return null

    // Bind JWK to header `alg` (key family + any pinned `alg`) to block algorithm-confusion attacks.
    if (!ktyMatchesFamily(jwk.kty, spec.family)) return null
    if (jwk.alg !== undefined && jwk.alg !== header.alg) return null

    const keyObj = jwkToPublicKey(jwk)
    const v = createVerify(spec.hash)
    v.update(decoded.signedBytes)
    v.end()
    let ok: boolean
    if (spec.family === "EC") {
      ok = v.verify({ key: keyObj, dsaEncoding: "ieee-p1363" }, decoded.signature)
    } else if (spec.pss) {
      ok = v.verify({ key: keyObj, padding: constants.RSA_PKCS1_PSS_PADDING, saltLength: constants.RSA_PSS_SALTLEN_DIGEST }, decoded.signature)
    } else {
      ok = v.verify(keyObj, decoded.signature)
    }
    if (!ok) return null

    const now = Math.floor(Date.now() / 1000)
    const exp = decoded.payload.exp
    if (typeof exp === "number") {
      if (exp + clockSkewSec < now) return null
    } else if (requireExp) {
      return null
    }
    const nbf = decoded.payload.nbf
    if (typeof nbf === "number" && nbf - clockSkewSec > now) return null

    if (opts.issuer !== undefined) {
      if (decoded.payload.iss !== opts.issuer) return null
    } else if (requireIss && (typeof decoded.payload.iss !== "string" || decoded.payload.iss.length === 0)) {
      return null
    }

    if (opts.audience !== undefined) {
      const expected = Array.isArray(opts.audience) ? opts.audience : [opts.audience]
      const audList = audToStringList(decoded.payload.aud)
      if (!audList.some((a) => expected.includes(a))) return null
    } else if (requireAud) {
      if (audToStringList(decoded.payload.aud).length === 0) return null
    }
    return decoded.payload
  }
}

// Normalise `aud` (string | array | junk) to a list of string entries only.
function audToStringList(aud: unknown): string[] {
  const list = Array.isArray(aud) ? aud : aud !== undefined ? [aud] : []
  return list.filter((a): a is string => typeof a === "string")
}
