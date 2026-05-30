import { createHash } from "node:crypto"
import { AccessControlConfig, AccessRule, MessageMeta } from "./types"
import { ErrorCode } from "./error-code"
import { MessagingError } from "./errors"
import { getDefaultLogger } from "./logger"

const JWT_CACHE_MAX = 1024
const JWT_CACHE_TTL_MS = 60_000

type Verifier = NonNullable<AccessControlConfig["jwtVerifier"]>
type VerifiedClaims = Record<string, unknown>

interface VerifyCacheEntry {
  claims: VerifiedClaims
  expiresAt: number
}

// Cache of successful token verifications. It is scoped PER verifier (so two
// verifiers with different issuers/keys can never read each other's results) and
// keyed by a SHA-256 hash of the token — never the raw bearer string — so live
// secrets are not left lying around in a process-global Map. Only positive
// results are cached, and an entry never outlives the token's own `exp`, so a
// cached claim is never served past the point a fresh verification would reject
// it. The WeakMap lets a verifier's cache be reclaimed together with the verifier.
const verifyCaches = new WeakMap<Verifier, Map<string, VerifyCacheEntry>>()

function hashToken(token: string): string {
  return createHash("sha256").update(token).digest("hex")
}

async function verifyToken(verifier: Verifier, token: string): Promise<VerifiedClaims | null> {
  let cache = verifyCaches.get(verifier)
  if (!cache) {
    cache = new Map()
    verifyCaches.set(verifier, cache)
  }

  const key = hashToken(token)
  const now = Date.now()
  const cached = cache.get(key)
  if (cached && now < cached.expiresAt) return cached.claims

  // Let verifier errors propagate so the call site fails closed; a throwing
  // verifier must never populate the cache.
  const verified = (await verifier(token)) as VerifiedClaims | null
  if (!verified) return null

  let expiresAt = now + JWT_CACHE_TTL_MS
  const exp = typeof verified["exp"] === "number" ? (verified["exp"] as number) * 1000 : undefined
  if (exp !== undefined && exp < expiresAt) expiresAt = exp

  // Skip caching an already-expired token: it would only ever produce a hit we
  // would immediately have to reject.
  if (expiresAt > now) {
    if (cache.size >= JWT_CACHE_MAX) {
      const oldest = cache.keys().next().value
      if (oldest !== undefined) cache.delete(oldest)
    }
    cache.set(key, { claims: verified, expiresAt })
  }
  return verified
}

let warnedTokenWithoutVerifier = false

function warnTokenWithoutVerifier(): void {
  if (warnedTokenWithoutVerifier) return
  warnedTokenWithoutVerifier = true
  const msg =
    "[NevoMessaging][ACL] meta.auth.token is present but no jwtVerifier is configured; the token is ignored for caller identity. Token-based identity requires a configured jwtVerifier (see docs/security.md)."
  try {
    getDefaultLogger().warn({ event: "acl.token_without_verifier" }, msg)
  } catch {
    console.warn(msg)
  }
}

export async function extractCallerService(meta?: MessageMeta, verifier?: AccessControlConfig["jwtVerifier"]): Promise<string | undefined> {
  if (verifier) {
    // A verifier is configured: the caller identity is derived ONLY from a
    // cryptographically verified token. The client-supplied, unauthenticated
    // meta.service is never trusted as identity here — trusting it would let any
    // caller impersonate any service simply by stamping meta.service.
    const token = meta?.auth?.token
    if (!token) return undefined
    const verified = await verifyToken(verifier, token)
    if (!verified) return undefined
    const identity = (verified["service"] || verified["serviceName"] || verified["svc"] || verified["sub"]) as string | undefined
    // If the caller ALSO stamped a meta.service that disagrees with the verified
    // identity, fail closed instead of silently preferring one over the other.
    if (identity && meta?.service && meta.service !== identity) {
      throw new MessagingError(ErrorCode.UNAUTHORIZED, {
        message: "meta.service does not match the verified token identity"
      })
    }
    return identity
  }

  // No verifier configured: "trusted network" mode. Identity is taken from the
  // unauthenticated meta.service. An unsigned token cannot be trusted (the classic
  // "alg:none" forgery), so it is never decoded for identity — doing so previously
  // let a caller assert any identity with a forged JWT. Warn once so the
  // misconfiguration is visible.
  if (meta?.auth?.token) warnTokenWithoutVerifier()
  return meta?.service
}

interface CompiledAcl {
  globalDefault: AccessRule[]
  byTopic: Map<string, AccessRule[]>
  byMethod: Map<string, AccessRule[]>
  byTopicMethod: Map<string, AccessRule[]>
  allowAllByDefault: boolean
  logDenied: boolean
  jwtVerifier?: AccessControlConfig["jwtVerifier"]
}

const COMPILED = new WeakMap<AccessControlConfig, CompiledAcl>()

function compile(config: AccessControlConfig): CompiledAcl {
  const cached = COMPILED.get(config)
  if (cached) return cached

  const compiled: CompiledAcl = {
    globalDefault: [],
    byTopic: new Map(),
    byMethod: new Map(),
    byTopicMethod: new Map(),
    allowAllByDefault: config.allowAllByDefault !== false,
    logDenied: config.logDenied !== false,
    jwtVerifier: config.jwtVerifier
  }

  for (const rule of config.rules ?? []) {
    const topic = rule.topic && rule.topic !== "*" ? rule.topic : null
    const method = rule.method && rule.method !== "*" ? rule.method : null
    if (topic && method) {
      const key = `${topic}::${method}`
      let arr = compiled.byTopicMethod.get(key)
      if (!arr) { arr = []; compiled.byTopicMethod.set(key, arr) }
      arr.push(rule)
    } else if (topic && !method) {
      let arr = compiled.byTopic.get(topic)
      if (!arr) { arr = []; compiled.byTopic.set(topic, arr) }
      arr.push(rule)
    } else if (!topic && method) {
      let arr = compiled.byMethod.get(method)
      if (!arr) { arr = []; compiled.byMethod.set(method, arr) }
      arr.push(rule)
    } else {
      compiled.globalDefault.push(rule)
    }
  }

  COMPILED.set(config, compiled)
  return compiled
}

function listHasValue(list: string[] | undefined, value: string | undefined): boolean {
  if (!list || list.length === 0) return false
  if (list.includes("*")) return true
  if (!value) return false
  return list.includes(value)
}

export function isAccessAllowed(config: AccessControlConfig | undefined, topic: string, method: string, callerService: string | undefined): boolean {
  if (!config) return true
  const compiled = compile(config)

  const tmKey = `${topic}::${method}`
  const candidates: AccessRule[] = []
  const tmRules = compiled.byTopicMethod.get(tmKey)
  if (tmRules) candidates.push(...tmRules)
  const tRules = compiled.byTopic.get(topic)
  if (tRules) candidates.push(...tRules)
  const mRules = compiled.byMethod.get(method)
  if (mRules) candidates.push(...mRules)
  if (compiled.globalDefault.length) candidates.push(...compiled.globalDefault)

  if (candidates.length === 0) return compiled.allowAllByDefault

  let matched = false
  let denied = false
  for (const rule of candidates) {
    matched = true
    if (listHasValue(rule.deny, callerService)) { denied = true; continue }
    if (rule.allow && rule.allow.length > 0) {
      if (listHasValue(rule.allow, callerService)) return true
      continue
    }
    return true
  }

  if (denied) return false
  return matched ? false : compiled.allowAllByDefault
}

export function logAccessDenied(config: AccessControlConfig | undefined, details: Record<string, unknown>) {
  if (config?.logDenied === false) return
  try {
    getDefaultLogger().warn({ event: "acl.denied", ...details }, "[NevoMessaging][ACL] Access denied")
  } catch {
    console.warn("[NevoMessaging][ACL] Access denied:", details)
  }
}

export function createAccessDeniedError(method: string, serviceName: string, callerService?: string) {
  return {
    code: ErrorCode.UNAUTHORIZED,
    message: "Access denied",
    details: { method, serviceName, callerService },
    service: serviceName
  }
}
