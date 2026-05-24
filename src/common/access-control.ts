import { AccessControlConfig, AccessRule, MessageMeta } from "./types"
import { ErrorCode } from "./error-code"
import { getDefaultLogger } from "./logger"

const JWT_CACHE_MAX = 1024
const JWT_CACHE_TTL_MS = 60_000

interface JwtCacheEntry {
  payload: Record<string, unknown> | null
  expiresAt: number
}

const jwtCache = new Map<string, JwtCacheEntry>()

function decodeJwtPayload(token: string): Record<string, unknown> | null {
  const cached = jwtCache.get(token)
  const now = performance.now()
  if (cached && now < cached.expiresAt) {
    return cached.payload
  }

  const parts = token.split(".")
  if (parts.length < 2) {
    jwtCache.set(token, { payload: null, expiresAt: now + JWT_CACHE_TTL_MS })
    return null
  }

  let payload: Record<string, unknown> | null = null
  try {
    const segment = parts[1].replaceAll("-", "+").replaceAll("_", "/")
    const padded = segment.padEnd(segment.length + ((4 - (segment.length % 4)) % 4), "=")
    const json = Buffer.from(padded, "base64").toString("utf8")
    payload = JSON.parse(json)
  } catch {
    payload = null
  }

  if (jwtCache.size >= JWT_CACHE_MAX) {
    const oldest = jwtCache.keys().next().value
    if (oldest !== undefined) jwtCache.delete(oldest)
  }
  jwtCache.set(token, { payload, expiresAt: now + JWT_CACHE_TTL_MS })
  return payload
}

export async function extractCallerService(meta?: MessageMeta, verifier?: AccessControlConfig["jwtVerifier"]): Promise<string | undefined> {
  if (meta?.service) return meta.service

  const token = meta?.auth?.token
  if (!token) return undefined

  if (verifier) {
    const verified = await verifier(token)
    if (!verified) return undefined
    return (verified["service"] || verified["serviceName"] || verified["svc"] || verified["sub"]) as string | undefined
  }

  const payload = decodeJwtPayload(token)
  if (!payload) return undefined
  return (payload["serviceName"] || payload["service"] || payload["svc"] || payload["sub"]) as string | undefined
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
