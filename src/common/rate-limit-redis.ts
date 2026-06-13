import { MessagingError } from "./errors"
import { ErrorCode } from "./error-code"
import { RateLimiter, type RateLimiterKeyContext, type RateLimiterKeyExtractor, type RateLimiterOptions } from "./rate-limit"
import { getDefaultLogger, type NevoLogger } from "./logger"

/** Redis-backed token-bucket rate limiter shared across replicas. */

export interface RateLimitRedisClient {
  eval(args: { script: string; keys: string[]; args: string[] }): Promise<unknown>
  scriptLoad?(script: string): Promise<string>
  evalSha?(args: { sha: string; keys: string[]; args: string[] }): Promise<unknown>
  scanKeys?(prefix: string, count?: number): Promise<string[]>
  get?(key: string): Promise<string | null>
  hget?(key: string, field: string): Promise<string | null>
  hmget?(key: string, ...fields: string[]): Promise<(string | null)[]>
}

export interface RedisRateLimiterOptions extends RateLimiterOptions {
  client: RateLimitRedisClient
  keyPrefix?: string
  logger?: NevoLogger
  /** Fail-open on Redis errors (default true). When false, errors raise. */
  failOpen?: boolean
}

/** Token bucket Lua script: atomic refill + consume, returns [allowed, retryAfterMs, tokens]. */
const TOKEN_BUCKET_LUA = `
-- KEYS[1] = bucket key
-- ARGV[1] = capacity (number)
-- ARGV[2] = refillPerSec (number)
-- ARGV[3] = ttlMs
-- Returns { allowed: 0|1, retryAfterMs, tokensRemaining }

local capacity     = tonumber(ARGV[1])
local refill       = tonumber(ARGV[2])
local ttl          = tonumber(ARGV[3])
local serverTime   = redis.call('TIME')
local now          = tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000)

local data = redis.call('HMGET', KEYS[1], 'tokens', 'lastRefill')
local tokens     = tonumber(data[1])
local lastRefill = tonumber(data[2])

if tokens == nil then
  tokens = capacity
  lastRefill = now
end

-- Refill from elapsed time.
local elapsedMs = now - lastRefill
if elapsedMs > 0 then
  tokens = math.min(capacity, tokens + (elapsedMs / 1000.0) * refill)
end

if tokens < 1 then
  local need = 1 - tokens
  local retryAfterMs = refill > 0 and math.ceil((need / refill) * 1000.0) or ttl
  redis.call('HMSET', KEYS[1], 'tokens', tokens, 'lastRefill', now)
  redis.call('PEXPIRE', KEYS[1], ttl)
  return { 0, retryAfterMs, tokens }
end

tokens = tokens - 1
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'lastRefill', now)
redis.call('PEXPIRE', KEYS[1], ttl)
return { 1, 0, tokens }
`

const DEFAULT_KEY: RateLimiterKeyExtractor = (ctx) => {
  if (ctx.callerService) return `${ctx.topic}:${ctx.method}:${ctx.callerService}`
  if (ctx.tenantId) return `${ctx.topic}:${ctx.method}:tenant:${ctx.tenantId}`
  return `${ctx.topic}:${ctx.method}`
}

function buildKeyFn(keyBy: ("service" | "method" | "callerService" | "tenantId")[]): RateLimiterKeyExtractor {
  return (ctx) => {
    const parts: string[] = []
    for (const k of keyBy) {
      if (k === "service") parts.push(ctx.topic)
      else if (k === "method") parts.push(ctx.method)
      else if (k === "callerService") parts.push(ctx.callerService ?? "anon")
      else if (k === "tenantId") parts.push(ctx.tenantId ?? "no-tenant")
    }
    return parts.join(":")
  }
}

export class RedisRateLimiter {
  private readonly client: RateLimitRedisClient
  private readonly enabled: boolean
  private readonly defaultCapacity: number
  private readonly defaultRefill: number
  private readonly keyPrefix: string
  private readonly keyExtractor: RateLimiterKeyExtractor
  private readonly logger: NevoLogger
  private readonly failOpen: boolean
  private readonly ttlMs: number
  private scriptSha?: string
  private scriptLoad?: Promise<string>

  constructor(opts: RedisRateLimiterOptions) {
    if (!opts.client) throw new Error("RedisRateLimiter: `client` is required")
    this.client = opts.client
    this.enabled = opts.enabled !== false
    this.defaultCapacity = opts.capacity ?? 100
    this.defaultRefill = opts.refillPerSec ?? 50
    this.keyPrefix = opts.keyPrefix ?? "nevo:rl:"
    this.keyExtractor = opts.keyExtractor ?? (opts.keyBy ? buildKeyFn(opts.keyBy) : DEFAULT_KEY)
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "rate-limit.redis" })
    this.failOpen = opts.failOpen !== false
    this.ttlMs = Math.max(60_000, opts.idleEvictMs ?? 10 * 60_000)
  }

  isEnabled(): boolean {
    return this.enabled
  }

  private async loadScript(): Promise<string> {
    if (this.scriptSha) return this.scriptSha
    if (!this.client.scriptLoad) throw new Error("Redis client does not support SCRIPT LOAD")
    if (!this.scriptLoad) {
      this.scriptLoad = this.client
        .scriptLoad(TOKEN_BUCKET_LUA)
        .then((sha) => {
          this.scriptSha = sha
          return sha
        })
        .finally(() => {
          this.scriptLoad = undefined
        })
    }
    return this.scriptLoad
  }

  private async executeScript(key: string, args: string[]): Promise<unknown> {
    if (!this.client.evalSha || !this.client.scriptLoad) {
      return this.client.eval({ script: TOKEN_BUCKET_LUA, keys: [key], args })
    }
    let sha = await this.loadScript()
    try {
      return await this.client.evalSha({ sha, keys: [key], args })
    } catch (err) {
      if (!/NOSCRIPT/i.test((err as Error)?.message ?? "")) throw err
      this.scriptSha = undefined
      sha = await this.loadScript()
      return this.client.evalSha({ sha, keys: [key], args })
    }
  }

  async check(ctx: RateLimiterKeyContext): Promise<void> {
    if (!this.enabled) return
    const subKey = this.keyExtractor(ctx)
    if (!subKey) return
    const key = this.keyPrefix + subKey

    let result: unknown
    try {
      result = await this.executeScript(key, [String(this.defaultCapacity), String(this.defaultRefill), String(this.ttlMs)])
    } catch (err) {
      this.logger.warn({ event: "rate-limit.redis.eval.failed", err: (err as Error)?.message }, "Redis eval failed for rate limit")
      if (!this.failOpen) {
        throw new MessagingError(ErrorCode.INTERNAL, {
          message: "RedisRateLimiter eval failed",
          retryable: true
        })
      }
      return // fail-open
    }

    const arr = Array.isArray(result) ? (result as Array<number | string>) : []
    const allowed = Number(arr[0])
    const retryAfterMs = Number(arr[1])
    if (allowed === 1) return

    throw new MessagingError(ErrorCode.RATE_LIMITED, {
      message: `Rate limit exceeded for ${ctx.method}`,
      topic: ctx.topic,
      method: ctx.method,
      callerService: ctx.callerService,
      tenantId: ctx.tenantId,
      retryAfterMs,
      retryable: true
    })
  }

  /** Best-effort snapshot of bucket states via SCAN (dashboards only). */
  async snapshot(): Promise<Record<string, { tokens: number; capacity: number }>> {
    const out: Record<string, { tokens: number; capacity: number }> = {}
    if (!this.client.scanKeys || !this.client.hget) return out
    const keys = await this.client.scanKeys(this.keyPrefix)
    for (const key of keys) {
      try {
        const raw = await this.client.hget(key, "tokens")
        if (raw === null || raw === undefined) continue
        const tokens = Number(raw)
        if (!Number.isFinite(tokens)) continue
        const sub = key.slice(this.keyPrefix.length)
        out[sub] = { tokens, capacity: this.defaultCapacity }
      } catch (err) {
        this.logger.warn(
          { event: "rate-limit.redis.snapshot.failed", key, err: (err as Error)?.message },
          "Redis snapshot read failed for rate-limit bucket"
        )
      }
    }
    return out
  }

  /**
   * Compose local (per-pod) + remote (cluster-wide) limiters; remote is checked first so a
   * rejected request never spends a local token. Configure local capacity >= remote capacity.
   */
  static withLocalShield(local: RateLimiter, remote: RedisRateLimiter): { check(ctx: RateLimiterKeyContext): Promise<void>; isEnabled(): boolean } {
    return {
      isEnabled: () => local.isEnabled() || remote.isEnabled(),
      async check(ctx) {
        if (remote.isEnabled()) await remote.check(ctx)
        if (local.isEnabled()) local.check(ctx)
      }
    }
  }
}
