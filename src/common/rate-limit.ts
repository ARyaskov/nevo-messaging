import { MessagingError } from "./errors"
import { ErrorCode } from "./error-code"
import type { MessageMeta } from "./types"

export interface RateLimiterKeyContext {
  topic: string
  method: string
  callerService?: string
  tenantId?: string
  meta?: MessageMeta
}

export type RateLimiterKeyExtractor = (ctx: RateLimiterKeyContext) => string | null | undefined

export interface RateLimiterScope {
  capacity: number
  refillPerSec: number
  match?: (ctx: RateLimiterKeyContext) => boolean
  keyExtractor?: RateLimiterKeyExtractor
}

export interface RateLimiterOptions {
  enabled?: boolean
  capacity?: number
  refillPerSec?: number
  keyExtractor?: RateLimiterKeyExtractor
  keyBy?: ("service" | "method" | "callerService" | "tenantId")[]
  scopes?: RateLimiterScope[]
  maxEntries?: number
  idleEvictMs?: number
}

interface Bucket {
  tokens: number
  lastRefill: number
  capacity: number
  refillPerSec: number
  lastTouched: number
}

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

export class RateLimiter {
  private readonly enabled: boolean
  private readonly defaultCapacity: number
  private readonly defaultRefill: number
  private readonly defaultKeyExtractor: RateLimiterKeyExtractor
  private readonly scopes: RateLimiterScope[]
  private readonly buckets = new Map<string, Bucket>()
  private readonly maxEntries: number
  private readonly idleEvictMs: number
  private evictTimer?: NodeJS.Timeout

  constructor(opts?: RateLimiterOptions) {
    this.enabled = opts?.enabled !== false && (opts !== undefined)
    this.defaultCapacity = opts?.capacity ?? 100
    this.defaultRefill = opts?.refillPerSec ?? 50
    this.defaultKeyExtractor = opts?.keyExtractor ?? (opts?.keyBy ? buildKeyFn(opts.keyBy) : DEFAULT_KEY)
    this.scopes = opts?.scopes ?? []
    this.maxEntries = opts?.maxEntries ?? 100_000
    this.idleEvictMs = opts?.idleEvictMs ?? 5 * 60_000

    if (this.enabled) {
      this.evictTimer = setInterval(() => this.evictIdle(), Math.max(1000, Math.floor(this.idleEvictMs / 5)))
      if (typeof this.evictTimer.unref === "function") this.evictTimer.unref()
    }
  }

  isEnabled(): boolean { return this.enabled }

  stop(): void {
    if (this.evictTimer) clearInterval(this.evictTimer)
  }

  private pickScope(ctx: RateLimiterKeyContext): { key: string; capacity: number; refillPerSec: number } | null {
    for (const scope of this.scopes) {
      if (scope.match && !scope.match(ctx)) continue
      const keyFn = scope.keyExtractor ?? this.defaultKeyExtractor
      const key = keyFn(ctx)
      if (!key) return null
      return { key: `scope:${this.scopes.indexOf(scope)}:${key}`, capacity: scope.capacity, refillPerSec: scope.refillPerSec }
    }
    const key = this.defaultKeyExtractor(ctx)
    if (!key) return null
    return { key, capacity: this.defaultCapacity, refillPerSec: this.defaultRefill }
  }

  private getBucket(key: string, capacity: number, refillPerSec: number): Bucket {
    let b = this.buckets.get(key)
    const now = Date.now()
    if (!b) {
      b = { tokens: capacity, lastRefill: now, capacity, refillPerSec, lastTouched: now }
      this.buckets.set(key, b)
      this.maybeEvictOldest()
      return b
    }
    b.capacity = capacity
    b.refillPerSec = refillPerSec
    b.lastTouched = now
    return b
  }

  private maybeEvictOldest(): void {
    if (this.buckets.size <= this.maxEntries) return
    const firstKey = this.buckets.keys().next().value
    if (firstKey !== undefined) this.buckets.delete(firstKey)
  }

  private evictIdle(): void {
    const now = Date.now()
    for (const [k, b] of this.buckets.entries()) {
      if (now - b.lastTouched > this.idleEvictMs) this.buckets.delete(k)
    }
  }

  check(ctx: RateLimiterKeyContext): void {
    if (!this.enabled) return
    const picked = this.pickScope(ctx)
    if (!picked) return

    const b = this.getBucket(picked.key, picked.capacity, picked.refillPerSec)
    const now = Date.now()
    const elapsedSec = (now - b.lastRefill) / 1000
    if (elapsedSec > 0) {
      b.tokens = Math.min(b.capacity, b.tokens + elapsedSec * b.refillPerSec)
      b.lastRefill = now
    }
    if (b.tokens < 1) {
      const retryAfterMs = Math.ceil(((1 - b.tokens) / b.refillPerSec) * 1000)
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
    b.tokens -= 1
  }

  snapshot(): Record<string, { tokens: number; capacity: number }> {
    const out: Record<string, { tokens: number; capacity: number }> = {}
    for (const [k, b] of this.buckets.entries()) out[k] = { tokens: Math.floor(b.tokens), capacity: b.capacity }
    return out
  }
}

export function resolveRateLimiter(opts?: RateLimiterOptions | RateLimiter): RateLimiter {
  if (opts instanceof RateLimiter) return opts
  return new RateLimiter(opts)
}
