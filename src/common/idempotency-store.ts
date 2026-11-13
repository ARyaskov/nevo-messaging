import { setTimeout as sleep } from "node:timers/promises"
import { LruIdempotencyCache } from "./idempotency"
import type { IdempotencyOptions } from "./types"
import { getDefaultLogger, type NevoLogger } from "./logger"
import { getDefaultMetrics, NEVO_METRIC_NAMES } from "./metrics"

/** Marker stored while a handler runs, before the real result exists. */
export const IDEMPOTENCY_IN_PROGRESS = "\u0000nevo:idem:in-progress\u0000"

/** What `readErrorPolicy` does when a distributed read throws. */
export type StoreReadErrorPolicy = "open" | "closed"

/** Outcome of an atomic {@link IdempotencyStore.claim}. */
export interface IdempotencyClaim<T = unknown> {
  /** True when this caller reserved the key and must execute the handler. */
  acquired: boolean
  /** When `acquired` is false, the already-finished result if one exists yet. */
  existing?: T
}

/** What goes into a store: the response plus the fingerprint of the payload it answered. */
export interface IdempotencyEnvelope<T = unknown> {
  /** Payload fingerprint; absent for entries keyed by a unique envelope uuid. */
  f?: string
  v: T
}

export function idempotencyEnvelope<T>(value: T, fingerprint?: string): IdempotencyEnvelope<T> {
  return fingerprint === undefined ? { v: value } : { f: fingerprint, v: value }
}

/** Pluggable idempotency backend. */
export interface IdempotencyStore<T = unknown> {
  isEnabled(): boolean
  has(key: string): boolean | Promise<boolean>
  get(key: string): T | undefined | Promise<T | undefined>
  set(key: string, value: T): void | Promise<void>
  /** Atomically reserve `key` for execution; single winner gets `{ acquired: true }`. Optional. */
  claim?(key: string, opts?: { ttlMs?: number }): Promise<IdempotencyClaim<T>>
  /** Poll until a real (non-sentinel) result is stored or the deadline passes. */
  awaitResult?(key: string, opts?: { timeoutMs?: number; pollMs?: number }): Promise<T | undefined>
  delete?(key: string): void | Promise<void>
}

/** Synchronous adapter detection for `LruIdempotencyCache`-shaped stores. */
export function isSyncIdempotencyStore<T>(store: IdempotencyStore<T>): store is IdempotencyStore<T> & {
  has(key: string): boolean
  get(key: string): T | undefined
} {
  return store instanceof LruIdempotencyCache || (store as { __sync?: boolean }).__sync === true
}

/** Minimal Redis-ish client shape consumed by {@link RedisIdempotencyStore}. */
export interface IdempotencyRedisLike {
  get(key: string): Promise<string | null>
  set(key: string, value: string, options: { ttlMs: number; ifNotExists?: boolean }): Promise<"OK" | null | string>
  del?(key: string): Promise<number>
}

export interface RedisIdempotencyStoreOptions extends IdempotencyOptions {
  client: IdempotencyRedisLike
  keyPrefix?: string
  encode?: (value: unknown) => string
  decode?: <T>(blob: string) => T
  /** How long an in-progress claim is honoured before it can be re-claimed. Default = `ttlMs`. */
  claimTtlMs?: number
  /** On a Redis read failure: `"open"` (default) treat as a miss and proceed; `"closed"` rethrow. */
  readErrorPolicy?: StoreReadErrorPolicy
  /**
   * On a claim (SET NX) failure: `"closed"` (default) rethrow so dedup never
   * silently disables while the store is down; `"open"` executes anyway.
   * Defaults to `readErrorPolicy` when that is explicitly provided.
   */
  claimErrorPolicy?: StoreReadErrorPolicy
  logger?: NevoLogger
  metrics?: ReturnType<typeof getDefaultMetrics>
}

/** Redis-backed idempotency cache with an in-memory L1 LRU + atomic claim. */
export class RedisIdempotencyStore<T = unknown> implements IdempotencyStore<T> {
  private readonly enabled: boolean
  private readonly ttlMs: number
  private readonly claimTtlMs: number
  private readonly keyPrefix: string
  private readonly encode: (value: unknown) => string
  private readonly decode: <U>(blob: string) => U
  private readonly l1: LruIdempotencyCache<T>
  private readonly client: IdempotencyRedisLike
  private readonly readErrorPolicy: StoreReadErrorPolicy
  private readonly claimErrorPolicy: StoreReadErrorPolicy
  private readonly logger: NevoLogger
  private readonly metrics: ReturnType<typeof getDefaultMetrics>

  constructor(opts: RedisIdempotencyStoreOptions) {
    if (!opts.client) throw new Error("RedisIdempotencyStore: `client` is required")
    this.enabled = opts.enabled !== false
    this.ttlMs = opts.ttlMs ?? 5 * 60_000
    this.claimTtlMs = opts.claimTtlMs ?? this.ttlMs
    this.keyPrefix = opts.keyPrefix ?? "nevo:idem:"
    this.encode = opts.encode ?? ((v) => JSON.stringify(v))
    this.decode = (opts.decode as <U>(blob: string) => U) ?? (<U>(blob: string) => JSON.parse(blob) as U)
    this.client = opts.client
    this.readErrorPolicy = opts.readErrorPolicy ?? "open"
    // Default the claim to fail-closed: an "open" claim on a down store would
    // hand out { acquired: true } to every caller and silently disable dedup.
    this.claimErrorPolicy = opts.claimErrorPolicy ?? opts.readErrorPolicy ?? "closed"
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "idempotency.redis" })
    this.metrics = opts.metrics ?? getDefaultMetrics()
    this.l1 = new LruIdempotencyCache<T>({
      enabled: this.enabled,
      ttlMs: Math.min(this.ttlMs, 60_000),
      maxEntries: opts.maxEntries ?? 1024
    })
  }

  isEnabled(): boolean {
    return this.enabled
  }

  private k(key: string): string {
    return this.keyPrefix + key
  }

  /** Centralised read-failure handling: metric + log, then policy decides. */
  private onReadError<R>(op: string, err: unknown, failOpenValue: R, policy: StoreReadErrorPolicy = this.readErrorPolicy): R {
    this.metrics.incCounter(NEVO_METRIC_NAMES.storeErrors, { store: "idempotency", op, policy })
    this.logger.error({ event: "idem.distributed.read.failed", op, policy, err: (err as Error)?.message }, "Distributed idempotency read failed")
    if (policy === "closed") throw err instanceof Error ? err : new Error(String(err))
    return failOpenValue
  }

  /** Read the stored value, treating the in-progress sentinel as absent. Warms L1. */
  private async readReal(key: string): Promise<T | undefined> {
    const v = await this.client.get(this.k(key))
    if (v == null || v === IDEMPOTENCY_IN_PROGRESS) return undefined
    try {
      const decoded = this.decode<T>(v)
      this.l1.set(key, decoded)
      return decoded
    } catch {
      return undefined
    }
  }

  async has(key: string): Promise<boolean> {
    if (!this.enabled) return false
    if (this.l1.has(key)) return true
    try {
      const v = await this.client.get(this.k(key))
      if (v == null || v === IDEMPOTENCY_IN_PROGRESS) return false
      try {
        this.l1.set(key, this.decode<T>(v))
      } catch (decodeErr) {
        this.logger.warn(
          { event: "idem.decode_failed", err: (decodeErr as Error)?.message },
          "Could not decode a stored idempotency entry; the L1 fill was skipped"
        )
      }
      return true
    } catch (err) {
      return this.onReadError<boolean>("has", err, false)
    }
  }

  async get(key: string): Promise<T | undefined> {
    if (!this.enabled) return undefined
    const local = this.l1.get(key)
    if (local !== undefined) return local
    try {
      return await this.readReal(key)
    } catch (err) {
      return this.onReadError<T | undefined>("get", err, undefined)
    }
  }

  /** Atomic claim (`SET NX PX`): single winner gets `{ acquired: true }`; losers get the finished result if present. */
  async claim(key: string, opts?: { ttlMs?: number }): Promise<IdempotencyClaim<T>> {
    if (!this.enabled) return { acquired: true }
    // A finished result already cached locally — no need to claim.
    const local = this.l1.get(key)
    if (local !== undefined) return { acquired: false, existing: local }
    const ttlMs = opts?.ttlMs ?? this.claimTtlMs
    try {
      const res = await this.client.set(this.k(key), IDEMPOTENCY_IN_PROGRESS, { ttlMs, ifNotExists: true })
      if (res === "OK") return { acquired: true }
    } catch (err) {
      return this.onReadError<IdempotencyClaim<T>>("claim", err, { acquired: true }, this.claimErrorPolicy)
    }
    // Lost the race: something is already there. Surface a finished result if present.
    try {
      const existing = await this.readReal(key)
      return { acquired: false, existing }
    } catch (err) {
      return this.onReadError<IdempotencyClaim<T>>("claim", err, { acquired: false }, this.claimErrorPolicy)
    }
  }

  /** Poll for a finished result (skipping the in-progress sentinel) until the deadline. */
  async awaitResult(key: string, opts?: { timeoutMs?: number; pollMs?: number }): Promise<T | undefined> {
    if (!this.enabled) return undefined
    const local = this.l1.get(key)
    if (local !== undefined) return local
    const deadline = Date.now() + (opts?.timeoutMs ?? this.ttlMs)
    const pollMs = Math.max(5, opts?.pollMs ?? 25)
    while (Date.now() < deadline) {
      let v: T | undefined
      try {
        v = await this.readReal(key)
      } catch (err) {
        return this.onReadError<T | undefined>("awaitResult", err, undefined)
      }
      if (v !== undefined) return v
      await sleep(pollMs)
    }
    return undefined
  }

  async set(key: string, value: T): Promise<void> {
    if (!this.enabled) return
    this.l1.set(key, value)
    try {
      // Overwrite (no NX): the claimer replaces its own in-progress sentinel with the result.
      await this.client.set(this.k(key), this.encode(value), { ttlMs: this.ttlMs })
    } catch (err) {
      this.metrics.incCounter(NEVO_METRIC_NAMES.storeErrors, { store: "idempotency", op: "set", policy: this.readErrorPolicy })
      this.logger.warn(
        { event: "idem.distributed.write.failed", err: (err as Error)?.message },
        "Distributed idempotency write failed; entry kept in L1 only"
      )
    }
  }

  async delete(key: string): Promise<void> {
    this.l1.delete(key)
    if (this.client.del) await this.client.del(this.k(key))
  }
}

/** Adapter exposing an async store through the sync `LruIdempotencyCache` shape. */
export class StoreBackedCacheAdapter<T = unknown> {
  private readonly inner: IdempotencyStore<T>
  constructor(inner: IdempotencyStore<T>) {
    this.inner = inner
  }
  isEnabled(): boolean {
    return this.inner.isEnabled()
  }
  has(_key: string): boolean {
    return false
  }
  get(_key: string): T | undefined {
    return undefined
  }
  set(_key: string, _value: T): void {
    /* deferred */
  }
  async hasAsync(key: string): Promise<boolean> {
    return this.inner.has(key)
  }
  async getAsync(key: string): Promise<T | undefined> {
    return this.inner.get(key)
  }
  async setAsync(key: string, value: T): Promise<void> {
    await this.inner.set(key, value)
  }
}
