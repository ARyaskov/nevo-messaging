import { setTimeout as sleep } from "node:timers/promises"
import { LruIdempotencyCache } from "./idempotency"
import { IDEMPOTENCY_IN_PROGRESS, type IdempotencyClaim, type IdempotencyStore, type StoreReadErrorPolicy } from "./idempotency-store"
import type { IdempotencyOptions } from "./types"
import { getDefaultLogger, type NevoLogger } from "./logger"
import { getDefaultMetrics, NEVO_METRIC_NAMES } from "./metrics"

interface EtcdGetBuilder {
  string(): Promise<string | null>
}

interface EtcdPutBuilder extends PromiseLike<unknown> {
  value(value: string | Buffer | number): EtcdPutBuilder
}

interface EtcdDeleteBuilder {
  key(key: string | Buffer): PromiseLike<unknown>
}

interface EtcdLease {
  put(key: string | Buffer): EtcdPutBuilder
  revoke(): Promise<void>
}

interface EtcdTxn {
  then(...operations: unknown[]): EtcdTxn
  commit(): Promise<{ succeeded: boolean }>
}

/**
 * Structural subset of the `etcd3` client used by {@link EtcdIdempotencyStore}.
 * It maps directly to the v3 KV transaction and lease APIs supported by etcd
 * 3.6.x, while allowing applications to wrap another v3 client if preferred.
 */
export interface IdempotencyEtcdClient {
  get(key: string | Buffer): EtcdGetBuilder
  put(key: string | Buffer): EtcdPutBuilder
  delete(): EtcdDeleteBuilder
  lease(ttlSeconds: number, options?: { autoKeepAlive?: boolean }): EtcdLease
  if(key: string | Buffer, column: "Create", comparator: "==", value: number): EtcdTxn
}

export interface EtcdIdempotencyStoreOptions extends IdempotencyOptions {
  client: IdempotencyEtcdClient
  keyPrefix?: string
  encode?: (value: unknown) => string
  decode?: <T>(blob: string) => T
  /** How long an in-progress claim is retained. Default = `ttlMs`. */
  claimTtlMs?: number
  /** On an etcd read/transaction failure: `"open"` (default) or `"closed"`. */
  readErrorPolicy?: StoreReadErrorPolicy
  logger?: NevoLogger
  metrics?: ReturnType<typeof getDefaultMetrics>
}

/**
 * etcd v3-backed idempotency store for multi-pod deployments.
 *
 * Claims use a linearizable transaction comparing the key's create revision
 * with zero. Both claims and completed values are attached to server-side
 * leases, so expiry is based on etcd's clock rather than pod clocks.
 */
export class EtcdIdempotencyStore<T = unknown> implements IdempotencyStore<T> {
  private readonly enabled: boolean
  private readonly ttlMs: number
  private readonly claimTtlMs: number
  private readonly keyPrefix: string
  private readonly encode: (value: unknown) => string
  private readonly decode: <U>(blob: string) => U
  private readonly l1: LruIdempotencyCache<T>
  private readonly client: IdempotencyEtcdClient
  private readonly readErrorPolicy: StoreReadErrorPolicy
  private readonly logger: NevoLogger
  private readonly metrics: ReturnType<typeof getDefaultMetrics>

  constructor(opts: EtcdIdempotencyStoreOptions) {
    if (!opts.client) throw new Error("EtcdIdempotencyStore: `client` is required")
    this.enabled = opts.enabled !== false
    this.ttlMs = opts.ttlMs ?? 5 * 60_000
    this.claimTtlMs = opts.claimTtlMs ?? this.ttlMs
    this.keyPrefix = opts.keyPrefix ?? "nevo/idem/"
    this.encode = opts.encode ?? ((value) => JSON.stringify(value))
    this.decode = (opts.decode as <U>(blob: string) => U) ?? (<U>(blob: string) => JSON.parse(blob) as U)
    this.client = opts.client
    this.readErrorPolicy = opts.readErrorPolicy ?? "open"
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "idempotency.etcd" })
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

  private leaseFor(ttlMs: number): EtcdLease {
    return this.client.lease(Math.max(1, Math.ceil(ttlMs / 1000)), { autoKeepAlive: false })
  }

  private onReadError<R>(op: string, err: unknown, failOpenValue: R): R {
    this.metrics.incCounter(NEVO_METRIC_NAMES.storeErrors, {
      store: "idempotency.etcd",
      op,
      policy: this.readErrorPolicy
    })
    this.logger.error(
      { event: "idem.etcd.read.failed", op, policy: this.readErrorPolicy, err: (err as Error)?.message },
      "etcd idempotency operation failed"
    )
    if (this.readErrorPolicy === "closed") throw err instanceof Error ? err : new Error(String(err))
    return failOpenValue
  }

  private async readReal(key: string): Promise<T | undefined> {
    const blob = await this.client.get(this.k(key)).string()
    if (blob === null || blob === IDEMPOTENCY_IN_PROGRESS) return undefined
    try {
      const decoded = this.decode<T>(blob)
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
      const value = await this.readReal(key)
      return value !== undefined
    } catch (err) {
      return this.onReadError("has", err, false)
    }
  }

  async get(key: string): Promise<T | undefined> {
    if (!this.enabled) return undefined
    const local = this.l1.get(key)
    if (local !== undefined) return local
    try {
      return await this.readReal(key)
    } catch (err) {
      return this.onReadError("get", err, undefined)
    }
  }

  async claim(key: string, opts?: { ttlMs?: number }): Promise<IdempotencyClaim<T>> {
    if (!this.enabled) return { acquired: true }
    const local = this.l1.get(key)
    if (local !== undefined) return { acquired: false, existing: local }

    const storageKey = this.k(key)
    const lease = this.leaseFor(opts?.ttlMs ?? this.claimTtlMs)
    try {
      const put = lease.put(storageKey).value(IDEMPOTENCY_IN_PROGRESS)
      const result = await this.client.if(storageKey, "Create", "==", 0).then(put).commit()
      if (result.succeeded) return { acquired: true }
      await lease.revoke().catch(() => {})
    } catch (err) {
      await lease.revoke().catch(() => {})
      return this.onReadError<IdempotencyClaim<T>>("claim", err, { acquired: true })
    }

    try {
      return { acquired: false, existing: await this.readReal(key) }
    } catch (err) {
      return this.onReadError<IdempotencyClaim<T>>("claim", err, { acquired: false })
    }
  }

  async awaitResult(key: string, opts?: { timeoutMs?: number; pollMs?: number }): Promise<T | undefined> {
    if (!this.enabled) return undefined
    const local = this.l1.get(key)
    if (local !== undefined) return local
    const deadline = Date.now() + (opts?.timeoutMs ?? this.ttlMs)
    const pollMs = Math.max(5, opts?.pollMs ?? 25)
    while (Date.now() < deadline) {
      try {
        const value = await this.readReal(key)
        if (value !== undefined) return value
      } catch (err) {
        return this.onReadError<T | undefined>("awaitResult", err, undefined)
      }
      await sleep(pollMs)
    }
    return undefined
  }

  async set(key: string, value: T): Promise<void> {
    if (!this.enabled) return
    this.l1.set(key, value)
    const lease = this.leaseFor(this.ttlMs)
    try {
      await lease.put(this.k(key)).value(this.encode(value))
    } catch (err) {
      await lease.revoke().catch(() => {})
      this.metrics.incCounter(NEVO_METRIC_NAMES.storeErrors, {
        store: "idempotency.etcd",
        op: "set",
        policy: this.readErrorPolicy
      })
      this.logger.warn({ event: "idem.etcd.write.failed", err: (err as Error)?.message }, "etcd idempotency write failed; entry kept in L1 only")
    }
  }

  async delete(key: string): Promise<void> {
    this.l1.delete(key)
    await this.client.delete().key(this.k(key))
  }
}
