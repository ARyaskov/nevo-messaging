import type { InboxStore } from "./inbox"
import type { IdempotencyClaim, StoreReadErrorPolicy } from "./idempotency-store"
import { getDefaultLogger, type NevoLogger } from "./logger"
import { getDefaultMetrics, NEVO_METRIC_NAMES } from "./metrics"

/** Distributed inbox store backed by Redis. */

/** In-progress marker written by {@link RedisInboxStore.claim} before the real result exists. */
const INBOX_IN_PROGRESS = " nevo:inbox:in-progress "

/** Marker for a handler that finished with no value (void/null), distinct from absent and from in-progress. */
const INBOX_DONE_NO_VALUE = " nevo:inbox:done "

export interface InboxRedisClient {
  get(key: string): Promise<string | null>
  set(key: string, value: string, options: { ttlMs: number; ifNotExists?: boolean }): Promise<"OK" | null | string>
  del?(key: string): Promise<number>
  exists?(key: string): Promise<number>
}

export interface RedisInboxStoreOptions {
  client: InboxRedisClient
  keyPrefix?: string
  ttlMs?: number
  /** How long an in-progress claim is honoured before it can be re-claimed. Default 60s. */
  claimTtlMs?: number
  /** On a Redis read failure: `"open"` (default) run the handler; `"closed"` treat as seen / rethrow. */
  readErrorPolicy?: StoreReadErrorPolicy
  logger?: NevoLogger
  metrics?: ReturnType<typeof getDefaultMetrics>
  encode?: (value: unknown) => string
  decode?: <T>(blob: string) => T
}

export class RedisInboxStore implements InboxStore {
  private readonly client: InboxRedisClient
  private readonly keyPrefix: string
  private readonly ttlMs: number
  private readonly claimTtlMs: number
  private readonly readErrorPolicy: StoreReadErrorPolicy
  private readonly encode: (value: unknown) => string
  private readonly decode: <T>(blob: string) => T
  private readonly logger: NevoLogger
  private readonly metrics: ReturnType<typeof getDefaultMetrics>

  constructor(opts: RedisInboxStoreOptions) {
    if (!opts.client) throw new Error("RedisInboxStore: `client` is required")
    this.client = opts.client
    this.keyPrefix = opts.keyPrefix ?? "nevo:inbox:"
    this.ttlMs = opts.ttlMs ?? 24 * 60 * 60_000
    this.claimTtlMs = opts.claimTtlMs ?? 60_000
    this.readErrorPolicy = opts.readErrorPolicy ?? "open"
    this.encode = opts.encode ?? ((v) => JSON.stringify(v ?? null))
    this.decode = (opts.decode as <T>(blob: string) => T) ?? (<T>(b: string) => JSON.parse(b) as T)
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "inbox.redis" })
    this.metrics = opts.metrics ?? getDefaultMetrics()
  }

  private k(uuid: string): string {
    return this.keyPrefix + uuid
  }

  private recordReadError(op: string, err: unknown): void {
    this.metrics.incCounter(NEVO_METRIC_NAMES.storeErrors, { store: "inbox", op, policy: this.readErrorPolicy })
    this.logger.error({ event: "inbox.redis.read.failed", op, policy: this.readErrorPolicy, err: (err as Error)?.message }, "Inbox read failed")
  }

  /** Read the stored result value, mapping every sentinel/empty marker to `undefined`. */
  private async readReal(uuid: string): Promise<unknown | undefined> {
    const blob = await this.client.get(this.k(uuid))
    return this.decodeBlob(blob)
  }

  /** Decode a raw blob into a value, mapping sentinels / empties to `undefined`. */
  private decodeBlob(blob: string | null): unknown | undefined {
    if (blob === null || blob === "" || blob === "null" || blob === INBOX_IN_PROGRESS || blob === INBOX_DONE_NO_VALUE) return undefined
    return this.decode(blob)
  }

  /** Whether `uuid` holds a finished result (real value or done-no-value sentinel). */
  private async readDone(uuid: string): Promise<boolean> {
    const blob = await this.client.get(this.k(uuid))
    if (blob === null || blob === "" || blob === INBOX_IN_PROGRESS) return false
    return true
  }

  async hasSeen(uuid: string): Promise<boolean> {
    try {
      if (this.client.exists) return (await this.client.exists(this.k(uuid))) > 0
      return (await this.client.get(this.k(uuid))) !== null
    } catch (err) {
      this.recordReadError("hasSeen", err)
      // fail-closed → assume seen; fail-open → assume unseen.
      return this.readErrorPolicy === "closed"
    }
  }

  /** Atomic claim (`SET NX PX`): single winner gets `{ acquired: true }`; losers get the finished result if present. */
  async claim(uuid: string, opts?: { ttlMs?: number }): Promise<IdempotencyClaim<unknown>> {
    const ttlMs = opts?.ttlMs ?? this.claimTtlMs
    try {
      const res = await this.client.set(this.k(uuid), INBOX_IN_PROGRESS, { ttlMs, ifNotExists: true })
      if (res === "OK") return { acquired: true }
    } catch (err) {
      this.recordReadError("claim", err)
      if (this.readErrorPolicy === "closed") throw err instanceof Error ? err : new Error(String(err))
      return { acquired: true }
    }
    try {
      const existing = await this.readReal(uuid)
      return { acquired: false, existing }
    } catch (err) {
      this.recordReadError("claim", err)
      if (this.readErrorPolicy === "closed") throw err instanceof Error ? err : new Error(String(err))
      return { acquired: false }
    }
  }

  async markSeen(uuid: string, result?: unknown): Promise<void> {
    try {
      // Overwrite (no NX): the claim winner replaces its own in-progress sentinel.
      // A void/null completion stores a distinct done sentinel so losers skip.
      const blob = result == null ? INBOX_DONE_NO_VALUE : this.encode(result)
      await this.client.set(this.k(uuid), blob, { ttlMs: this.ttlMs })
    } catch (err) {
      this.metrics.incCounter(NEVO_METRIC_NAMES.storeErrors, { store: "inbox", op: "markSeen", policy: this.readErrorPolicy })
      this.logger.warn({ event: "inbox.redis.write.failed", err: (err as Error)?.message }, "Inbox markSeen failed; handler may run twice")
    }
  }

  async getResult(uuid: string): Promise<unknown | undefined> {
    try {
      return await this.readReal(uuid)
    } catch (err) {
      this.recordReadError("getResult", err)
      return undefined
    }
  }

  /** Whether `uuid` holds a finished result (real value or void/null completion). */
  async isDone(uuid: string): Promise<boolean> {
    try {
      return await this.readDone(uuid)
    } catch (err) {
      this.recordReadError("isDone", err)
      // fail-closed → assume done; fail-open → assume not.
      return this.readErrorPolicy === "closed"
    }
  }
}
