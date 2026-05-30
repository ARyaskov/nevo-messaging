import type { InboxStore } from "./inbox"
import type { IdempotencyClaim, StoreReadErrorPolicy } from "./idempotency-store"
import { getDefaultLogger, type NevoLogger } from "./logger"
import { getDefaultMetrics, NEVO_METRIC_NAMES } from "./metrics"

/** Distributed inbox store backed by Redis. */

/**
 * In-progress marker written by {@link RedisInboxStore.claim} before the real
 * result exists. NUL-wrapped so it can never collide with an encoded payload.
 */
const INBOX_IN_PROGRESS = " nevo:inbox:in-progress "

/**
 * Marker for a handler that completed but produced no value (void / null
 * return). Distinct, non-empty, and space-wrapped (like {@link INBOX_IN_PROGRESS})
 * so it can never collide with an encoded payload — `JSON.stringify` of any
 * value either escapes its quotes or yields a non-string. Stored instead of the
 * literal `"null"` (which `readReal` treats as absent), so a finished void
 * completion is reliably reported as seen — losers must not re-run the handler —
 * while `getResult` still yields `undefined` for the actual value.
 */
const INBOX_DONE_NO_VALUE = " nevo:inbox:done "

export interface InboxRedisClient {
  get(key: string): Promise<string | null>
  set(
    key: string,
    value: string,
    options: { ttlMs: number; ifNotExists?: boolean }
  ): Promise<"OK" | null | string>
  del?(key: string): Promise<number>
  exists?(key: string): Promise<number>
}

export interface RedisInboxStoreOptions {
  client: InboxRedisClient
  keyPrefix?: string
  ttlMs?: number
  /** How long an in-progress claim is honoured before it can be re-claimed. Default 60s. */
  claimTtlMs?: number
  /**
   * What to do when a Redis read (`hasSeen`/`claim`) throws.
   * - `"open"` (default): `hasSeen` returns false / `claim` acquires — the
   *   handler runs (at-least-once; may duplicate). A high-severity metric +
   *   error log are emitted either way.
   * - `"closed"`: `hasSeen` returns true (treat as already-seen) / `claim`
   *   rethrows so the message is redelivered rather than risk a duplicate.
   */
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

  private k(uuid: string): string { return this.keyPrefix + uuid }

  private recordReadError(op: string, err: unknown): void {
    this.metrics.incCounter(NEVO_METRIC_NAMES.storeErrors, { store: "inbox", op, policy: this.readErrorPolicy })
    this.logger.error(
      { event: "inbox.redis.read.failed", op, policy: this.readErrorPolicy, err: (err as Error)?.message },
      "Inbox read failed"
    )
  }

  /**
   * Read the stored result VALUE, treating every sentinel / empty marker as
   * having no value. Note this returns `undefined` for BOTH "absent" and
   * "completed with no value" — use {@link readDone} to tell those apart.
   */
  private async readReal(uuid: string): Promise<unknown | undefined> {
    const blob = await this.client.get(this.k(uuid))
    return this.decodeBlob(blob)
  }

  /** Decode a raw blob into a value, mapping sentinels / empties to `undefined`. */
  private decodeBlob(blob: string | null): unknown | undefined {
    if (
      blob === null ||
      blob === "" ||
      blob === "null" ||
      blob === INBOX_IN_PROGRESS ||
      blob === INBOX_DONE_NO_VALUE
    ) return undefined
    return this.decode(blob)
  }

  /**
   * Whether `uuid` holds a FINISHED result (real value or the done-no-value
   * sentinel) — i.e. the handler ran to completion. The bare in-progress
   * sentinel and an absent key both report `false`.
   */
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
      // fail-closed → assume seen (skip, no duplicate); fail-open → assume unseen.
      return this.readErrorPolicy === "closed"
    }
  }

  /**
   * Atomic claim: `SET uuid <sentinel> NX PX <claimTtlMs>`. The single winner
   * gets `{ acquired: true }` and must run the handler then {@link markSeen} the
   * result; losers get the finished result if present, else `{ acquired: false }`.
   */
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
      // Overwrite (no NX): the claim winner replaces its own in-progress sentinel
      // with the real result. NX here would strand pollers behind the sentinel.
      // A void/null completion stores a distinct done sentinel (NOT the literal
      // "null", which reads as absent) so losers see it as finished and skip.
      const blob = result == null ? INBOX_DONE_NO_VALUE : this.encode(result)
      await this.client.set(this.k(uuid), blob, { ttlMs: this.ttlMs })
    } catch (err) {
      this.metrics.incCounter(NEVO_METRIC_NAMES.storeErrors, { store: "inbox", op: "markSeen", policy: this.readErrorPolicy })
      this.logger.warn(
        { event: "inbox.redis.write.failed", err: (err as Error)?.message },
        "Inbox markSeen failed; handler may run twice"
      )
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

  /**
   * Whether `uuid` holds a FINISHED result — a real value OR a void/null
   * completion — as opposed to merely the in-progress claim sentinel (or being
   * absent). Lets callers distinguish "done, no value" from "still running":
   * {@link getResult} returns `undefined` for both, but only the former must
   * stop a loser from re-executing the handler.
   */
  async isDone(uuid: string): Promise<boolean> {
    try {
      return await this.readDone(uuid)
    } catch (err) {
      this.recordReadError("isDone", err)
      // fail-closed → assume done (skip, no duplicate); fail-open → assume not.
      return this.readErrorPolicy === "closed"
    }
  }
}
