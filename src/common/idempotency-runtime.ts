import { LruIdempotencyCache } from "./idempotency"
import { idempotencyEnvelope, type IdempotencyEnvelope, type IdempotencyStore } from "./idempotency-store"
import type { IdempotencyOptions } from "./types"
import { getDefaultLogger, type NevoLogger } from "./logger"

/** Shared two-tier (L1 LRU + in-process leader election + distributed claim) idempotency runtime. */
export type IdempotencyBegin<T> =
  | { status: "hit"; value: T }
  /** The key already holds a response produced for a different payload. */
  | { status: "conflict" }
  | { status: "execute" }

export interface TwoTierIdempotencyOptions<T> {
  /** Provide an existing L1 cache (e.g. to share with a subclass field). */
  l1?: LruIdempotencyCache<IdempotencyEnvelope<T>>
  /** Otherwise construct one from these options. */
  l1Options?: IdempotencyOptions
  /** Optional distributed L2 (Redis, …). */
  distributed?: IdempotencyStore<IdempotencyEnvelope<T>>
  logger?: NevoLogger
  /** Deadline for polling a peer replica's in-flight result. Default 5s. */
  awaitTimeoutMs?: number
}

interface Leader<T> {
  resolve: (value: IdempotencyEnvelope<T>) => void
  reject: (err: unknown) => void
}

function fingerprintConflicts(stored: string | undefined, incoming: string | undefined): boolean {
  return stored !== undefined && incoming !== undefined && stored !== incoming
}

export class TwoTierIdempotency<T> {
  private readonly l1: LruIdempotencyCache<IdempotencyEnvelope<T>>
  private readonly distributed?: IdempotencyStore<IdempotencyEnvelope<T>>
  private readonly logger: NevoLogger
  private readonly awaitTimeoutMs: number
  private readonly inflight = new Map<string, Promise<IdempotencyEnvelope<T>>>()
  private readonly leaders = new Map<string, Leader<T>>()

  constructor(opts?: TwoTierIdempotencyOptions<T>) {
    this.l1 = opts?.l1 ?? new LruIdempotencyCache<IdempotencyEnvelope<T>>(opts?.l1Options)
    this.distributed = opts?.distributed
    this.logger = (opts?.logger ?? getDefaultLogger()).child({ component: "idempotency" })
    this.awaitTimeoutMs = opts?.awaitTimeoutMs ?? 5_000
  }

  /** The L1 cache, for callers that want to keep a field pointing at it. */
  get local(): LruIdempotencyCache<IdempotencyEnvelope<T>> {
    return this.l1
  }

  isEnabled(): boolean {
    return this.l1.isEnabled() || (this.distributed?.isEnabled() ?? false)
  }

  /** On `execute` the caller must {@link commit} or {@link release}. */
  async begin(key: string, fingerprint?: string): Promise<IdempotencyBegin<T>> {
    if (!key || !this.isEnabled()) return { status: "execute" }

    const settle = (entry: IdempotencyEnvelope<T>): IdempotencyBegin<T> =>
      fingerprintConflicts(entry.f, fingerprint) ? { status: "conflict" } : { status: "hit", value: entry.v }

    // 1. L1.
    if (this.l1.isEnabled()) {
      const entry = this.l1.get(key)
      if (entry !== undefined) return settle(entry)
    }

    // 2. In-process leader election: one caller per key leads, the rest await it.
    while (!this.openLease(key)) {
      const pending = this.inflight.get(key)
      if (!pending) continue // entry vanished between checks — race for leadership again
      try {
        return settle(await pending)
      } catch {
        // The leader we were awaiting failed; loop to try to lead ourselves.
      }
    }

    // 3. Distributed claim (cross-replica). Only the in-process leader gets here.
    if (this.distributed?.isEnabled()) {
      try {
        if (typeof this.distributed.claim === "function") {
          const claim = await this.distributed.claim(key)
          if (!claim.acquired) {
            let existing = claim.existing
            if (existing === undefined && typeof this.distributed.awaitResult === "function") {
              existing = await this.distributed.awaitResult(key, { timeoutMs: this.awaitTimeoutMs })
            }
            if (existing !== undefined) {
              if (this.l1.isEnabled()) this.l1.set(key, existing)
              this.settleLease(key, existing)
              return settle(existing)
            }
            // Claim held by a peer that produced no result before the deadline — execute it ourselves.
          }
        } else {
          // Store without atomic claim: legacy read-through (races).
          const remote = await this.distributed.get(key)
          if (remote !== undefined) {
            if (this.l1.isEnabled()) this.l1.set(key, remote)
            this.settleLease(key, remote)
            return settle(remote)
          }
        }
      } catch (err) {
        // readErrorPolicy="closed" surfaces here — release the lease and fail the request.
        this.failLease(key, err)
        throw err
      }
    }

    return { status: "execute" }
  }

  /** Persist `value` for `key` (L1 + awaited distributed write-through); never throws on a write failure. */
  async commit(key: string, value: T, fingerprint?: string): Promise<void> {
    if (!key) return
    const entry = idempotencyEnvelope(value, fingerprint)
    if (this.l1.isEnabled()) this.l1.set(key, entry)
    this.settleLease(key, entry)
    if (this.distributed?.isEnabled()) {
      try {
        await this.distributed.set(key, entry)
      } catch (err) {
        this.logger.warn(
          { event: "idem.commit.write.failed", err: (err as Error)?.message },
          "Distributed idempotency write failed; result kept in L1 only"
        )
      }
    }
  }

  /** Drop the claim without storing a result (handler error / early return). */
  async release(key: string, err?: unknown): Promise<void> {
    if (!key) return
    this.failLease(key, err ?? new Error("idempotency lease released"))
    if (this.distributed?.isEnabled() && typeof this.distributed.delete === "function") {
      try {
        await this.distributed.delete(key)
      } catch {
        // The sentinel carries a TTL, so a failed delete self-heals.
      }
    }
  }

  // Synchronously claim in-process leadership for `key`; no `await`, so check + set are atomic.
  private openLease(key: string): boolean {
    if (this.inflight.has(key)) return false
    let resolve!: (value: IdempotencyEnvelope<T>) => void
    let reject!: (err: unknown) => void
    const p = new Promise<IdempotencyEnvelope<T>>((res, rej) => {
      resolve = res
      reject = rej
    })
    // Pre-attach a no-op catch so a rejection (release) is never an unhandled one.
    p.catch(() => {})
    this.inflight.set(key, p)
    this.leaders.set(key, { resolve, reject })
    return true
  }

  private settleLease(key: string, value: IdempotencyEnvelope<T>): void {
    const leader = this.leaders.get(key)
    if (leader) leader.resolve(value)
    this.leaders.delete(key)
    this.inflight.delete(key)
  }

  private failLease(key: string, err: unknown): void {
    const leader = this.leaders.get(key)
    if (leader) leader.reject(err)
    this.leaders.delete(key)
    this.inflight.delete(key)
  }
}
