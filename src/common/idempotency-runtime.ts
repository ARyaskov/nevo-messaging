import { LruIdempotencyCache } from "./idempotency"
import type { IdempotencyStore } from "./idempotency-store"
import type { IdempotencyOptions } from "./types"
import { getDefaultLogger, type NevoLogger } from "./logger"

/**
 * Shared two-tier idempotency runtime used by BOTH server pipelines
 * (`BaseMessageController` and the live `signal-router.utils` path) so there is
 * a single source of truth for the cross-cutting dedup logic.
 *
 * Three layers, checked in order on {@link begin}:
 *  1. **L1** — the in-process LRU (sub-ms, per-replica).
 *  2. **In-process leader election** — concurrent calls for the same key in one
 *     process await the first caller instead of all executing (the local half of
 *     claim-before-execute; works even with no distributed store).
 *  3. **Distributed claim** — `SET NX` reserve across replicas, then poll for the
 *     winner's result (the cross-replica half).
 *
 * On success the caller {@link commit}s (L1 write + awaited write-through);
 * on failure / early-return it {@link release}s the claim so it isn't stranded.
 */

export type IdempotencyBegin<T> =
  | { status: "hit"; value: T }
  | { status: "execute" }

export interface TwoTierIdempotencyOptions<T> {
  /** Provide an existing L1 cache (e.g. to share with a subclass field). */
  l1?: LruIdempotencyCache<T>
  /** Otherwise construct one from these options. */
  l1Options?: IdempotencyOptions
  /** Optional distributed L2 (Redis, …). */
  distributed?: IdempotencyStore<T>
  logger?: NevoLogger
  /** Deadline for polling a peer replica's in-flight result. Default 5s. */
  awaitTimeoutMs?: number
}

interface Leader<T> {
  resolve: (value: T) => void
  reject: (err: unknown) => void
}

export class TwoTierIdempotency<T> {
  private readonly l1: LruIdempotencyCache<T>
  private readonly distributed?: IdempotencyStore<T>
  private readonly logger: NevoLogger
  private readonly awaitTimeoutMs: number
  private readonly inflight = new Map<string, Promise<T>>()
  private readonly leaders = new Map<string, Leader<T>>()

  constructor(opts?: TwoTierIdempotencyOptions<T>) {
    this.l1 = opts?.l1 ?? new LruIdempotencyCache<T>(opts?.l1Options)
    this.distributed = opts?.distributed
    this.logger = (opts?.logger ?? getDefaultLogger()).child({ component: "idempotency" })
    this.awaitTimeoutMs = opts?.awaitTimeoutMs ?? 5_000
  }

  /** The L1 cache, for callers that want to keep a field pointing at it. */
  get local(): LruIdempotencyCache<T> { return this.l1 }

  isEnabled(): boolean {
    return this.l1.isEnabled() || (this.distributed?.isEnabled() ?? false)
  }

  /**
   * Either return an already-computed result (`hit`) or signal that THIS caller
   * holds the claim and must run the handler (`execute`). When `execute` is
   * returned the caller MUST eventually call {@link commit} or {@link release}.
   */
  async begin(key: string): Promise<IdempotencyBegin<T>> {
    if (!key || !this.isEnabled()) return { status: "execute" }

    // 1. L1.
    if (this.l1.isEnabled()) {
      const v = this.l1.get(key)
      if (v !== undefined) return { status: "hit", value: v }
    }

    // 2. In-process leader election. `openLease` is a synchronous check-and-set
    //    (no `await` between the lookup and the registration), so exactly one
    //    concurrent caller per key becomes the leader; the rest await its
    //    result. If the leader FAILS (releases without committing) every waiter
    //    wakes at once — but they must NOT all stampede into a fresh claim. The
    //    loop re-runs `openLease`, which refuses to overwrite a live entry, so
    //    only ONE waiter is promoted to the new leader and the rest await it (or
    //    re-claim sequentially on repeated failures).
    while (!this.openLease(key)) {
      const pending = this.inflight.get(key)
      if (!pending) continue // entry vanished between checks — race for leadership again
      try {
        return { status: "hit", value: await pending }
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
              return { status: "hit", value: existing }
            }
            // Claim held by a peer that never produced a result before the
            // deadline (crash / TTL) — best-effort: execute it ourselves.
          }
        } else {
          // Store without atomic claim: legacy read-through (races, but better
          // than nothing for custom backends).
          const remote = await this.distributed.get(key)
          if (remote !== undefined) {
            if (this.l1.isEnabled()) this.l1.set(key, remote)
            this.settleLease(key, remote)
            return { status: "hit", value: remote }
          }
        }
      } catch (err) {
        // readErrorPolicy="closed" surfaces here — release the lease and let the
        // caller fail the request rather than risk a duplicate execution.
        this.failLease(key, err)
        throw err
      }
    }

    return { status: "execute" }
  }

  /**
   * Persist `value` for `key`: L1 write, then an AWAITED distributed
   * write-through (closing the window where a peer re-executes before the result
   * is stored). In-process waiters are unblocked immediately. Never throws on a
   * distributed write failure — the L1 absorbed it and a metric/log was emitted.
   */
  async commit(key: string, value: T): Promise<void> {
    if (!key) return
    if (this.l1.isEnabled()) this.l1.set(key, value)
    this.settleLease(key, value)
    if (this.distributed?.isEnabled()) {
      try {
        await this.distributed.set(key, value)
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

  /**
   * Synchronously try to become the in-process leader for `key`. Returns `true`
   * when this caller registered the (single) in-flight lease, `false` when one
   * already exists — in which case the caller must await the existing promise
   * rather than overwrite it (overwriting would orphan earlier waiters and let a
   * herd of callers each fire a distributed claim). Contains no `await`, so the
   * check + set are atomic with respect to other microtasks.
   */
  private openLease(key: string): boolean {
    if (this.inflight.has(key)) return false
    let resolve!: (value: T) => void
    let reject!: (err: unknown) => void
    const p = new Promise<T>((res, rej) => { resolve = res; reject = rej })
    // Pre-attach a no-op catch so a rejection (release) is never an unhandled one.
    p.catch(() => {})
    this.inflight.set(key, p)
    this.leaders.set(key, { resolve, reject })
    return true
  }

  private settleLease(key: string, value: T): void {
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
