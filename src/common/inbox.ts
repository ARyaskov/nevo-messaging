import { setTimeout as sleep } from "node:timers/promises"
import { LruIdempotencyCache } from "./idempotency"
import type { IdempotencyClaim } from "./idempotency-store"

export interface InboxStore {
  hasSeen(uuid: string): Promise<boolean>
  markSeen(uuid: string, result?: unknown): Promise<void>
  getResult(uuid: string): Promise<unknown | undefined>
  /**
   * Atomically reserve `uuid` for processing (claim-before-execute). The single
   * winner gets `{ acquired: true }` and must run the handler then
   * {@link markSeen} the result; losers get the finished result if present, else
   * `{ acquired: false }`. Optional — stores without it fall back to the racy
   * `hasSeen`/`markSeen` check-then-act.
   */
  claim?(uuid: string, opts?: { ttlMs?: number }): Promise<IdempotencyClaim<unknown>>
  /**
   * Whether `uuid` holds a FINISHED result — a real value OR a void/null
   * completion — as opposed to merely an in-progress claim (or being absent).
   * Optional: lets a store report a completed handler whose result is
   * `undefined`, which {@link getResult} alone cannot distinguish from "not yet
   * stored". Stores without it fall back to {@link hasSeen}.
   */
  isDone?(uuid: string): Promise<boolean>
}

export class InMemoryInboxStore implements InboxStore {
  private readonly cache: LruIdempotencyCache<unknown>
  constructor(opts?: { maxEntries?: number; ttlMs?: number }) {
    this.cache = new LruIdempotencyCache<unknown>({ enabled: true, maxEntries: opts?.maxEntries ?? 50_000, ttlMs: opts?.ttlMs ?? 10 * 60_000 })
  }
  async hasSeen(uuid: string): Promise<boolean> { return this.cache.has(uuid) }
  async markSeen(uuid: string, result?: unknown): Promise<void> { this.cache.set(uuid, result) }
  async getResult(uuid: string): Promise<unknown | undefined> { return this.cache.get(uuid) }
}

export interface InboxOptions {
  enabled?: boolean
  store?: InboxStore
  /** Deadline for awaiting a peer's in-flight result before executing anyway. Default 5s. */
  awaitTimeoutMs?: number
}

interface InboxLeader {
  resolve: (value: unknown) => void
  reject: (err: unknown) => void
}

export class Inbox {
  private readonly store: InboxStore
  private readonly enabled: boolean
  private readonly awaitTimeoutMs: number
  // In-process leader election so concurrent same-uuid calls in one process run
  // the handler exactly once even when the store has no atomic `claim`.
  private readonly inflight = new Map<string, Promise<unknown>>()
  private readonly leaders = new Map<string, InboxLeader>()

  constructor(opts?: InboxOptions) {
    this.enabled = opts?.enabled !== false
    this.store = opts?.store ?? new InMemoryInboxStore()
    this.awaitTimeoutMs = opts?.awaitTimeoutMs ?? 5_000
  }

  isEnabled(): boolean { return this.enabled }

  async dedupe<T>(uuid: string, handler: () => Promise<T>, opts?: { tx?: (commit: () => Promise<void>) => Promise<void> }): Promise<T> {
    if (!this.enabled) return handler()

    // In-process leader election. `openLease` is a synchronous check-and-set (no
    // `await` between its lookup and registration), so exactly one concurrent
    // caller per uuid leads; the rest await its result instead of re-running the
    // handler. When the leader FAILS, every waiter wakes at once — but the loop
    // re-runs `openLease`, which refuses to overwrite a live entry, so only ONE
    // waiter is promoted to the new leader; the rest await it (no handler herd).
    while (!this.openLease(uuid)) {
      const pending = this.inflight.get(uuid)
      if (!pending) continue // entry vanished between checks — race for leadership again
      try {
        return (await pending) as T
      } catch {
        // The leader we were awaiting failed; loop to try to lead ourselves.
      }
    }

    try {
      // Cross-process claim when the store supports it, else legacy check-then-act.
      if (typeof this.store.claim === "function") {
        const claim = await this.store.claim(uuid)
        if (!claim.acquired) {
          let existing = claim.existing
          if (existing === undefined) existing = await this.awaitResult(uuid)
          if (existing !== undefined) {
            this.settleLease(uuid, existing)
            return existing as T
          }
          // No value surfaced. Distinguish a peer that FINISHED with no value
          // (void/null result — must not re-run) from one that crashed / let the
          // claim TTL lapse before producing anything (safe to run ourselves).
          if (await this.isFinished(uuid)) {
            this.settleLease(uuid, undefined)
            return undefined as T
          }
          // Claim held by a peer that never produced a result before the deadline
          // (crash / TTL) — best-effort: run it ourselves.
        }
      } else if (await this.store.hasSeen(uuid)) {
        const existing = await this.store.getResult(uuid)
        this.settleLease(uuid, existing)
        return existing as T
      }

      const result = await handler()
      if (opts?.tx) {
        await opts.tx(async () => { await this.store.markSeen(uuid, result) })
      } else {
        await this.store.markSeen(uuid, result)
      }
      this.settleLease(uuid, result)
      return result
    } catch (err) {
      this.failLease(uuid, err)
      throw err
    }
  }

  private async awaitResult(uuid: string): Promise<unknown | undefined> {
    const deadline = Date.now() + this.awaitTimeoutMs
    while (Date.now() < deadline) {
      const v = await this.store.getResult(uuid)
      if (v !== undefined) return v
      // A handler that finished with no value (void/null) leaves `getResult`
      // undefined forever; stop waiting as soon as the entry reports done so the
      // caller settles instead of re-running. Stores without `isDone` keep
      // polling on value alone (legacy behaviour).
      if (this.store.isDone && (await this.store.isDone(uuid))) return undefined
      await sleep(25)
    }
    return undefined
  }

  /**
   * Whether a finished entry exists for `uuid` (real value or void completion).
   * Prefers the store's {@link InboxStore.isDone} probe so a `undefined`-valued
   * completion is recognised; falls back to {@link InboxStore.hasSeen}.
   */
  private async isFinished(uuid: string): Promise<boolean> {
    if (this.store.isDone) return this.store.isDone(uuid)
    return this.store.hasSeen(uuid)
  }

  /**
   * Synchronously try to become the in-process leader for `uuid`. Returns `true`
   * when this caller registered the (single) in-flight lease, `false` when one
   * already exists — the caller must then await the existing promise rather than
   * overwrite it (overwriting orphans earlier waiters and lets a herd each run
   * the handler / fire a claim). No `await`, so check + set are atomic.
   */
  private openLease(uuid: string): boolean {
    if (this.inflight.has(uuid)) return false
    let resolve!: (value: unknown) => void
    let reject!: (err: unknown) => void
    const p = new Promise<unknown>((res, rej) => { resolve = res; reject = rej })
    p.catch(() => {})
    this.inflight.set(uuid, p)
    this.leaders.set(uuid, { resolve, reject })
    return true
  }

  private settleLease(uuid: string, value: unknown): void {
    const leader = this.leaders.get(uuid)
    if (leader) leader.resolve(value)
    this.leaders.delete(uuid)
    this.inflight.delete(uuid)
  }

  private failLease(uuid: string, err: unknown): void {
    const leader = this.leaders.get(uuid)
    if (leader) leader.reject(err)
    this.leaders.delete(uuid)
    this.inflight.delete(uuid)
  }
}
