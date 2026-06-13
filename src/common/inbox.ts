import { setTimeout as sleep } from "node:timers/promises"
import { LruIdempotencyCache } from "./idempotency"
import type { IdempotencyClaim } from "./idempotency-store"

export interface InboxStore {
  hasSeen(uuid: string): Promise<boolean>
  markSeen(uuid: string, result?: unknown): Promise<void>
  getResult(uuid: string): Promise<unknown | undefined>
  /** Atomically reserve `uuid` for processing; single winner gets `{ acquired: true }`. Optional. */
  claim?(uuid: string, opts?: { ttlMs?: number }): Promise<IdempotencyClaim<unknown>>
  /** Whether `uuid` holds a finished result (real value or void/null completion). Optional. */
  isDone?(uuid: string): Promise<boolean>
}

export class InMemoryInboxStore implements InboxStore {
  private readonly cache: LruIdempotencyCache<unknown>
  constructor(opts?: { maxEntries?: number; ttlMs?: number }) {
    this.cache = new LruIdempotencyCache<unknown>({ enabled: true, maxEntries: opts?.maxEntries ?? 50_000, ttlMs: opts?.ttlMs ?? 10 * 60_000 })
  }
  async hasSeen(uuid: string): Promise<boolean> {
    return this.cache.has(uuid)
  }
  async markSeen(uuid: string, result?: unknown): Promise<void> {
    this.cache.set(uuid, result)
  }
  async getResult(uuid: string): Promise<unknown | undefined> {
    return this.cache.get(uuid)
  }
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
  // In-process leader election so concurrent same-uuid calls run the handler once.
  private readonly inflight = new Map<string, Promise<unknown>>()
  private readonly leaders = new Map<string, InboxLeader>()

  constructor(opts?: InboxOptions) {
    this.enabled = opts?.enabled !== false
    this.store = opts?.store ?? new InMemoryInboxStore()
    this.awaitTimeoutMs = opts?.awaitTimeoutMs ?? 5_000
  }

  isEnabled(): boolean {
    return this.enabled
  }

  async dedupe<T>(uuid: string, handler: () => Promise<T>, opts?: { tx?: (commit: () => Promise<void>) => Promise<void> }): Promise<T> {
    if (!this.enabled) return handler()

    // In-process leader election: one caller per uuid leads, the rest await it.
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
          // No value surfaced: a peer that finished void must not be re-run; one that crashed may.
          if (await this.isFinished(uuid)) {
            this.settleLease(uuid, undefined)
            return undefined as T
          }
          // Claim held by a peer that produced no result before the deadline — run it ourselves.
        }
      } else if (await this.store.hasSeen(uuid)) {
        const existing = await this.store.getResult(uuid)
        this.settleLease(uuid, existing)
        return existing as T
      }

      const result = await handler()
      if (opts?.tx) {
        await opts.tx(async () => {
          await this.store.markSeen(uuid, result)
        })
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
      // Stop waiting once the entry reports done so a void completion doesn't poll forever.
      if (this.store.isDone && (await this.store.isDone(uuid))) return undefined
      await sleep(25)
    }
    return undefined
  }

  // Whether a finished entry exists for `uuid`; prefers `isDone`, falls back to `hasSeen`.
  private async isFinished(uuid: string): Promise<boolean> {
    if (this.store.isDone) return this.store.isDone(uuid)
    return this.store.hasSeen(uuid)
  }

  // Synchronously claim in-process leadership for `uuid`; no `await`, so check + set are atomic.
  private openLease(uuid: string): boolean {
    if (this.inflight.has(uuid)) return false
    let resolve!: (value: unknown) => void
    let reject!: (err: unknown) => void
    const p = new Promise<unknown>((res, rej) => {
      resolve = res
      reject = rej
    })
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
