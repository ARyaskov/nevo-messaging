import { MessagingError } from "./errors"
import { ErrorCode } from "./error-code"
import { LruIdempotencyCache } from "./idempotency"

export interface ReplayGuardOptions {
  enabled?: boolean
  windowMs?: number
  maxEntries?: number
  sharedCache?: LruIdempotencyCache<unknown>
}

export class ReplayGuard {
  private readonly windowMs: number
  private readonly enabled: boolean
  private readonly seen: LruIdempotencyCache<unknown>
  private readonly externallyManaged: boolean

  constructor(opts?: ReplayGuardOptions) {
    this.enabled = opts?.enabled !== false
    this.windowMs = opts?.windowMs ?? 5 * 60_000
    if (opts?.sharedCache) {
      this.seen = opts.sharedCache
      this.externallyManaged = true
    } else {
      this.seen = new LruIdempotencyCache<unknown>({
        enabled: this.enabled,
        maxEntries: opts?.maxEntries ?? 50_000,
        ttlMs: this.windowMs
      })
      this.externallyManaged = false
    }
  }

  check(uuid: string | undefined, ts: number | undefined): void {
    if (!this.enabled) return
    if (!uuid) return
    const now = Date.now()
    if (ts && Math.abs(now - ts) > this.windowMs) {
      throw new MessagingError(ErrorCode.REPLAY_DETECTED, {
        message: `Message timestamp outside replay window (${this.windowMs}ms)`,
        ts
      })
    }
    if (this.seen.has(uuid)) {
      if (this.externallyManaged && this.seen.get(uuid) === undefined) {
        // marker-only entry; treat as replay
      }
      throw new MessagingError(ErrorCode.REPLAY_DETECTED, {
        message: `Duplicate message uuid within replay window`,
        uuid
      })
    }
    this.seen.set(uuid, true)
  }
}
