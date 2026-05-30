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

  constructor(opts?: ReplayGuardOptions) {
    this.enabled = opts?.enabled !== false
    this.windowMs = opts?.windowMs ?? 5 * 60_000
    if (opts?.sharedCache) {
      this.seen = opts.sharedCache
    } else {
      this.seen = new LruIdempotencyCache<unknown>({
        enabled: this.enabled,
        maxEntries: opts?.maxEntries ?? 50_000,
        ttlMs: this.windowMs
      })
    }
  }

  check(uuid: string | undefined, ts: number | undefined): void {
    if (!this.enabled) return
    // Fail closed: with replay protection on, a message that omits uuid or ts can't
    // be checked, so silently allowing it lets an attacker bypass the guard simply by
    // leaving the fields off. Require both and reject when absent or non-finite.
    if (!uuid) {
      throw new MessagingError(ErrorCode.REPLAY_DETECTED, {
        message: "Message missing uuid; replay protection requires a uuid"
      })
    }
    if (ts === undefined || !Number.isFinite(ts)) {
      throw new MessagingError(ErrorCode.REPLAY_DETECTED, {
        message: "Message missing ts; replay protection requires a timestamp"
      })
    }
    const now = Date.now()
    if (Math.abs(now - ts) > this.windowMs) {
      throw new MessagingError(ErrorCode.REPLAY_DETECTED, {
        message: `Message timestamp outside replay window (${this.windowMs}ms)`,
        ts
      })
    }
    if (this.seen.has(uuid)) {
      throw new MessagingError(ErrorCode.REPLAY_DETECTED, {
        message: `Duplicate message uuid within replay window`,
        uuid
      })
    }
    this.seen.set(uuid, true)
  }
}
