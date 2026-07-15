import type { CircuitBreakerOptions } from "./types"
import { CircuitRegistryBase, type BaseCircuitData, type CircuitRegistryDeps, type CircuitState } from "./circuit-breaker"

export interface SlidingCircuitOptions extends CircuitBreakerOptions {
  windowMs?: number
  bucketMs?: number
  errorRateThreshold?: number
  minSampleSize?: number
}

interface Bucket {
  startedAt: number
  success: number
  failure: number
}

interface SlidingCircuitData extends BaseCircuitData {
  buckets: Bucket[]
  halfOpenSuccesses: number
}

/** Error-rate circuit breaker over a sliding time window. */
export class SlidingCircuitBreakerRegistry extends CircuitRegistryBase<SlidingCircuitData> {
  private readonly windowMs: number
  private readonly bucketMs: number
  private readonly errorRateThreshold: number
  private readonly minSampleSize: number

  constructor(opts?: SlidingCircuitOptions, deps?: CircuitRegistryDeps) {
    super(opts?.enabled === true, opts?.resetTimeoutMs ?? 10_000, opts?.halfOpenSuccessThreshold ?? 1, opts?.maxKeys ?? 10_000, deps)
    this.windowMs = opts?.windowMs ?? 10_000
    this.bucketMs = opts?.bucketMs ?? 1_000
    this.errorRateThreshold = opts?.errorRateThreshold ?? 0.5
    this.minSampleSize = opts?.minSampleSize ?? 20
  }

  private currentBucket(c: SlidingCircuitData): Bucket {
    const now = Date.now()
    const last = c.buckets.length > 0 ? c.buckets[c.buckets.length - 1] : null
    if (last && now - last.startedAt < this.bucketMs) return last
    const next: Bucket = { startedAt: now, success: 0, failure: 0 }
    c.buckets.push(next)
    const cutoff = now - this.windowMs
    while (c.buckets.length > 0 && c.buckets[0].startedAt < cutoff) c.buckets.shift()
    return next
  }

  private aggregate(c: SlidingCircuitData): { success: number; failure: number; total: number; rate: number } {
    const cutoff = Date.now() - this.windowMs
    let success = 0
    let failure = 0
    for (const b of c.buckets) {
      if (b.startedAt < cutoff) continue
      success += b.success
      failure += b.failure
    }
    const total = success + failure
    return { success, failure, total, rate: total > 0 ? failure / total : 0 }
  }

  protected newCircuit(): SlidingCircuitData {
    return { state: "closed", buckets: [], openedAt: 0, halfOpenInFlight: false, halfOpenProbeAt: 0, halfOpenSuccesses: 0 }
  }

  protected transitionStats(c: SlidingCircuitData): { failures: number; successes: number; extra?: Record<string, unknown> } {
    const agg = this.aggregate(c)
    return { failures: agg.failure, successes: agg.success, extra: { mode: "sliding-window" } }
  }

  protected recordOutcome(c: SlidingCircuitData, ok: boolean): void {
    const bucket = this.currentBucket(c)
    if (ok) bucket.success++
    else bucket.failure++
  }

  protected shouldOpen(c: SlidingCircuitData): boolean {
    const agg = this.aggregate(c)
    return agg.total >= this.minSampleSize && agg.rate >= this.errorRateThreshold
  }

  protected onEnterHalfOpen(c: SlidingCircuitData): void {
    c.halfOpenSuccesses = 0
  }

  protected onProbeSuccess(c: SlidingCircuitData): boolean {
    c.halfOpenSuccesses++
    return c.halfOpenSuccesses >= this.halfOpenSuccessThreshold
  }

  protected resetOnClose(c: SlidingCircuitData): void {
    c.buckets = []
    c.halfOpenSuccesses = 0
  }

  snapshot(): Record<string, { state: CircuitState; errorRate: number; sampleSize: number }> {
    const out: Record<string, { state: CircuitState; errorRate: number; sampleSize: number }> = {}
    for (const [k, c] of this.circuits.entries()) {
      const agg = this.aggregate(c)
      out[k] = { state: c.state, errorRate: agg.rate, sampleSize: agg.total }
    }
    return out
  }
}
