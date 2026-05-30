export interface AdaptiveOptions {
  enabled?: boolean
  windowMs?: number
  targetP99Ms?: number
  minRetries?: number
  maxRetries?: number
  minTimeoutMs?: number
  maxTimeoutMs?: number
}

interface Sample { ts: number; durationMs: number; ok: boolean }

// Upper bound on retained samples. Eviction is by time window *and* by this cap:
// once the ring is full the oldest slot is overwritten, so memory stays O(cap)
// regardless of throughput. 2048 recent samples is ample for a stable p99.
const MAX_SAMPLES = 2048

/**
 * Hoare-style quickselect: returns the k-th smallest element (0-indexed),
 * mutating `arr` into a partial ordering around k. O(n) on average — avoids the
 * O(n log n) full sort the old percentile() paid on every observe(). Safe to
 * call repeatedly on the same array (correct for any input ordering).
 */
function selectKth(arr: number[], k: number): number {
  let lo = 0
  let hi = arr.length - 1
  while (lo < hi) {
    const pivot = arr[(lo + hi) >> 1]
    let i = lo
    let j = hi
    while (i <= j) {
      while (arr[i] < pivot) i++
      while (arr[j] > pivot) j--
      if (i <= j) {
        const tmp = arr[i]
        arr[i] = arr[j]
        arr[j] = tmp
        i++
        j--
      }
    }
    if (k <= j) hi = j
    else if (k >= i) lo = i
    else break
  }
  return arr[k]
}

export class AdaptiveTuner {
  private readonly enabled: boolean
  private readonly windowMs: number
  private readonly target: number
  private readonly minRetries: number
  private readonly maxRetries: number
  private readonly minTimeoutMs: number
  private readonly maxTimeoutMs: number
  // Fixed-capacity ring buffer. `start` indexes the oldest live slot, `size` is
  // the number of live slots (≤ MAX_SAMPLES). Writes are O(1) — out-of-window
  // entries are skipped lazily during aggregation instead of being shifted out.
  private readonly ring: Sample[] = new Array(MAX_SAMPLES)
  private start = 0
  private size = 0
  private currentRetries: number
  private currentTimeoutMs: number

  constructor(opts?: AdaptiveOptions) {
    this.enabled = opts?.enabled === true
    this.windowMs = opts?.windowMs ?? 30_000
    this.target = opts?.targetP99Ms ?? 1000
    this.minRetries = opts?.minRetries ?? 1
    this.maxRetries = opts?.maxRetries ?? 5
    this.minTimeoutMs = opts?.minTimeoutMs ?? 500
    this.maxTimeoutMs = opts?.maxTimeoutMs ?? 30_000
    this.currentRetries = Math.max(this.minRetries, 2)
    this.currentTimeoutMs = Math.max(this.minTimeoutMs, Math.min(this.maxTimeoutMs, this.target * 4))
  }

  isEnabled(): boolean { return this.enabled }

  observe(durationMs: number, ok: boolean): void {
    if (!this.enabled) return
    const now = Date.now()
    const slot = (this.start + this.size) % MAX_SAMPLES
    this.ring[slot] = { ts: now, durationMs, ok }
    if (this.size < MAX_SAMPLES) {
      this.size++
    } else {
      // Buffer full — advancing `start` overwrites the oldest sample.
      this.start = (this.start + 1) % MAX_SAMPLES
    }
    this.recompute(now)
  }

  /** Collect in-window durations into a fresh array and count errors in one pass. */
  private window(now: number): { durations: number[]; errors: number } {
    const cutoff = now - this.windowMs
    const durations: number[] = []
    let errors = 0
    for (let i = 0; i < this.size; i++) {
      const s = this.ring[(this.start + i) % MAX_SAMPLES]
      if (s.ts < cutoff) continue
      durations.push(s.durationMs)
      if (!s.ok) errors++
    }
    return { durations, errors }
  }

  private percentile(durations: number[], p: number): number {
    if (durations.length === 0) return this.target
    const idx = Math.min(durations.length - 1, Math.floor((p / 100) * durations.length))
    return selectKth(durations, idx)
  }

  private recompute(now: number): void {
    if (this.size < 10) return
    const { durations, errors } = this.window(now)
    if (durations.length < 10) return
    const p99 = this.percentile(durations, 99)
    const err = errors / durations.length

    if (p99 > this.target * 1.5 && this.currentTimeoutMs < this.maxTimeoutMs) {
      this.currentTimeoutMs = Math.min(this.maxTimeoutMs, Math.floor(this.currentTimeoutMs * 1.5))
    } else if (p99 < this.target * 0.5 && this.currentTimeoutMs > this.minTimeoutMs) {
      this.currentTimeoutMs = Math.max(this.minTimeoutMs, Math.floor(this.currentTimeoutMs * 0.8))
    }

    if (err > 0.1 && this.currentRetries < this.maxRetries) {
      this.currentRetries++
    } else if (err < 0.01 && this.currentRetries > this.minRetries) {
      this.currentRetries--
    }
  }

  getRetries(): number { return this.currentRetries }
  getTimeoutMs(): number { return this.currentTimeoutMs }

  snapshot(): { p50: number; p95: number; p99: number; errorRate: number; sampleSize: number; retries: number; timeoutMs: number } {
    const { durations, errors } = this.window(Date.now())
    const sampleSize = durations.length
    const errorRate = sampleSize === 0 ? 0 : errors / sampleSize
    // `percentile` mutates `durations` (partial sort); reusing the same array
    // across calls is fine — selectKth is correct on any ordering.
    return {
      p50: this.percentile(durations, 50),
      p95: this.percentile(durations, 95),
      p99: this.percentile(durations, 99),
      errorRate,
      sampleSize,
      retries: this.currentRetries,
      timeoutMs: this.currentTimeoutMs
    }
  }
}
