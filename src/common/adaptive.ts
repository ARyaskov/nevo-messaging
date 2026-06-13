export interface AdaptiveOptions {
  enabled?: boolean
  windowMs?: number
  targetP99Ms?: number
  minRetries?: number
  maxRetries?: number
  minTimeoutMs?: number
  maxTimeoutMs?: number
  /** Minimum interval between O(n) window recomputations. Default 250ms. */
  recomputeIntervalMs?: number
}

interface Sample {
  ts: number
  durationMs: number
  ok: boolean
}

const MAX_SAMPLES = 2048

/** Quickselect: returns the k-th smallest element (0-indexed), mutating `arr`. */
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
  // Fixed-capacity ring buffer; `start` is the oldest live slot, `size` the live count.
  private readonly ring: Sample[] = new Array(MAX_SAMPLES)
  private start = 0
  private size = 0
  private currentRetries: number
  private currentTimeoutMs: number
  private readonly recomputeIntervalMs: number
  private observationsSinceRecompute = 0
  private lastRecomputeAt = 0

  constructor(opts?: AdaptiveOptions) {
    this.enabled = opts?.enabled === true
    this.windowMs = opts?.windowMs ?? 30_000
    this.target = opts?.targetP99Ms ?? 1000
    this.minRetries = opts?.minRetries ?? 1
    this.maxRetries = opts?.maxRetries ?? 5
    this.minTimeoutMs = opts?.minTimeoutMs ?? 500
    this.maxTimeoutMs = opts?.maxTimeoutMs ?? 30_000
    this.recomputeIntervalMs = Math.max(10, opts?.recomputeIntervalMs ?? 250)
    this.currentRetries = Math.max(this.minRetries, 2)
    this.currentTimeoutMs = Math.max(this.minTimeoutMs, Math.min(this.maxTimeoutMs, this.target * 4))
  }

  isEnabled(): boolean {
    return this.enabled
  }

  observe(durationMs: number, ok: boolean): void {
    if (!this.enabled) return
    const now = Date.now()
    const slot = (this.start + this.size) % MAX_SAMPLES
    this.ring[slot] = { ts: now, durationMs, ok }
    if (this.size < MAX_SAMPLES) {
      this.size++
    } else {
      this.start = (this.start + 1) % MAX_SAMPLES
    }
    this.observationsSinceRecompute++
    if (
      this.size >= 10 &&
      (this.lastRecomputeAt === 0 || this.observationsSinceRecompute >= 64 || now - this.lastRecomputeAt >= this.recomputeIntervalMs)
    ) {
      this.recompute(now)
    }
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
    this.lastRecomputeAt = now
    this.observationsSinceRecompute = 0

    if (p99 > this.target * 1.5 && this.currentTimeoutMs < this.maxTimeoutMs) {
      this.currentTimeoutMs = Math.min(this.maxTimeoutMs, Math.floor(this.currentTimeoutMs * 1.5))
    } else if (p99 < this.target * 0.5 && this.currentTimeoutMs > this.minTimeoutMs) {
      this.currentTimeoutMs = Math.max(this.minTimeoutMs, Math.floor(this.currentTimeoutMs * 0.8))
    }

    // High error rates reduce amplification. Only a healthy, low-latency
    // window may cautiously restore retry capacity.
    if (err > 0.1 && this.currentRetries > this.minRetries) {
      this.currentRetries--
    } else if (err < 0.01 && p99 <= this.target && this.currentRetries < this.maxRetries) {
      this.currentRetries++
    }
  }

  getRetries(): number {
    return this.currentRetries
  }
  getTimeoutMs(): number {
    return this.currentTimeoutMs
  }

  snapshot(): { p50: number; p95: number; p99: number; errorRate: number; sampleSize: number; retries: number; timeoutMs: number } {
    const { durations, errors } = this.window(Date.now())
    const sorted = durations.slice().sort((a, b) => a - b)
    const sampleSize = durations.length
    const errorRate = sampleSize === 0 ? 0 : errors / sampleSize
    const percentile = (p: number) => {
      if (sorted.length === 0) return this.target
      const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))
      return sorted[idx]
    }
    return {
      p50: percentile(50),
      p95: percentile(95),
      p99: percentile(99),
      errorRate,
      sampleSize,
      retries: this.currentRetries,
      timeoutMs: this.currentTimeoutMs
    }
  }
}
