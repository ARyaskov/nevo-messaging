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

export class AdaptiveTuner {
  private readonly enabled: boolean
  private readonly windowMs: number
  private readonly target: number
  private readonly minRetries: number
  private readonly maxRetries: number
  private readonly minTimeoutMs: number
  private readonly maxTimeoutMs: number
  private readonly samples: Sample[] = []
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
    this.samples.push({ ts: now, durationMs, ok })
    const cutoff = now - this.windowMs
    while (this.samples.length > 0 && this.samples[0].ts < cutoff) this.samples.shift()
    this.recompute()
  }

  private percentile(p: number): number {
    if (this.samples.length === 0) return this.target
    const sorted = this.samples.map((s) => s.durationMs).sort((a, b) => a - b)
    const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))
    return sorted[idx]
  }

  private errorRate(): number {
    if (this.samples.length === 0) return 0
    let errs = 0
    for (const s of this.samples) if (!s.ok) errs++
    return errs / this.samples.length
  }

  private recompute(): void {
    if (this.samples.length < 10) return
    const p99 = this.percentile(99)
    const err = this.errorRate()

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
    return {
      p50: this.percentile(50),
      p95: this.percentile(95),
      p99: this.percentile(99),
      errorRate: this.errorRate(),
      sampleSize: this.samples.length,
      retries: this.currentRetries,
      timeoutMs: this.currentTimeoutMs
    }
  }
}
