export interface BackpressureOptions {
  maxInflight?: number
  highWatermark?: number
  lowWatermark?: number
}

export interface PausableSubscription {
  unsubscribe(): Promise<void>
  pause(): void
  resume(): void
  isPaused(): boolean
}

export class BackpressureLimiter {
  private inflight = 0
  private paused = false
  private readonly max: number
  private readonly high: number
  private readonly low: number
  private readonly onPause: () => void
  private readonly onResume: () => void

  constructor(opts: BackpressureOptions, callbacks: { onPause: () => void; onResume: () => void }) {
    this.max = opts.maxInflight ?? 64
    this.high = opts.highWatermark ?? Math.floor(this.max * 0.9)
    this.low = opts.lowWatermark ?? Math.floor(this.max * 0.5)
    this.onPause = callbacks.onPause
    this.onResume = callbacks.onResume
  }

  begin(): boolean {
    if (this.inflight >= this.max) return false
    this.inflight++
    if (this.inflight >= this.high && !this.paused) {
      this.paused = true
      this.onPause()
    }
    return true
  }

  end(): void {
    this.inflight = Math.max(0, this.inflight - 1)
    if (this.inflight <= this.low && this.paused) {
      this.paused = false
      this.onResume()
    }
  }

  getInflight(): number { return this.inflight }
  isPaused(): boolean { return this.paused }
}
