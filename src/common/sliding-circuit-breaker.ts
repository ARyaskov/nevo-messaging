import { createRequire } from "node:module"
import { CircuitOpenError, MessagingError } from "./errors"
import { ErrorCode } from "./error-code"
import type { CircuitBreakerOptions } from "./types"
import type { CircuitState } from "./circuit-breaker"
import { getDevToolsBus, DevToolsBus } from "./devtools"
import type { DevToolsRegistry } from "./devtools-registry"

const nodeRequire = createRequire(__filename)

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

interface CircuitData {
  state: CircuitState
  buckets: Bucket[]
  openedAt: number
  halfOpenSuccesses: number
}

export class SlidingCircuitBreakerRegistry {
  private readonly circuits = new Map<string, CircuitData>()
  private readonly enabled: boolean
  private readonly windowMs: number
  private readonly bucketMs: number
  private readonly errorRateThreshold: number
  private readonly minSampleSize: number
  private readonly resetTimeoutMs: number
  private readonly halfOpenSuccessThreshold: number
  private readonly bus: DevToolsBus
  private readonly registry: DevToolsRegistry

  constructor(opts?: SlidingCircuitOptions, deps?: { bus?: DevToolsBus; registry?: DevToolsRegistry }) {
    this.enabled = opts?.enabled === true
    this.windowMs = opts?.windowMs ?? 10_000
    this.bucketMs = opts?.bucketMs ?? 1_000
    this.errorRateThreshold = opts?.errorRateThreshold ?? 0.5
    this.minSampleSize = opts?.minSampleSize ?? 20
    this.resetTimeoutMs = opts?.resetTimeoutMs ?? 10_000
    this.halfOpenSuccessThreshold = opts?.halfOpenSuccessThreshold ?? 1
    this.bus = deps?.bus ?? getDevToolsBus()
    if (deps?.registry) {
      this.registry = deps.registry
    } else {
      const { getDevToolsRegistry } = nodeRequire("./devtools-registry") as typeof import("./devtools-registry")
      this.registry = getDevToolsRegistry()
    }
  }

  isEnabled(): boolean { return this.enabled }

  private getOrCreate(key: string): CircuitData {
    let c = this.circuits.get(key)
    if (!c) {
      c = { state: "closed", buckets: [], openedAt: 0, halfOpenSuccesses: 0 }
      this.circuits.set(key, c)
    }
    return c
  }

  private currentBucket(c: CircuitData): Bucket {
    const now = Date.now()
    const last = c.buckets.length > 0 ? c.buckets[c.buckets.length - 1] : null
    if (last && now - last.startedAt < this.bucketMs) return last
    const next: Bucket = { startedAt: now, success: 0, failure: 0 }
    c.buckets.push(next)
    const cutoff = now - this.windowMs
    while (c.buckets.length > 0 && c.buckets[0].startedAt < cutoff) c.buckets.shift()
    return next
  }

  private aggregate(c: CircuitData): { success: number; failure: number; total: number; rate: number } {
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

  private emitTransition(key: string, prev: CircuitState, next: CircuitState, agg: { failure: number; success: number }): void {
    if (prev === next) return
    const [service = "unknown", method = "unknown"] = key.split(":")
    this.registry.recordCircuit(key, next, { failures: agg.failure, successes: agg.success })
    this.bus.publish({
      ts: Date.now(),
      type: "circuit",
      service,
      method,
      extra: { key, from: prev, to: next, failures: agg.failure, successes: agg.success, mode: "sliding-window" }
    })
  }

  before(key: string): void {
    if (!this.enabled) return
    const c = this.getOrCreate(key)
    if (c.state === "open") {
      if (Date.now() - c.openedAt >= this.resetTimeoutMs) {
        const prev = c.state
        c.state = "half-open"
        c.halfOpenSuccesses = 0
        this.emitTransition(key, prev, c.state, this.aggregate(c))
      } else {
        const [service, method] = key.split(":")
        throw new CircuitOpenError(service ?? "unknown", method ?? "unknown")
      }
    }
  }

  onSuccess(key: string): void {
    if (!this.enabled) return
    const c = this.getOrCreate(key)
    this.currentBucket(c).success++
    if (c.state === "half-open") {
      c.halfOpenSuccesses++
      if (c.halfOpenSuccesses >= this.halfOpenSuccessThreshold) {
        const prev = c.state
        c.state = "closed"
        c.buckets = []
        c.halfOpenSuccesses = 0
        this.emitTransition(key, prev, c.state, { failure: 0, success: 0 })
      }
    }
  }

  onFailure(key: string, err: unknown): void {
    if (!this.enabled) return
    if (err instanceof MessagingError && err.code === ErrorCode.VALIDATION_FAILED) return
    if (err instanceof MessagingError && err.code === ErrorCode.UNAUTHORIZED) return
    const c = this.getOrCreate(key)
    this.currentBucket(c).failure++
    if (c.state === "half-open") {
      const prev = c.state
      c.state = "open"
      c.openedAt = Date.now()
      this.emitTransition(key, prev, c.state, this.aggregate(c))
      return
    }
    const agg = this.aggregate(c)
    if (agg.total >= this.minSampleSize && agg.rate >= this.errorRateThreshold) {
      const prev = c.state
      c.state = "open"
      c.openedAt = Date.now()
      this.emitTransition(key, prev, c.state, agg)
    }
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
