import { CircuitOpenError, MessagingError } from "./errors"
import { ErrorCode } from "./error-code"
import type { CircuitBreakerOptions } from "./types"
import { getDevToolsBus, DevToolsBus } from "./devtools"
import { getDevToolsRegistry, type DevToolsRegistry } from "./devtools-registry"

export type CircuitState = "closed" | "open" | "half-open"

export interface ResolvedCircuitOptions {
  enabled: boolean
  failureThreshold: number
  resetTimeoutMs: number
  halfOpenSuccessThreshold: number
  maxKeys: number
}

export function resolveCircuitOptions(opts?: CircuitBreakerOptions): ResolvedCircuitOptions {
  return {
    enabled: opts?.enabled === true,
    failureThreshold: opts?.failureThreshold ?? 5,
    resetTimeoutMs: opts?.resetTimeoutMs ?? 10000,
    halfOpenSuccessThreshold: opts?.halfOpenSuccessThreshold ?? 1,
    maxKeys: opts?.maxKeys ?? 10_000
  }
}

export interface BaseCircuitData {
  state: CircuitState
  openedAt: number
  halfOpenInFlight: boolean
  halfOpenProbeAt: number
}

export interface CircuitRegistryDeps {
  bus?: DevToolsBus
  registry?: DevToolsRegistry
}

/**
 * Shared open/half-open state machine for circuit breakers. Subclasses supply
 * the failure-accounting strategy (consecutive counter vs sliding window).
 */
export abstract class CircuitRegistryBase<D extends BaseCircuitData> {
  protected readonly circuits = new Map<string, D>()
  private readonly bus: DevToolsBus
  private readonly registry: DevToolsRegistry

  protected constructor(
    private readonly enabled: boolean,
    protected readonly resetTimeoutMs: number,
    protected readonly halfOpenSuccessThreshold: number,
    private readonly maxKeys: number,
    deps?: CircuitRegistryDeps
  ) {
    this.bus = deps?.bus ?? getDevToolsBus()
    this.registry = deps?.registry ?? getDevToolsRegistry()
  }

  isEnabled(): boolean {
    return this.enabled
  }

  protected abstract newCircuit(): D
  /** Stats reported on state transitions (devtools/registry). */
  protected abstract transitionStats(c: D): { failures: number; successes: number; extra?: Record<string, unknown> }
  /** Record one call outcome; runs before any state transition. */
  protected abstract recordOutcome(c: D, ok: boolean): void
  /** After a failure while closed: should the circuit open? */
  protected abstract shouldOpen(c: D): boolean
  protected abstract onEnterHalfOpen(c: D): void
  /** Count a half-open probe success; true when enough to close. */
  protected abstract onProbeSuccess(c: D): boolean
  protected abstract resetOnClose(c: D): void

  protected getOrCreate(key: string): D {
    let c = this.circuits.get(key)
    if (!c) {
      this.evictIfFull()
      c = this.newCircuit()
      this.circuits.set(key, c)
    }
    return c
  }

  private evictIfFull(): void {
    if (this.circuits.size < this.maxKeys) return
    let fallback: string | undefined
    for (const [k, v] of this.circuits) {
      if (fallback === undefined) fallback = k
      if (v.state === "closed") {
        this.circuits.delete(k)
        return
      }
    }
    if (fallback !== undefined) this.circuits.delete(fallback)
  }

  protected emitTransition(key: string, prev: CircuitState, next: CircuitState, c: D, err?: unknown): void {
    if (prev === next) return
    const [service = "unknown", method = "unknown"] = key.split(":")
    const stats = this.transitionStats(c)
    this.registry.recordCircuit(key, next, {
      failures: stats.failures,
      successes: stats.successes,
      lastError: err instanceof Error ? err.message : err === undefined ? undefined : String(err)
    })
    this.bus.publish({
      ts: Date.now(),
      type: "circuit",
      service,
      method,
      extra: {
        key,
        from: prev,
        to: next,
        failures: stats.failures,
        successes: stats.successes,
        ...(stats.extra ?? {})
      }
    })
  }

  private throwOpen(key: string): never {
    const [service, method] = key.split(":")
    throw new CircuitOpenError(service ?? "unknown", method ?? "unknown")
  }

  before(key: string): void {
    if (!this.enabled) return
    const c = this.getOrCreate(key)
    if (c.state === "open") {
      if (Date.now() - c.openedAt >= this.resetTimeoutMs) {
        const prev = c.state
        c.state = "half-open"
        this.onEnterHalfOpen(c)
        c.halfOpenInFlight = false
        this.emitTransition(key, prev, c.state, c)
      } else {
        this.throwOpen(key)
      }
    }
    if (c.state === "half-open") {
      // A probe that never settled must not wedge the breaker forever; after
      // resetTimeoutMs the next caller takes over the probe.
      if (c.halfOpenInFlight && Date.now() - c.halfOpenProbeAt < this.resetTimeoutMs) {
        this.throwOpen(key)
      }
      c.halfOpenInFlight = true
      c.halfOpenProbeAt = Date.now()
    }
  }

  onSuccess(key: string): void {
    if (!this.enabled) return
    const c = this.getOrCreate(key)
    this.recordOutcome(c, true)
    if (c.state === "half-open") {
      c.halfOpenInFlight = false
      if (this.onProbeSuccess(c)) {
        const prev = c.state
        c.state = "closed"
        this.resetOnClose(c)
        c.halfOpenInFlight = false
        this.emitTransition(key, prev, c.state, c)
      }
    }
  }

  onFailure(key: string, err: unknown): void {
    if (!this.enabled) return
    // Client-side errors say nothing about downstream health.
    if (err instanceof MessagingError && (err.code === ErrorCode.VALIDATION_FAILED || err.code === ErrorCode.UNAUTHORIZED)) {
      const current = this.circuits.get(key)
      if (current?.state === "half-open") current.halfOpenInFlight = false
      return
    }
    const c = this.getOrCreate(key)
    this.recordOutcome(c, false)
    if (c.state === "half-open") {
      c.halfOpenInFlight = false
      const prev = c.state
      c.state = "open"
      c.openedAt = Date.now()
      this.emitTransition(key, prev, c.state, c, err)
      return
    }
    if (c.state === "closed" && this.shouldOpen(c)) {
      const prev = c.state
      c.state = "open"
      c.openedAt = Date.now()
      this.emitTransition(key, prev, c.state, c, err)
    }
  }
}

interface CountCircuitData extends BaseCircuitData {
  failures: number
  successes: number
}

/** Consecutive-failure circuit breaker. */
export class CircuitBreakerRegistry extends CircuitRegistryBase<CountCircuitData> {
  private readonly opts: ResolvedCircuitOptions

  constructor(opts?: CircuitBreakerOptions, deps?: CircuitRegistryDeps) {
    const resolved = resolveCircuitOptions(opts)
    super(resolved.enabled, resolved.resetTimeoutMs, resolved.halfOpenSuccessThreshold, resolved.maxKeys, deps)
    this.opts = resolved
  }

  protected newCircuit(): CountCircuitData {
    return { state: "closed", failures: 0, successes: 0, openedAt: 0, halfOpenInFlight: false, halfOpenProbeAt: 0 }
  }

  protected transitionStats(c: CountCircuitData): { failures: number; successes: number } {
    return { failures: c.failures, successes: c.successes }
  }

  protected recordOutcome(c: CountCircuitData, ok: boolean): void {
    if (ok) {
      if (c.state === "closed") c.failures = 0
    } else {
      c.failures++
    }
  }

  protected shouldOpen(c: CountCircuitData): boolean {
    return c.failures >= this.opts.failureThreshold
  }

  protected onEnterHalfOpen(c: CountCircuitData): void {
    c.successes = 0
  }

  protected onProbeSuccess(c: CountCircuitData): boolean {
    c.successes++
    return c.successes >= this.opts.halfOpenSuccessThreshold
  }

  protected resetOnClose(c: CountCircuitData): void {
    c.failures = 0
    c.successes = 0
  }

  snapshot(): Record<string, { state: CircuitState; failures: number }> {
    const out: Record<string, { state: CircuitState; failures: number }> = {}
    for (const [k, v] of this.circuits.entries()) {
      out[k] = { state: v.state, failures: v.failures }
    }
    return out
  }
}
