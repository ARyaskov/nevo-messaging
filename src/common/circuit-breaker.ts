import { createRequire } from "node:module"
import { CircuitOpenError, MessagingError } from "./errors"
import { ErrorCode } from "./error-code"
import type { CircuitBreakerOptions } from "./types"
import { getDevToolsBus, DevToolsBus } from "./devtools"
import type { DevToolsRegistry } from "./devtools-registry"

const nodeRequire = createRequire(__filename)

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

interface CircuitData {
  state: CircuitState
  failures: number
  successes: number
  openedAt: number
  halfOpenInFlight: boolean
}

export class CircuitBreakerRegistry {
  private readonly circuits = new Map<string, CircuitData>()
  private readonly opts: ResolvedCircuitOptions
  private readonly bus: DevToolsBus
  private readonly registry: DevToolsRegistry

  constructor(opts?: CircuitBreakerOptions, deps?: { bus?: DevToolsBus; registry?: DevToolsRegistry }) {
    this.opts = resolveCircuitOptions(opts)
    this.bus = deps?.bus ?? getDevToolsBus()
    if (deps?.registry) {
      this.registry = deps.registry
    } else {
      const { getDevToolsRegistry } = nodeRequire("./devtools-registry") as typeof import("./devtools-registry")
      this.registry = getDevToolsRegistry()
    }
  }

  isEnabled(): boolean {
    return this.opts.enabled
  }

  private getCircuit(key: string): CircuitData {
    let c = this.circuits.get(key)
    if (!c) {
      this.evictIfFull()
      c = { state: "closed", failures: 0, successes: 0, openedAt: 0, halfOpenInFlight: false }
      this.circuits.set(key, c)
    }
    return c
  }

  private evictIfFull(): void {
    if (this.circuits.size < this.opts.maxKeys) return
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

  private emitTransition(key: string, prev: CircuitState, next: CircuitState, c: CircuitData, err?: unknown): void {
    if (prev === next) return
    const [service = "unknown", method = "unknown"] = key.split(":")
    this.registry.recordCircuit(key, next, {
      failures: c.failures,
      successes: c.successes,
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
        failures: c.failures,
        successes: c.successes
      }
    })
  }

  before(key: string): void {
    if (!this.opts.enabled) return
    const c = this.getCircuit(key)
    if (c.state === "open") {
      if (Date.now() - c.openedAt >= this.opts.resetTimeoutMs) {
        const prev = c.state
        c.state = "half-open"
        c.successes = 0
        c.halfOpenInFlight = false
        this.emitTransition(key, prev, c.state, c)
      } else {
        const [service, method] = key.split(":")
        throw new CircuitOpenError(service ?? "unknown", method ?? "unknown")
      }
    }
    if (c.state === "half-open") {
      if (c.halfOpenInFlight) {
        const [service, method] = key.split(":")
        throw new CircuitOpenError(service ?? "unknown", method ?? "unknown")
      }
      c.halfOpenInFlight = true
    }
  }

  onSuccess(key: string): void {
    if (!this.opts.enabled) return
    const c = this.getCircuit(key)
    if (c.state === "half-open") {
      c.halfOpenInFlight = false
      c.successes++
      if (c.successes >= this.opts.halfOpenSuccessThreshold) {
        const prev = c.state
        c.state = "closed"
        c.failures = 0
        c.successes = 0
        c.halfOpenInFlight = false
        this.emitTransition(key, prev, c.state, c)
      }
    } else if (c.state === "closed") {
      c.failures = 0
    }
  }

  onFailure(key: string, err: unknown): void {
    if (!this.opts.enabled) return
    if (err instanceof MessagingError && (err.code === ErrorCode.VALIDATION_FAILED || err.code === ErrorCode.UNAUTHORIZED)) {
      const current = this.circuits.get(key)
      if (current?.state === "half-open") current.halfOpenInFlight = false
      return
    }
    const c = this.getCircuit(key)
    if (c.state === "half-open") {
      c.halfOpenInFlight = false
      const prev = c.state
      c.state = "open"
      c.openedAt = Date.now()
      c.failures++
      this.emitTransition(key, prev, c.state, c, err)
      return
    }
    c.failures++
    if (c.failures >= this.opts.failureThreshold) {
      const prev = c.state
      c.state = "open"
      c.openedAt = Date.now()
      this.emitTransition(key, prev, c.state, c, err)
    }
  }

  snapshot(): Record<string, { state: CircuitState; failures: number }> {
    const out: Record<string, { state: CircuitState; failures: number }> = {}
    for (const [k, v] of this.circuits.entries()) {
      out[k] = { state: v.state, failures: v.failures }
    }
    return out
  }
}
