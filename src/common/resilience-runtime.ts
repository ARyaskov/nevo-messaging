import { hedge, type HedgingOptions } from "./hedging"
import { CircuitBreakerRegistry } from "./circuit-breaker"
import { SlidingCircuitBreakerRegistry, type SlidingCircuitOptions } from "./sliding-circuit-breaker"
import { AdaptiveTuner, type AdaptiveOptions } from "./adaptive"
import { BackpressureLimiter, type BackpressureOptions, type PausableSubscription } from "./backpressure"
import { MessagingError } from "./errors"
import { ErrorCode } from "./error-code"
import {
  getMethodHedge,
  getMethodCircuit,
  getMethodAdaptive,
  getMethodBackpressure,
  type CircuitBreakerDecoratorOptions
} from "./resilience-decorators"
import { buildResilienceKey, type TenantKeyDimension, type ResilienceKeyContext } from "./tenant-policy"
import { computeDelay, resolveRetryOptions, shouldRetry } from "./retry"
import { setTimeout as sleep } from "node:timers/promises"

/**
 * Glue layer that materialises decorator metadata into actual resilience behaviour at call time.
 */

export interface ResilienceContext {
  /** Logical breaker key — usually `service:method`. */
  key: string
  dimensions?: ResilienceKeyContext
}

export interface CompiledResilience {
  hedge?: HedgingOptions
  circuit?: { mode: "count" | "sliding"; opts: CircuitBreakerDecoratorOptions; keyBy?: TenantKeyDimension[] }
  adaptive?: AdaptiveOptions & { keyBy?: TenantKeyDimension[] }
  backpressure?: BackpressureOptions & { keyBy?: TenantKeyDimension[] }
}

/** Apply optional `keyBy` widening to the supplied base key. */
function widenKey(baseKey: string, keyBy: TenantKeyDimension[] | undefined, dimensions: ResilienceKeyContext | undefined): string {
  if (!keyBy || keyBy.length === 0 || !dimensions) return baseKey
  // Append only the dynamic dims (skip service/method — they're already in baseKey).
  const extra = keyBy.filter((d) => d === "tenantId" || d === "callerService")
  if (extra.length === 0) return baseKey
  return `${baseKey}:${buildResilienceKey(dimensions, extra)}`
}

/** Sliding-specific options imply sliding; a bare `failureThreshold` implies count; default stays sliding. */
function inferCircuitMode(c: CircuitBreakerDecoratorOptions): "count" | "sliding" {
  const s = c as SlidingCircuitOptions
  if (
    typeof s.windowMs === "number" ||
    typeof s.bucketMs === "number" ||
    typeof s.errorRateThreshold === "number" ||
    typeof s.minSampleSize === "number"
  ) {
    return "sliding"
  }
  return typeof c.failureThreshold === "number" ? "count" : "sliding"
}

/** Read every resilience annotation on `target[propertyKey]` and return a normalised config bundle. */
export function readMethodResilience(target: any, propertyKey: string): CompiledResilience | undefined {
  const h = getMethodHedge(target, propertyKey)
  const c = getMethodCircuit(target, propertyKey)
  const a = getMethodAdaptive(target, propertyKey)
  const b = getMethodBackpressure(target, propertyKey)
  if (!h && !c && !a && !b) return undefined
  return {
    hedge: h,
    circuit: c
      ? {
          mode: c.mode ?? inferCircuitMode(c),
          opts: c,
          keyBy: c.keyBy
        }
      : undefined,
    adaptive: a,
    backpressure: b
  }
}

// Bounded: with keyBy tenant/caller dimensions the key space is caller-controlled.
const MAX_RUNTIME_KEYS = 10_000
const adaptiveByKey = new Map<string, AdaptiveTuner>()
const backpressureByKey = new Map<string, BackpressureLimiter>()

// Map insertion order doubles as the LRU list: refresh on hit, evict oldest on overflow.
function lruGet<V>(map: Map<string, V>, key: string): V | undefined {
  const v = map.get(key)
  if (v !== undefined) {
    map.delete(key)
    map.set(key, v)
  }
  return v
}

function lruEvict<V>(map: Map<string, V>, canEvict?: (v: V) => boolean): void {
  if (map.size < MAX_RUNTIME_KEYS) return
  for (const [k, v] of map) {
    if (!canEvict || canEvict(v)) {
      map.delete(k)
      return
    }
  }
}

// Keyed on `globalThis` so multiple imports share state across re-imports.
// Registries are keyed per option-set: each distinct decorator config gets its
// own registry instead of silently inheriting the first method's thresholds.
const slidingRegistries: Map<string, SlidingCircuitBreakerRegistry> = ((globalThis as any).__nevoSlidingRegistries ??= new Map())
const countRegistries: Map<string, CircuitBreakerRegistry> = ((globalThis as any).__nevoCountRegistries ??= new Map())

function circuitConfigKey(opts: CircuitBreakerDecoratorOptions): string {
  const plain: Record<string, unknown> = {}
  for (const k of Object.keys(opts as Record<string, unknown>).sort()) {
    const v = (opts as Record<string, unknown>)[k]
    if (v === undefined || typeof v === "function") continue
    plain[k] = v
  }
  return JSON.stringify(plain)
}

function getSlidingRegistry(opts: CircuitBreakerDecoratorOptions): SlidingCircuitBreakerRegistry {
  const key = circuitConfigKey(opts)
  let r = slidingRegistries.get(key)
  if (!r) {
    r = new SlidingCircuitBreakerRegistry({ enabled: true, ...(opts as SlidingCircuitOptions) })
    slidingRegistries.set(key, r)
  }
  return r
}

function getCountRegistry(opts: CircuitBreakerDecoratorOptions): CircuitBreakerRegistry {
  const key = circuitConfigKey(opts)
  let r = countRegistries.get(key)
  if (!r) {
    r = new CircuitBreakerRegistry({ enabled: true, ...opts })
    countRegistries.set(key, r)
  }
  return r
}

function getAdaptive(key: string, opts: AdaptiveOptions): AdaptiveTuner {
  let t = lruGet(adaptiveByKey, key)
  if (!t) {
    lruEvict(adaptiveByKey)
    t = new AdaptiveTuner({ enabled: true, ...opts })
    adaptiveByKey.set(key, t)
  }
  return t
}

function getBackpressureLimiter(key: string, opts: BackpressureOptions, subscription?: PausableSubscription): BackpressureLimiter {
  let l = lruGet(backpressureByKey, key)
  if (!l) {
    lruEvict(backpressureByKey, (limiter) => limiter.getInflight() === 0)
    l = new BackpressureLimiter(opts, {
      onPause: () => subscription?.pause?.(),
      onResume: () => subscription?.resume?.()
    })
    backpressureByKey.set(key, l)
  }
  return l
}

/** Public snapshot used by DevTools tests. */
export function snapshotResilience(): {
  adaptive: Record<string, ReturnType<AdaptiveTuner["snapshot"]>>
  sliding: Record<string, { state: string; errorRate: number; sampleSize: number }> | null
  backpressure: Record<string, { inflight: number; paused: boolean }>
} {
  const adaptive: Record<string, ReturnType<AdaptiveTuner["snapshot"]>> = {}
  for (const [k, t] of adaptiveByKey.entries()) adaptive[k] = t.snapshot()
  let sliding: Record<string, { state: string; errorRate: number; sampleSize: number }> | null = null
  for (const r of slidingRegistries.values()) {
    const snap = r.snapshot()
    if (sliding) Object.assign(sliding, snap)
    else sliding = { ...snap }
  }
  const backpressure: Record<string, { inflight: number; paused: boolean }> = {}
  for (const [k, l] of backpressureByKey.entries()) {
    backpressure[k] = { inflight: l.getInflight(), paused: l.isPaused() }
  }
  return { adaptive, sliding, backpressure }
}

export interface ApplyResilienceArgs<T> {
  config: CompiledResilience
  ctx: ResilienceContext
  invoke: (attempt: number, signal: AbortSignal) => Promise<T>
  subscription?: PausableSubscription
  budget?: InvocationBudget
}

export const DEFAULT_MAX_PHYSICAL_CALLS = 8

/** Shared cap across adaptive retries, transport retries, and hedge copies. */
export class InvocationBudget {
  private used = 0
  constructor(readonly maxCalls = DEFAULT_MAX_PHYSICAL_CALLS) {}

  async run<T>(fn: () => Promise<T>): Promise<T> {
    if (this.used >= this.maxCalls) {
      throw new MessagingError(ErrorCode.SERVICE_UNAVAILABLE, {
        message: `Resilience invocation budget exhausted after ${this.maxCalls} physical calls`,
        retryable: false,
        maxPhysicalCalls: this.maxCalls
      })
    }
    this.used++
    return fn()
  }

  get usedCalls(): number {
    return this.used
  }
}

/** Apply the compiled resilience config around `invoke`. */
export async function applyResilience<T>(args: ApplyResilienceArgs<T>): Promise<T> {
  const { config, ctx, invoke, subscription } = args
  const budget = args.budget ?? new InvocationBudget()
  const key = ctx.key

  if (config.backpressure) {
    const bpKey = widenKey(key, config.backpressure.keyBy, ctx.dimensions)
    const limiter = getBackpressureLimiter(bpKey, config.backpressure, subscription)
    const admitted = limiter.begin()
    if (!admitted) {
      throw new MessagingError(ErrorCode.RATE_LIMITED, {
        message: `Backpressure: in-flight cap reached for ${key}`,
        method: key,
        retryable: true
      })
    }
    try {
      return await runCircuitHedge(config, ctx, invoke, budget)
    } finally {
      limiter.end()
    }
  }

  return runCircuitHedge(config, ctx, invoke, budget)
}

async function runCircuitHedge<T>(
  config: CompiledResilience,
  ctx: ResilienceContext,
  invoke: (attempt: number, signal: AbortSignal) => Promise<T>,
  budget: InvocationBudget
): Promise<T> {
  const key = ctx.key
  const circuitKey = widenKey(key, config.circuit?.keyBy, ctx.dimensions)
  const adaptiveKey = widenKey(key, config.adaptive?.keyBy, ctx.dimensions)
  const started = Date.now()
  const useSliding = config.circuit?.mode !== "count"
  const slidingReg = config.circuit && useSliding ? getSlidingRegistry(config.circuit.opts) : null
  const countReg = config.circuit && !useSliding ? getCountRegistry(config.circuit.opts) : null
  const tuner = config.adaptive ? getAdaptive(adaptiveKey, config.adaptive) : null

  if (slidingReg) slidingReg.before(circuitKey)
  if (countReg) countReg.before(circuitKey)

  const finish = (ok: boolean, err?: unknown) => {
    const duration = Date.now() - started
    // Feed the whole logical call (all adaptive retries + hedge copies count as
    // one observation) back into the tuner so its next read reflects reality.
    if (tuner) {
      try {
        tuner.observe(duration, ok)
      } catch {}
    }
    if (slidingReg) {
      if (ok) slidingReg.onSuccess(circuitKey)
      else slidingReg.onFailure(circuitKey, err)
    }
    if (countReg) {
      if (ok) countReg.onSuccess(circuitKey)
      else countReg.onFailure(circuitKey, err)
    }
  }

  // A single logical attempt: hedge (N racing copies, first wins) when
  // configured, otherwise a bare invoke. The adaptive retry/timeout loop, when
  // enabled, wraps this — so one breaker `before`/outcome still spans the whole
  // call no matter how many retries or hedged copies fire underneath.
  const attemptOnce = (attempt: number, signal: AbortSignal): Promise<T> => {
    const hedgeOpts = config.hedge
    if (hedgeOpts && hedgeOpts.enabled !== false && (hedgeOpts.copies ?? 2) > 1) {
      return hedge<T>((hAttempt, hSignal) => budget.run(() => invoke(hAttempt, hSignal)), hedgeOpts)
    }
    return budget.run(() => invoke(attempt, signal))
  }

  try {
    const result = tuner ? await runAdaptive<T>(tuner, attemptOnce) : await attemptOnce(1, new AbortController().signal)
    finish(true)
    return result
  } catch (err) {
    finish(false, err)
    throw err
  }
}

/**
 * Drive `attempt` under the tuner's *current* guidance: at most `getRetries()`
 * attempts, each bounded by `getTimeoutMs()`. The guidance is read here (before
 * the call) and updated afterwards via `observe()` in `finish()`, so the tuner's
 * output genuinely shapes retry count and per-attempt timeout instead of being
 * computed and thrown away.
 */
async function runAdaptive<T>(tuner: AdaptiveTuner, attempt: (n: number, signal: AbortSignal) => Promise<T>): Promise<T> {
  const maxAttempts = Math.max(1, tuner.getRetries())
  const timeoutMs = tuner.getTimeoutMs()
  const retry = resolveRetryOptions({
    enabled: true,
    maxAttempts,
    baseMs: 100,
    maxMs: 2000,
    jitter: true
  })
  let lastErr: unknown
  for (let n = 1; n <= maxAttempts; n++) {
    try {
      return await callWithTimeout(timeoutMs, (signal) => attempt(n, signal))
    } catch (err) {
      lastErr = err
      if (n >= maxAttempts || !shouldRetry(err, retry)) throw err
      await sleep(computeDelay(n, retry))
    }
  }
  throw lastErr
}

/**
 * Race `fn` against an adaptive timeout. An `AbortController` is tripped after
 * `timeoutMs` and passed to `fn` (cooperative cancellation); the race itself
 * enforces the deadline even when `fn` ignores the signal. The timer is unref'd
 * and always cleared, so it neither leaks nor keeps the process alive.
 */
async function callWithTimeout<T>(timeoutMs: number, fn: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const ctrl = new AbortController()
  const timer = setTimeout(() => ctrl.abort(), timeoutMs)
  if (typeof timer.unref === "function") timer.unref()
  try {
    return await new Promise<T>((resolve, reject) => {
      const onAbort = () => reject(new MessagingError(ErrorCode.TIMEOUT, { message: `Adaptive timeout after ${timeoutMs}ms`, retryable: true }))
      if (ctrl.signal.aborted) return onAbort()
      ctrl.signal.addEventListener("abort", onAbort, { once: true })
      fn(ctrl.signal).then(resolve, reject)
    })
  } finally {
    clearTimeout(timer)
  }
}

/** Wrap any async function with the resilience config declared on `target[propertyKey]`. */
export function wrapMethodWithResilience<Args extends any[], T>(
  target: any,
  propertyKey: string,
  fn: (...args: Args) => Promise<T>,
  ctxBuilder: (...args: Args) => ResilienceContext,
  subscription?: PausableSubscription
): (...args: Args) => Promise<T> {
  const config = readMethodResilience(target, propertyKey)
  if (!config) return fn
  return async (...args: Args): Promise<T> => {
    const ctx = ctxBuilder(...args)
    return applyResilience<T>({
      config,
      ctx,
      subscription,
      invoke: (_attempt, _signal) => fn(...args)
    })
  }
}

/** Lower-level hook: compile the resilience config once, return a per-invocation runner. */
export function makeResilienceRunner(
  target: any,
  propertyKey: string,
  subscription?: PausableSubscription
): (<T>(key: string, invoke: (attempt: number, signal: AbortSignal) => Promise<T>) => Promise<T>) | undefined {
  const config = readMethodResilience(target, propertyKey)
  if (!config) return undefined
  return async <T>(key: string, invoke: (attempt: number, signal: AbortSignal) => Promise<T>) =>
    applyResilience<T>({ config, ctx: { key }, invoke, subscription })
}
