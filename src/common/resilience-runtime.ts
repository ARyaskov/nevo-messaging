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
function widenKey(
  baseKey: string,
  keyBy: TenantKeyDimension[] | undefined,
  dimensions: ResilienceKeyContext | undefined
): string {
  if (!keyBy || keyBy.length === 0 || !dimensions) return baseKey
  // Append only the dynamic dims (skip service/method — they're already in baseKey).
  const extra = keyBy.filter((d) => d === "tenantId" || d === "callerService")
  if (extra.length === 0) return baseKey
  return `${baseKey}:${buildResilienceKey(dimensions, extra)}`
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
          mode: c.mode ?? (typeof (c as SlidingCircuitOptions).windowMs === "number" ? "sliding" : "sliding"),
          opts: c,
          keyBy: c.keyBy
        }
      : undefined,
    adaptive: a,
    backpressure: b
  }
}

const slidingByMode = new WeakMap<object, SlidingCircuitBreakerRegistry>()
const countByMode = new WeakMap<object, CircuitBreakerRegistry>()
const adaptiveByKey = new Map<string, AdaptiveTuner>()
const backpressureByKey = new Map<string, BackpressureLimiter>()

// Keyed by `globalThis` so multiple imports share state across re-imports.
const SLIDING_ANCHOR: object = ((globalThis as any).__nevoSlidingAnchor ??= {})
const COUNT_ANCHOR: object = ((globalThis as any).__nevoCountAnchor ??= {})

function getSlidingRegistry(opts: CircuitBreakerDecoratorOptions): SlidingCircuitBreakerRegistry {
  let r = slidingByMode.get(SLIDING_ANCHOR)
  if (!r) {
    r = new SlidingCircuitBreakerRegistry({ enabled: true, ...(opts as SlidingCircuitOptions) })
    slidingByMode.set(SLIDING_ANCHOR, r)
  }
  return r
}

function getCountRegistry(opts: CircuitBreakerDecoratorOptions): CircuitBreakerRegistry {
  let r = countByMode.get(COUNT_ANCHOR)
  if (!r) {
    r = new CircuitBreakerRegistry({ enabled: true, ...opts })
    countByMode.set(COUNT_ANCHOR, r)
  }
  return r
}

function getAdaptive(key: string, opts: AdaptiveOptions): AdaptiveTuner {
  let t = adaptiveByKey.get(key)
  if (!t) {
    t = new AdaptiveTuner({ enabled: true, ...opts })
    adaptiveByKey.set(key, t)
  }
  return t
}

function getBackpressureLimiter(
  key: string,
  opts: BackpressureOptions,
  subscription?: PausableSubscription
): BackpressureLimiter {
  let l = backpressureByKey.get(key)
  if (!l) {
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
  const sliding = slidingByMode.get(SLIDING_ANCHOR)?.snapshot() ?? null
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
}

/** Apply the compiled resilience config around `invoke`. */
export async function applyResilience<T>(args: ApplyResilienceArgs<T>): Promise<T> {
  const { config, ctx, invoke, subscription } = args
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
      return await runCircuitHedge(config, ctx, invoke)
    } finally {
      limiter.end()
    }
  }

  return runCircuitHedge(config, ctx, invoke)
}

async function runCircuitHedge<T>(
  config: CompiledResilience,
  ctx: ResilienceContext,
  invoke: (attempt: number, signal: AbortSignal) => Promise<T>
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
      try { tuner.observe(duration, ok) } catch {}
    }
    if (slidingReg) ok ? slidingReg.onSuccess(circuitKey) : slidingReg.onFailure(circuitKey, err)
    if (countReg) ok ? countReg.onSuccess(circuitKey) : countReg.onFailure(circuitKey, err)
  }

  // A single logical attempt: hedge (N racing copies, first wins) when
  // configured, otherwise a bare invoke. The adaptive retry/timeout loop, when
  // enabled, wraps this — so one breaker `before`/outcome still spans the whole
  // call no matter how many retries or hedged copies fire underneath.
  const attemptOnce = (attempt: number, signal: AbortSignal): Promise<T> => {
    const hedgeOpts = config.hedge
    if (hedgeOpts && hedgeOpts.enabled !== false && (hedgeOpts.copies ?? 1) > 1) {
      return hedge<T>((hAttempt, hSignal) => invoke(hAttempt, hSignal), hedgeOpts)
    }
    return invoke(attempt, signal)
  }

  try {
    const result = tuner
      ? await runAdaptive<T>(tuner, attemptOnce)
      : await attemptOnce(1, new AbortController().signal)
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
async function runAdaptive<T>(
  tuner: AdaptiveTuner,
  attempt: (n: number, signal: AbortSignal) => Promise<T>
): Promise<T> {
  const maxAttempts = Math.max(1, tuner.getRetries())
  const timeoutMs = tuner.getTimeoutMs()
  let lastErr: unknown
  for (let n = 1; n <= maxAttempts; n++) {
    try {
      return await callWithTimeout(timeoutMs, (signal) => attempt(n, signal))
    } catch (err) {
      lastErr = err
      if (n >= maxAttempts) throw err
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
      const onAbort = () =>
        reject(new MessagingError(ErrorCode.TIMEOUT, { message: `Adaptive timeout after ${timeoutMs}ms`, retryable: true }))
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
