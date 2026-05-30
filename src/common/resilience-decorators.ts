import "reflect-metadata"
import type { CircuitBreakerOptions } from "./types"
import type { SlidingCircuitOptions } from "./sliding-circuit-breaker"
import type { HedgingOptions } from "./hedging"
import type { AdaptiveOptions } from "./adaptive"
import type { BackpressureOptions } from "./backpressure"
import type { TenantKeyDimension } from "./tenant-policy"

/**
 * Method-level resilience decorators. Runtime lives in `./resilience-runtime.ts`.
 */

export const NEVO_METHOD_HEDGE = "nevo:method:hedge"
export const NEVO_METHOD_CIRCUIT = "nevo:method:circuit"
export const NEVO_METHOD_ADAPTIVE = "nevo:method:adaptive"
export const NEVO_METHOD_BACKPRESSURE = "nevo:method:backpressure"

export type CircuitBreakerDecoratorOptions = (CircuitBreakerOptions | SlidingCircuitOptions) & {
  mode?: "count" | "sliding"
  keyBy?: TenantKeyDimension[]
}

export type BackpressureDecoratorOptions = BackpressureOptions & {
  keyBy?: TenantKeyDimension[]
  onOverflow?: "reject" | "nack" | "drop"
}

export type AdaptiveDecoratorOptions = AdaptiveOptions & {
  keyBy?: TenantKeyDimension[]
}

function defineOnMethod(metaKey: string, target: any, propertyKey: string | symbol, value: unknown): void {
  const ctor = target?.constructor ?? target
  const map =
    (Reflect.getMetadata(metaKey, ctor) as Map<string, unknown> | undefined) ?? new Map<string, unknown>()
  map.set(propertyKey as string, value)
  Reflect.defineMetadata(metaKey, map, ctor)
}

function readOnMethod<T>(metaKey: string, target: any, propertyKey: string): T | undefined {
  const ctor = target?.constructor ?? target
  const map = Reflect.getMetadata(metaKey, ctor) as Map<string, T> | undefined
  return map?.get(propertyKey)
}

/** Declare that calls through this method should be hedged. Safe only for idempotent reads. */
export function Hedge(options: HedgingOptions = {}): MethodDecorator {
  return (target, propertyKey) => {
    defineOnMethod(NEVO_METHOD_HEDGE, target, propertyKey, { enabled: true, ...options })
  }
}

/** Declare a per-method circuit breaker (sliding window by default; pass `mode: "count"` for consecutive-failure). */
export function CircuitBreaker(options: CircuitBreakerDecoratorOptions = {}): MethodDecorator {
  return (target, propertyKey) => {
    defineOnMethod(NEVO_METHOD_CIRCUIT, target, propertyKey, { enabled: true, ...options })
  }
}

/** Declare adaptive retry/timeout tuning. Latency is fed back automatically by the runtime. */
export function Adaptive(options: AdaptiveDecoratorOptions = {}): MethodDecorator {
  return (target, propertyKey) => {
    defineOnMethod(NEVO_METHOD_ADAPTIVE, target, propertyKey, { enabled: true, ...options })
  }
}

/** Declare automatic backpressure (high/low-watermark gate). Use on subscribe handlers. */
export function Backpressure(options: BackpressureDecoratorOptions = {}): MethodDecorator {
  return (target, propertyKey) => {
    defineOnMethod(NEVO_METHOD_BACKPRESSURE, target, propertyKey, { enabled: true, ...options })
  }
}

export function getMethodHedge(target: any, propertyKey: string): HedgingOptions | undefined {
  return readOnMethod<HedgingOptions>(NEVO_METHOD_HEDGE, target, propertyKey)
}

export function getMethodCircuit(target: any, propertyKey: string): CircuitBreakerDecoratorOptions | undefined {
  return readOnMethod<CircuitBreakerDecoratorOptions>(NEVO_METHOD_CIRCUIT, target, propertyKey)
}

export function getMethodAdaptive(target: any, propertyKey: string): AdaptiveDecoratorOptions | undefined {
  return readOnMethod<AdaptiveDecoratorOptions>(NEVO_METHOD_ADAPTIVE, target, propertyKey)
}

export function getMethodBackpressure(target: any, propertyKey: string): BackpressureDecoratorOptions | undefined {
  return readOnMethod<BackpressureDecoratorOptions>(NEVO_METHOD_BACKPRESSURE, target, propertyKey)
}
