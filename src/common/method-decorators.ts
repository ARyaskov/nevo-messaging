import "reflect-metadata"
import type { RateLimiterOptions } from "./rate-limit"

export const NEVO_METHOD_RATE_LIMIT = "nevo:method:rate-limit"
export const NEVO_METHOD_CACHEABLE = "nevo:method:cacheable"

export interface RateLimitConfig {
  capacity: number
  refillPerSec: number
  keyBy?: ("service" | "method" | "callerService" | "tenantId")[]
}

export interface CacheableConfig {
  ttlMs?: number
  maxEntries?: number
  keyBy?: (params: unknown) => string
}

function defineOnMethod(metaKey: string, target: any, propertyKey: string | symbol, value: unknown): void {
  const ctor = target?.constructor ?? target
  const map = (Reflect.getMetadata(metaKey, ctor) as Map<string, unknown> | undefined) ?? new Map<string, unknown>()
  map.set(propertyKey as string, value)
  Reflect.defineMetadata(metaKey, map, ctor)
}

export function RateLimit(config: RateLimitConfig): MethodDecorator {
  return (target, propertyKey) => {
    defineOnMethod(NEVO_METHOD_RATE_LIMIT, target, propertyKey, config)
  }
}

export function Cacheable(config: CacheableConfig = {}): MethodDecorator {
  return (target, propertyKey) => {
    defineOnMethod(NEVO_METHOD_CACHEABLE, target, propertyKey, config)
  }
}

export function getMethodRateLimit(target: any, propertyKey: string): RateLimitConfig | undefined {
  const ctor = target?.constructor ?? target
  const map = Reflect.getMetadata(NEVO_METHOD_RATE_LIMIT, ctor) as Map<string, RateLimitConfig> | undefined
  return map?.get(propertyKey)
}

export function getMethodCacheable(target: any, propertyKey: string): CacheableConfig | undefined {
  const ctor = target?.constructor ?? target
  const map = Reflect.getMetadata(NEVO_METHOD_CACHEABLE, ctor) as Map<string, CacheableConfig> | undefined
  return map?.get(propertyKey)
}

export function rateLimitToOptions(rl: RateLimitConfig): RateLimiterOptions {
  return {
    enabled: true,
    capacity: rl.capacity,
    refillPerSec: rl.refillPerSec,
    keyExtractor: (ctx) => {
      const keyBy = rl.keyBy ?? ["topic", "method", "callerService"]
      const parts: string[] = []
      for (const k of keyBy) {
        if (k === "service") parts.push(ctx.topic)
        else if (k === "method") parts.push(ctx.method)
        else if (k === "callerService") parts.push(ctx.callerService ?? "anon")
        else if (k === "tenantId") parts.push(ctx.tenantId ?? "no-tenant")
      }
      return parts.join(":")
    }
  }
}
