import { BackpressureLimiter, type BackpressureOptions, type PausableSubscription } from "./backpressure"
import { MessagingError } from "./errors"
import { ErrorCode } from "./error-code"
import { getMethodBackpressure } from "./resilience-decorators"
import type { SubscriptionContext } from "./types"

/**
 * Glue between subscribe-style handlers and {@link BackpressureLimiter}.
 */

export interface SubscriptionHandler<T = unknown> {
  (data: T, ctx: SubscriptionContext): Promise<void> | void
}

export interface BackpressureWrapperOptions extends BackpressureOptions {
  /** Strategy at hard cap: `reject` (default, throws RATE_LIMITED), `nack`, or `drop`. */
  onOverflow?: "reject" | "nack" | "drop"
}

export function wrapSubscriptionHandler<T>(
  handler: SubscriptionHandler<T>,
  subscription: PausableSubscription | (() => PausableSubscription | undefined),
  opts: BackpressureWrapperOptions
): SubscriptionHandler<T> {
  let limiter: BackpressureLimiter | null = null
  const sub = (): PausableSubscription | undefined =>
    typeof subscription === "function" ? subscription() : subscription
  const getLimiter = () => {
    if (limiter) return limiter
    limiter = new BackpressureLimiter(opts, {
      onPause: () => sub()?.pause?.(),
      onResume: () => sub()?.resume?.()
    })
    return limiter
  }
  const overflow = opts.onOverflow ?? "reject"

  return async (data, ctx) => {
    const lim = getLimiter()
    if (!lim.begin()) {
      if (overflow === "drop") return
      if (overflow === "nack") {
        try { await ctx.nack?.("backpressure overflow") } catch {}
        return
      }
      throw new MessagingError(ErrorCode.RATE_LIMITED, {
        message: "Backpressure: in-flight cap reached",
        retryable: true
      })
    }
    try {
      await handler(data, ctx)
    } finally {
      lim.end()
    }
  }
}

/** Read `@Backpressure(...)` metadata on `target[methodName]` and return a wrapped handler if found. */
export function installBackpressureFromDecorator<T>(
  target: any,
  methodName: string,
  handler: SubscriptionHandler<T>,
  subscription: PausableSubscription | (() => PausableSubscription | undefined)
): SubscriptionHandler<T> {
  const opts = getMethodBackpressure(target, methodName)
  if (!opts) return handler
  return wrapSubscriptionHandler<T>(handler, subscription, opts as BackpressureWrapperOptions)
}

/** Lower-level builder: install backpressure once and run many invocations through it. */
export function makeBackpressureRunner<T>(
  opts: BackpressureWrapperOptions,
  subscription: PausableSubscription | (() => PausableSubscription | undefined)
): { run: (data: T, ctx: SubscriptionContext, handler: SubscriptionHandler<T>) => Promise<void>; getInflight: () => number; isPaused: () => boolean } {
  const sub = (): PausableSubscription | undefined =>
    typeof subscription === "function" ? subscription() : subscription
  const limiter = new BackpressureLimiter(opts, {
    onPause: () => sub()?.pause?.(),
    onResume: () => sub()?.resume?.()
  })
  const overflow = opts.onOverflow ?? "reject"
  return {
    getInflight: () => limiter.getInflight(),
    isPaused: () => limiter.isPaused(),
    async run(data, ctx, handler) {
      if (!limiter.begin()) {
        if (overflow === "drop") return
        if (overflow === "nack") {
          try { await ctx.nack?.("backpressure overflow") } catch {}
          return
        }
        throw new MessagingError(ErrorCode.RATE_LIMITED, {
          message: "Backpressure: in-flight cap reached",
          retryable: true
        })
      }
      try {
        await handler(data, ctx)
      } finally {
        limiter.end()
      }
    }
  }
}
