import { setTimeout as sleep } from "node:timers/promises"
import { ErrorCode, isRetryable } from "./error-code"
import { MessagingError } from "./errors"
import type { RetryOptions } from "./types"

export interface ResolvedRetryOptions {
  enabled: boolean
  maxAttempts: number
  baseMs: number
  maxMs: number
  jitter: boolean
  retryOnCodes: Set<number>
}

export function resolveRetryOptions(opts?: RetryOptions): ResolvedRetryOptions {
  return {
    enabled: opts?.enabled !== false && (opts?.maxAttempts ?? 0) !== 0,
    maxAttempts: opts?.maxAttempts ?? 3,
    baseMs: opts?.baseMs ?? 100,
    maxMs: opts?.maxMs ?? 2000,
    jitter: opts?.jitter !== false,
    retryOnCodes: new Set(opts?.retryOnCodes ?? [])
  }
}

export function shouldRetry(err: unknown, opts: ResolvedRetryOptions): boolean {
  if (!opts.enabled) return false
  if (err instanceof MessagingError) {
    if (opts.retryOnCodes.has(err.code)) return true
    if (err.retryable) return true
    return isRetryable(err.code)
  }
  if (err instanceof Error && /timeout|ECONNRESET|ENOTFOUND|ECONNREFUSED/i.test(err.message)) return true
  return false
}

export function computeDelay(attempt: number, opts: ResolvedRetryOptions): number {
  const exp = Math.min(opts.maxMs, opts.baseMs * Math.pow(2, attempt - 1))
  if (!opts.jitter) return exp
  return Math.floor(Math.random() * exp)
}

export async function withRetry<T>(fn: (attempt: number) => Promise<T>, opts: ResolvedRetryOptions, signal?: AbortSignal): Promise<T> {
  if (!opts.enabled) return fn(1)
  let lastErr: unknown
  for (let attempt = 1; attempt <= opts.maxAttempts; attempt++) {
    if (signal?.aborted) throw new MessagingError(ErrorCode.CANCELLED, { message: "Retry cancelled" })
    try {
      return await fn(attempt)
    } catch (err) {
      lastErr = err
      if (attempt >= opts.maxAttempts || !shouldRetry(err, opts)) throw err
      const delay = computeDelay(attempt, opts)
      await sleep(delay, undefined, { signal })
    }
  }
  throw lastErr
}
