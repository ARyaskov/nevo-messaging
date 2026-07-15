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
    enabled: opts?.enabled !== false && (opts?.maxAttempts ?? 3) !== 0,
    maxAttempts: opts?.maxAttempts ?? 3,
    baseMs: opts?.baseMs ?? 100,
    maxMs: opts?.maxMs ?? 2000,
    jitter: opts?.jitter !== false,
    retryOnCodes: new Set(opts?.retryOnCodes ?? [])
  }
}

const RETRYABLE_SYSCALL_CODES = new Set(["ECONNRESET", "ENOTFOUND", "ECONNREFUSED", "ETIMEDOUT", "EPIPE", "EAI_AGAIN", "ECONNABORTED"])

export function shouldRetry(err: unknown, opts: ResolvedRetryOptions): boolean {
  if (!opts.enabled) return false
  if (err instanceof MessagingError) {
    if (opts.retryOnCodes.has(err.code)) return true
    if (Object.prototype.hasOwnProperty.call(err.details, "retryable")) return err.retryable
    if (err.retryable) return true
    return isRetryable(err.code)
  }
  const sysCode = (err as { code?: unknown })?.code
  if (typeof sysCode === "string" && RETRYABLE_SYSCALL_CODES.has(sysCode)) return true
  if (err instanceof Error && /timeout|ECONNRESET|ENOTFOUND|ECONNREFUSED/i.test(err.message)) return true
  return false
}

// A server-provided retry-after hint (e.g. rate limiting) overrides the exponential backoff.
export function retryAfterHintMs(err: unknown): number | null {
  if (!(err instanceof MessagingError)) return null
  const hint = err.details["retryAfterMs"]
  if (typeof hint !== "number" || !Number.isFinite(hint) || hint <= 0) return null
  return Math.min(hint, 30_000)
}

export function computeDelay(attempt: number, opts: ResolvedRetryOptions, err?: unknown): number {
  const hint = retryAfterHintMs(err)
  if (hint !== null) return hint
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
      const delay = computeDelay(attempt, opts, err)
      await sleep(delay, undefined, { signal })
    }
  }
  throw lastErr
}
