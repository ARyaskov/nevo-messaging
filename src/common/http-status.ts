import { MessagingError } from "./errors"
import { ErrorCode } from "./error-code"

/** Only 502/503/504 and a 429 carrying `Retry-After` are retryable. */
export function httpStatusToError(status: number, serviceName: string, headers?: Record<string, string>): MessagingError {
  switch (status) {
    case 413:
      return new MessagingError(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Remote returned HTTP ${status}`, httpStatus: status }, serviceName)
    case 429: {
      const retryAfterMs = parseRetryAfterMs(headers?.["retry-after"])
      return new MessagingError(
        ErrorCode.RATE_LIMITED,
        {
          message: `Remote returned HTTP ${status}`,
          httpStatus: status,
          ...(retryAfterMs !== null ? { retryAfterMs, retryable: true } : {})
        },
        serviceName
      )
    }
    case 502:
    case 503:
      return new MessagingError(
        ErrorCode.SERVICE_UNAVAILABLE,
        { message: `Remote returned HTTP ${status}`, httpStatus: status, retryable: true },
        serviceName
      )
    case 504:
      return new MessagingError(ErrorCode.TIMEOUT, { message: `Remote returned HTTP ${status}`, httpStatus: status, retryable: true }, serviceName)
    default:
      return new MessagingError(ErrorCode.REMOTE_ERROR, { message: `Remote returned HTTP ${status}`, httpStatus: status }, serviceName)
  }
}

/** RFC 9110 `Retry-After`: delay-seconds or an HTTP-date. Returns null when unusable. */
export function parseRetryAfterMs(value: string | undefined, now = Date.now()): number | null {
  if (!value) return null
  const trimmed = value.trim()
  if (/^\d+$/.test(trimmed)) {
    const ms = Number(trimmed) * 1000
    return Number.isFinite(ms) && ms >= 0 ? Math.min(ms, 300_000) : null
  }
  const at = Date.parse(trimmed)
  if (Number.isNaN(at)) return null
  const delta = at - now
  return delta > 0 ? Math.min(delta, 300_000) : 0
}
