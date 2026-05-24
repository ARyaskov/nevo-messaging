import { ErrorCode } from "./error-code"
import { ErrorMessages } from "./error-messages"
import { isProduction } from "./env"
import type { ErrorDetails } from "./types"

export class MessagingError extends Error {
  readonly code: ErrorCode
  readonly details: Record<string, unknown>
  readonly serviceName: string
  readonly retryable: boolean

  constructor(code: ErrorCode, options?: Record<string, unknown>, serviceName = "unknown") {
    super((options?.["message"] as string) || ErrorMessages[code] || "Messaging error")

    this.name = "MessagingError"
    this.code = code
    this.details = { ...(options || {}) }
    delete (this.details as any).message
    this.serviceName = serviceName
    this.retryable = Boolean(options?.["retryable"])

    if (isProduction()) {
      this.stack = ""
    } else if (this.stack) {
      const stack = this.stack.split("\n")
      this.stack = stack.slice(1).join("\n")
    }
  }

  toJSON(): ErrorDetails {
    const error: ErrorDetails = {
      code: this.code,
      message: this.message,
      details: this.details,
      service: this.serviceName
    }

    if (!isProduction() && this.stack) {
      error.stack = this.stack
    }

    return error
  }

  static fromJSON(error: ErrorDetails): MessagingError {
    const { code, message, details, service, stack } = error
    const messagingError = new MessagingError(code, { message, ...details }, service)

    if (!isProduction() && stack) {
      messagingError.stack = stack
    }

    return messagingError
  }
}

export class TimeoutError extends MessagingError {
  constructor(serviceName: string, method: string, timeoutMs: number) {
    super(ErrorCode.TIMEOUT, { message: `Request to ${serviceName}.${method} timed out after ${timeoutMs}ms`, retryable: true }, serviceName)
  }
}

export class CircuitOpenError extends MessagingError {
  constructor(serviceName: string, method: string) {
    super(ErrorCode.CIRCUIT_OPEN, { message: `Circuit breaker open for ${serviceName}.${method}`, retryable: false }, serviceName)
  }
}

export class PayloadTooLargeError extends MessagingError {
  constructor(size: number, limit: number) {
    super(ErrorCode.PAYLOAD_TOO_LARGE, { message: `Payload size ${size}B exceeds limit ${limit}B`, size, limit })
  }
}
