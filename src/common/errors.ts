import { ErrorCode, ErrorDetails, ErrorMessages } from "./"

export class MessagingError extends Error {
  readonly code: ErrorCode
  readonly details: Record<string, unknown>
  readonly serviceName: string

  constructor(code: ErrorCode, options?: Record<string, unknown>, serviceName = "unknown") {
    super((options?.["message"] as string) || ErrorMessages[code])

    this.name = "MessagingError"
    this.code = code
    this.details = { ...(options || {}) }
    this.serviceName = serviceName

    if (process.env["MODE"] === "production") {
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

    if (process.env["MODE"] !== "production" && this.stack) {
      error.stack = this.stack
    }

    return error
  }

  static fromJSON(error: ErrorDetails): MessagingError {
    const { code, message, details, service, stack } = error
    const messagingError = new MessagingError(code, { message, ...details }, service)

    if (process.env["MODE"] !== "production" && stack) {
      messagingError.stack = stack
    }

    return messagingError
  }
}
