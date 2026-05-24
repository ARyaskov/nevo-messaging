import { ErrorCode } from "./error-code"

export const ErrorMessages: Record<number, string> = {
  [ErrorCode.UNKNOWN]: "An unknown error occurred",
  [ErrorCode.UNAUTHORIZED]: "Access denied",
  [ErrorCode.TIMEOUT]: "Request timed out",
  [ErrorCode.METHOD_NOT_FOUND]: "Method not found",
  [ErrorCode.SERVICE_NOT_FOUND]: "Service not registered",
  [ErrorCode.SERVICE_UNAVAILABLE]: "Service unavailable",
  [ErrorCode.BAD_REQUEST]: "Bad request",
  [ErrorCode.VALIDATION_FAILED]: "Validation failed",
  [ErrorCode.RATE_LIMITED]: "Rate limit exceeded",
  [ErrorCode.CIRCUIT_OPEN]: "Circuit breaker is open",
  [ErrorCode.PAYLOAD_TOO_LARGE]: "Payload exceeds size limit",
  [ErrorCode.PARSE_ERROR]: "Failed to parse message",
  [ErrorCode.REPLAY_DETECTED]: "Replay attack detected",
  [ErrorCode.IDEMPOTENT_REPLAY]: "Idempotent request already processed",
  [ErrorCode.CONNECTION_LOST]: "Connection to broker lost",
  [ErrorCode.INTERNAL]: "Internal error",
  [ErrorCode.CANCELLED]: "Request cancelled",
  [ErrorCode.UNSUPPORTED_VERSION]: "Unsupported method version",
  [ErrorCode.ACK_FAILED]: "Failed to acknowledge message"
}
