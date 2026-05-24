export enum ErrorCode {
  UNKNOWN = 0,
  UNAUTHORIZED = 1,
  TIMEOUT = 2,
  METHOD_NOT_FOUND = 3,
  SERVICE_NOT_FOUND = 4,
  SERVICE_UNAVAILABLE = 5,
  BAD_REQUEST = 6,
  VALIDATION_FAILED = 7,
  RATE_LIMITED = 8,
  CIRCUIT_OPEN = 9,
  PAYLOAD_TOO_LARGE = 10,
  PARSE_ERROR = 11,
  REPLAY_DETECTED = 12,
  IDEMPOTENT_REPLAY = 13,
  CONNECTION_LOST = 14,
  INTERNAL = 15,
  CANCELLED = 16,
  UNSUPPORTED_VERSION = 17,
  ACK_FAILED = 18
}

export function isRetryable(code: ErrorCode): boolean {
  switch (code) {
    case ErrorCode.TIMEOUT:
    case ErrorCode.SERVICE_UNAVAILABLE:
    case ErrorCode.CONNECTION_LOST:
    case ErrorCode.INTERNAL:
      return true
    default:
      return false
  }
}
