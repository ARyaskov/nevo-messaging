# Error codes

`MessagingError` carries a structured numeric `ErrorCode` so callers can branch on outcomes without parsing strings.

## Real enum

```ts
enum ErrorCode {
  UNKNOWN              = 0,
  UNAUTHORIZED         = 1,
  TIMEOUT              = 2,
  METHOD_NOT_FOUND     = 3,
  SERVICE_NOT_FOUND    = 4,
  SERVICE_UNAVAILABLE  = 5,
  BAD_REQUEST          = 6,
  VALIDATION_FAILED    = 7,
  RATE_LIMITED         = 8,
  CIRCUIT_OPEN         = 9,
  PAYLOAD_TOO_LARGE    = 10,
  PARSE_ERROR          = 11,
  REPLAY_DETECTED      = 12,
  IDEMPOTENT_REPLAY    = 13,
  CONNECTION_LOST      = 14,
  INTERNAL             = 15,
  CANCELLED            = 16,
  UNSUPPORTED_VERSION  = 17,
  ACK_FAILED           = 18
}
```

Stable numbers — safe to switch on. The full list is 0–18.

## Throwing

```ts
import { MessagingError, ErrorCode } from "@riaskov/nevo-messaging"

throw new MessagingError(ErrorCode.UNAUTHORIZED, {
  message: "Invalid bearer token",
  details: { reason: "expired" }
})
```

## Catching

```ts
try {
  await this.query("user", "user.create", input)
} catch (err) {
  if (err instanceof MessagingError) {
    switch (err.code) {
      case ErrorCode.VALIDATION_FAILED:  return showFormErrors(err.details)
      case ErrorCode.RATE_LIMITED:        return retryLater()
      case ErrorCode.CIRCUIT_OPEN:        return showDegraded()
      case ErrorCode.UNAUTHORIZED:        return redirectToLogin()
      default:                            throw err
    }
  }
  throw err
}
```

## Retryable codes

The framework's built-in `isRetryable(code)` returns `true` for:

- `TIMEOUT`
- `SERVICE_UNAVAILABLE`
- `CONNECTION_LOST`
- `INTERNAL`

Everything else is treated as non-transient by default. Override via `retry.retryOnCodes`.

## Did-you-mean (`METHOD_NOT_FOUND`)

`METHOD_NOT_FOUND` errors include a `suggestion` field with the closest matching registered method (via Levenshtein):

```json
{
  "code": 3,
  "message": "Invalid method 'user.getByI', did you mean 'user.getById'?",
  "details": { "suggestion": "user.getById", "distance": 1 }
}
```

Works across all transports.

## Codes the framework does NOT define

These were aspirational and never made it into the enum:

- `NOT_FOUND` — use `METHOD_NOT_FOUND`, `SERVICE_NOT_FOUND`, or carry your own domain code in `details`
- `FORBIDDEN` — surfaced as `UNAUTHORIZED` (ACL denials too)
- `CONFLICT`, `CONCURRENCY_CONFLICT` — encode in `details.code`
- `CONCURRENCY_LIMIT` — surfaces as `TIMEOUT` or `SERVICE_UNAVAILABLE` from the adaptive layer
- `RETRY_BUDGET_EXHAUSTED` — there is no retry budget; the underlying error surfaces directly
- `RESPONSE_VALIDATION_FAILED`, `IDEMPOTENCY_MISMATCH` — not separate codes; surface via `INTERNAL` or `VALIDATION_FAILED`

For richer domain errors, attach a `details.code` (string) to the error envelope and switch on it in your code.

## See also

- [retry.md](./retry.md) — `retryOnCodes`
- [circuit-breaker.md](./circuit-breaker.md) — what counts as a failure
- [replay-protection.md](./replay-protection.md) — `REPLAY_DETECTED`
- [idempotency.md](./idempotency.md) — `IDEMPOTENT_REPLAY` semantics
