# Retry policy

Retries protect callers from transient transport errors. Nevo's retry policy is exponential-with-jitter, configured on a transport client.

## Options

```ts
interface RetryOptions {
  enabled?: boolean        // default: false
  maxAttempts?: number     // including the initial call, default: 3
  baseMs?: number          // first backoff, default: 100
  maxMs?: number           // cap, default: 2000
  jitter?: boolean         // full jitter, default: true
  retryOnCodes?: number[]  // ErrorCode values that are retryable
}
```

`retryOnCodes` is a list of numeric `ErrorCode` values. If omitted, the framework's default whitelist (timeouts, transport errors, internal errors, service unavailable) applies via the `isRetryable(code)` helper.

## Client-wide

```ts
import { ErrorCode } from "@riaskov/nevo-messaging"

createNevoNatsClient(["USER"], {
  clientIdPrefix: "order",
  retry: {
    enabled: true,
    maxAttempts: 3,
    baseMs: 100,
    maxMs: 2_000,
    jitter: true,
    retryOnCodes: [ErrorCode.TIMEOUT, ErrorCode.CONNECTION_LOST, ErrorCode.SERVICE_UNAVAILABLE]
  }
})
```

## Per-call override

```ts
await this.query("user", "user.getById", { id: 1n }, {
  retry: { enabled: true, maxAttempts: 2 }
})
```

## Programmatic usage

The retry helpers are exported so you can wrap arbitrary async work:

```ts
import { withRetry, resolveRetryOptions } from "@riaskov/nevo-messaging"

const opts = resolveRetryOptions({ enabled: true, maxAttempts: 3, baseMs: 100 })
const result = await withRetry(() => doSomething(), opts)
```

`withRetry(fn, opts, signal?)` accepts an `AbortSignal` so you can interrupt a retry loop on shutdown.

## Backoff calculation

```
delay(attempt) = min(baseMs * 2^(attempt - 1), maxMs)
if (jitter) delay = random_in(0, delay)
```

Default settings produce delays in roughly `[0, 100)`, `[0, 200)`, `[0, 400)` ms between attempts.

## What gets retried

Only errors whose `ErrorCode` is in `retryOnCodes`. The built-in `isRetryable()` whitelist is:

- `TIMEOUT`
- `SERVICE_UNAVAILABLE`
- `CONNECTION_LOST`
- `INTERNAL`

Validation, ACL, replay, and idempotency errors are **never** retried — they indicate a bug or attack, not a transient fault.

## Combining with the circuit breaker

Retries do not bypass the [circuit breaker](./circuit-breaker.md). If the breaker is open, the retry attempt rejects immediately with `ErrorCode.CIRCUIT_OPEN` — the breaker treats the call as a single failed attempt, not as many.

## In-flight backoff (a different mechanism)

When several callers in the same process want to issue the **same** query, Nevo can stall duplicates so the in-flight one completes first. This is configured under `backoff` (not `retry`):

```ts
createNevoKafkaClient(["USER"], {
  clientIdPrefix: "frontend",
  backoff: {
    enabled: true,
    baseMs: 100,
    maxMs: 2_000,
    maxAttempts: 0,   // 0 = wait until slot is free
    jitter: true
  }
})
```

This is useful when a server is paused on a breakpoint or otherwise slow — duplicate calls do not stampede.

## Metrics

Every retried attempt increments `nevo_messaging_retries_total{service,method}`. Watch for sudden spikes — they usually point at an unhealthy downstream rather than a real load increase.

## What is not available

- No global "retry budget" or rate cap (don't retry more than X%). This level of control is intentionally left to the [circuit breaker](./circuit-breaker.md) and [adaptive tuner](./adaptive.md).
- No callback-style `retryOn` predicate — pass a list of `ErrorCode` numbers.
