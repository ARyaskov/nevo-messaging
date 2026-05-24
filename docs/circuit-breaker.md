# Circuit breaker

A circuit breaker stops a caller from hammering a failing peer. Nevo ships two implementations:

- **Count breaker** (`CircuitBreakerRegistry`) — opens after N consecutive failures
- **Sliding-window breaker** (`SlidingCircuitBreakerRegistry`) — opens when the error rate within a time window crosses a threshold; better for high-RPS services

Both are keyed by `service:method`.

## Count breaker

```ts
interface CircuitBreakerOptions {
  enabled?: boolean                  // default: false
  failureThreshold?: number          // consecutive failures to open, default: 5
  resetTimeoutMs?: number            // time in open state, default: 30000
  halfOpenSuccessThreshold?: number  // consecutive successes in half-open to close, default: 2
}
```

```ts
import { CircuitBreakerRegistry } from "@riaskov/nevo-messaging"

const cb = new CircuitBreakerRegistry({
  enabled: true,
  failureThreshold: 5,
  resetTimeoutMs: 30_000,
  halfOpenSuccessThreshold: 2
})

// Manual use:
cb.before("user:user.getById")    // throws if open
try {
  const result = await callPeer()
  cb.onSuccess("user:user.getById")
} catch (err) {
  cb.onFailure("user:user.getById", err)
  throw err
}
```

In practice you don't call this manually — pass the options to the transport client and the client wires it in.

## Sliding-window breaker

```ts
interface SlidingCircuitOptions extends CircuitBreakerOptions {
  windowMs?: number             // rolling window, default: 10000
  bucketMs?: number             // bucket granularity, default: 1000
  errorRateThreshold?: number   // 0..1 fraction, default: 0.5
  minSampleSize?: number        // minimum calls before opening, default: 20
}
```

```ts
import { SlidingCircuitBreakerRegistry } from "@riaskov/nevo-messaging"

const cb = new SlidingCircuitBreakerRegistry({
  enabled: true,
  windowMs: 10_000,
  bucketMs: 1_000,
  errorRateThreshold: 0.5,
  minSampleSize: 20,
  resetTimeoutMs: 30_000,
  halfOpenSuccessThreshold: 3
})
```

Why prefer this: the consecutive-failure rule misbehaves at high RPS. At 1000 RPS a 5% error rate looks identical to a complete outage when measured in micro-windows. The sliding-window variant only trips on a sustained error rate.

## States

```
                error rate / count crossed threshold
    closed ─────────────────────────────────────────► open
      ▲                                                 │
      │                                                 │ resetTimeoutMs elapsed
      │ halfOpenSuccessThreshold probes succeeded       ▼
      └────────────────── half-open ◄──────── single probe traffic
                              │
                              │ any probe failed
                              ▼
                             open
```

- **Closed** — calls flow, errors are counted.
- **Open** — calls reject immediately with `ErrorCode.CIRCUIT_OPEN`.
- **Half-open** — a single probe is allowed; on success, more probes; after `halfOpenSuccessThreshold` consecutive successes, close.

## Inspecting state

```ts
const snap = cb.snapshot()
// → Array<{ key: "service:method", state: "closed"|"open"|"half-open", ...counters }>
```

The [DevTools dashboard](./devtools.md) renders this on the Circuits page.

## What counts as a failure

By default, any rejection from the underlying call counts. Transport-level errors (`TIMEOUT`, `CONNECTION_LOST`, `SERVICE_UNAVAILABLE`) always count. Validation errors (`VALIDATION_FAILED`) do not, because they signal a client bug, not a peer fault.

## Combining with retry

The breaker sees retries as separate attempts. If `retry.maxAttempts: 3` and the breaker opens after attempt 2, attempt 3 rejects immediately with `CIRCUIT_OPEN`. The original error is wrapped on the breaker rejection.

## What is not provided

- Per-tenant keying (`keyBy`) — keys are positional `service:method`. To partition by tenant, you would need to extend the breaker.
- Manual `forceOpen()` / `forceClose()` — there is no kill switch API today. Toggle the `enabled` flag in your options to disable the entire registry if you need an emergency override.

## See also

- [retry.md](./retry.md) — how retries interact with the breaker
- [hedging.md](./hedging.md) — for latency-driven fallbacks, not failure-driven
- [adaptive.md](./adaptive.md) — auto-tune retry/timeout based on observed p99
