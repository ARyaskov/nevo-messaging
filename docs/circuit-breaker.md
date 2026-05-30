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

One logical call is **one** breaker observation, no matter how many internal retries or hedged copies fire underneath it. The breaker records `before` once at the start of the call and a single `onSuccess`/`onFailure` at the end (see `runCircuitHedge` in `resilience-runtime.ts`) — so a call that retries three times and finally succeeds counts as one success, not two failures plus a success, and the breaker cannot flip to open *mid-retry* and reject a later attempt of the same call.

(This is a behaviour change from older versions, where each retry attempt was a separate breaker observation and the breaker could open between attempts.)

## Declarative form — `@CircuitBreaker`

`before/onSuccess/onFailure` is fine for transport-internal use, but bad for application code: it leaks the lifecycle into the call site. The annotation form does the wiring for you:

```ts
import { CircuitBreaker } from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends NatsClientBase {
  @CircuitBreaker({
    mode: "sliding",          // default — pass "count" for the simple variant
    windowMs: 10_000,
    errorRateThreshold: 0.5,
    minSampleSize: 20,
    resetTimeoutMs: 30_000
  })
  async getById(id: bigint) {
    return this.query("user", "user.getById", { id })
  }
}
```

The resilience runtime maintains one process-wide registry per mode, keyed by `service:method`, so multiple concurrent callers and retries share the same circuit state. `VALIDATION_FAILED` and `UNAUTHORIZED` still don't count as failures.

See [resilience-decorators.md](./resilience-decorators.md) for layering with `@Hedge` / `@Adaptive`.

## Per-tenant keying (`keyBy`)

The `@CircuitBreaker` decorator (like `@Backpressure` and `@Adaptive`) accepts a `keyBy` of `TenantKeyDimension[]` (`"service" | "method" | "callerService" | "tenantId"`), and the resilience runtime widens the breaker key with those dynamic dimensions:

```ts
@CircuitBreaker({ mode: "sliding", errorRateThreshold: 0.5, keyBy: ["service", "method", "tenantId"] })
async getById(id: bigint) {
  return this.query("user", "user.getById", { id })
}
```

Caveat: `keyBy` only takes effect when the call site supplies the tenant/caller dimensions to the runtime. The **built-in server router currently keys breakers as `service:method`** — it calls `applyResilience` with just `{ key: "service:method" }` and does not yet feed `tenantId` / `callerService` into the resilience context, so today a noisy tenant still trips the server-side breaker for everyone. The rate limiter, by contrast, *does* receive `tenantId` server-side. To isolate breakers per tenant today, drive the resilience runtime yourself (e.g. via `wrapMethodWithResilience` / `makeResilienceRunner` with a context that includes the tenant), or run one breaker registry per tenant in your application layer.

## What is *still* not provided

- Manual `forceOpen()` / `forceClose()` — there is no kill switch API today. Toggle the `enabled` flag in your options to disable the entire registry if you need an emergency override.

## See also

- [retry.md](./retry.md) — how retries interact with the breaker
- [hedging.md](./hedging.md) — for latency-driven fallbacks, not failure-driven
- [adaptive.md](./adaptive.md) — auto-tune retry/timeout based on observed p99
