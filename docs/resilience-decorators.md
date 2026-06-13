# Resilience decorators

Resilience primitives (`hedge()`, `CircuitBreakerRegistry`, `SlidingCircuitBreakerRegistry`, `AdaptiveTuner`, `BackpressureLimiter`) have always been available as functions and classes. As of v2.3 there is also a **declarative** form: four method decorators that the framework reads and applies for you.

You don't have to use them — the functional API still works exactly as before. They exist for two reasons:

1. **Less boilerplate at the call site.** No more `try { breaker.before(); … } finally { breaker.onSuccess/Failure() }` blocks.
2. **One place to layer features.** A single method can declare hedging + sliding-window breaker + adaptive timeouts + backpressure, and the runtime composes them in the right order.

## The four decorators

| Decorator | Wraps | When to use |
|---|---|---|
| `@Hedge(opts)` | `hedge(fn, opts)` | Idempotent reads with long-tail latency |
| `@CircuitBreaker(opts)` | `SlidingCircuitBreakerRegistry` (default) or `CircuitBreakerRegistry` | Stop hammering a failing peer |
| `@Adaptive(opts)` | `AdaptiveTuner.observe(durationMs, ok)` | Auto-tune retries/timeout against observed p99 |
| `@Backpressure(opts)` | `BackpressureLimiter.begin()/end()` + auto pause/resume on the subscription | Subscribe handlers slower than the broker |

Each decorator only stores metadata (via `Reflect.defineMetadata`). The runtime in `resilience-runtime.ts` reads it lazily.

## Quick example

```ts
import { Injectable, Inject } from "@nestjs/common"
import {
  NatsClientBase, NevoNatsClient,
  Hedge, CircuitBreaker, Adaptive, Backpressure
} from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends NatsClientBase {
  constructor(@Inject("NEVO_NATS_CLIENT") client: NevoNatsClient) {
    super(client)
  }

  // Idempotent read: hedge it, guard with sliding-window breaker,
  // and let the adaptive tuner pick the timeout/retries.
  @Hedge({ copies: 1, delayMs: 50 })
  @CircuitBreaker({ mode: "sliding", windowMs: 10_000, errorRateThreshold: 0.5, minSampleSize: 20 })
  @Adaptive({ targetP99Ms: 250 })
  async getById(id: bigint) {
    return this.query("user", "user.getById", { id })
  }

  // Subscribe handler with explicit backpressure cap. When in-flight crosses
  // 160, the subscription is paused; it resumes below 80. Overflow nacks so
  // the broker redelivers.
  @Backpressure({ maxInflight: 200, highWatermark: 160, lowWatermark: 80, onOverflow: "nack" })
  async onUserUpdated(msg: { id: bigint }) {
    await project(msg)
  }
}
```

## How the runtime composes them

When more than one decorator is declared on the same method, the runtime layers them as:

```
backpressure  ──►  circuit-breaker  ──►  hedge  ──►  invoke
                                                       │
                                            adaptive observes here
```

Concretely:

1. `applyResilience()` admits the call via the limiter (or rejects if at cap).
2. The circuit breaker's `before(key)` runs — throws `CIRCUIT_OPEN` if open.
3. `hedge()` fans out parallel attempts when `copies > 1`; otherwise calls once.
4. The `invoke(attempt, signal)` you supplied actually runs.
5. After return/throw, the runtime feeds latency to `AdaptiveTuner.observe()` and the breaker's `onSuccess/onFailure(key, err)`.
6. The limiter's `end()` runs in `finally`.

The key for circuit-breaker, adaptive, and backpressure registries is `service:method` — so multiple concurrent calls and retries share state automatically.

## Inspecting state

```ts
import { snapshotResilience } from "@riaskov/nevo-messaging"

const snap = snapshotResilience()
// {
//   adaptive: { "user:user.getById": { p50, p95, p99, errorRate, retries, timeoutMs } },
//   sliding:  { "user:user.getById": { state, errorRate, sampleSize } },
//   backpressure: { "user:user.onUserUpdated": { inflight, paused } }
// }
```

Export to dashboards via `prom-client` gauges, or pull through the [DevTools UI](./devtools.md).

## Lower-level helpers

If you're building a custom router and need to compose resilience yourself:

```ts
import {
  readMethodResilience,
  applyResilience,
  wrapMethodWithResilience,
  makeResilienceRunner
} from "@riaskov/nevo-messaging"

// One-shot: read decorators once, get a wrapped callable.
const fn = wrapMethodWithResilience(service, "getById",
  (id: bigint) => service.getById(id),
  (id) => ({ key: `user:user.getById:${id}` })
)

// Or run the same config many times:
const run = makeResilienceRunner(service, "getById")
if (run) {
  const result = await run("user:user.getById", (attempt, signal) => doWork(attempt, signal))
}
```

For subscribe handlers, the equivalent is `wrapSubscriptionHandler` / `installBackpressureFromDecorator` from [backpressure.md](./backpressure.md).

## When to *not* use decorators

The functional helpers remain the right choice when:

- You need fine-grained control over which call sites get hedged inside a single method.
- You want to share one `AdaptiveTuner` across multiple methods on the same client.
- You're outside of NestJS DI (decorators rely on `reflect-metadata`).

Mix freely — there is no enforcement that you pick one form.

## See also

- [hedging.md](./hedging.md)
- [circuit-breaker.md](./circuit-breaker.md)
- [adaptive.md](./adaptive.md)
- [backpressure.md](./backpressure.md)
- [method-decorators.md](./method-decorators.md) — `@RateLimit`, `@Cacheable`, `@Schema`
