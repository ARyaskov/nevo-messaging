# Adaptive tuner

`AdaptiveTuner` adjusts **retry count** and **timeout** at runtime based on observed call latency. It is not a concurrency controller — it does not change in-flight caps. Use it when you do not want to hand-tune retry/timeout knobs and prefer the framework to nudge them toward what the network is actually doing.

## Real API

```ts
interface AdaptiveOptions {
  enabled?: boolean        // default: false
  windowMs?: number        // observation window, default: 30000
  targetP99Ms?: number     // aim for p99 under this (ms), default: 500
  minRetries?: number      // floor for tuned retries, default: 0
  maxRetries?: number      // ceiling for tuned retries, default: 5
  minTimeoutMs?: number    // floor for tuned timeout, default: 100
  maxTimeoutMs?: number    // ceiling for tuned timeout, default: 30000
}

class AdaptiveTuner {
  constructor(opts?: AdaptiveOptions)
  isEnabled(): boolean
  observe(durationMs: number, ok: boolean): void
  getRetries(): number
  getTimeoutMs(): number
  snapshot(): { p50, p95, p99, samples, currentRetries, currentTimeoutMs }
}
```

## Usage

```ts
import { AdaptiveTuner } from "@riaskov/nevo-messaging"

const tuner = new AdaptiveTuner({
  enabled: true,
  windowMs: 30_000,
  targetP99Ms: 500,
  minRetries: 1,
  maxRetries: 4,
  minTimeoutMs: 100,
  maxTimeoutMs: 10_000
})

// On every call:
const start = performance.now()
let ok = true
try {
  await callPeer({
    retries: tuner.getRetries(),
    timeoutMs: tuner.getTimeoutMs()
  })
} catch (e) {
  ok = false
  throw e
} finally {
  tuner.observe(performance.now() - start, ok)
}
```

The tuner maintains a rolling histogram. When p99 latency moves above `targetP99Ms`, it shortens the timeout and reduces retries (so failing calls don't pile up). When p99 falls below `targetP99Ms`, it grows the timeout and adds back retries (so transient errors are absorbed). Bounds are honored.

## When to use this

- The downstream peer's capacity drifts (time-of-day, deployments, GC pauses)
- You don't want to hand-tune `timeoutMs` per service
- You're OK trading a little throughput stability for adaptation

A fixed timeout is still appropriate when an SLA contract dictates the cap.

## Inspecting

```ts
const snap = tuner.snapshot()
// → { p50: 30, p95: 120, p99: 450, samples: 1834, currentRetries: 2, currentTimeoutMs: 600 }
```

Export these as gauges for dashboards — see [metrics.md](./metrics.md).

## Declarative form — `@Adaptive`

You no longer have to wire `observe()` by hand. Annotate the method:

```ts
import { Adaptive } from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends NatsClientBase {
  @Adaptive({ targetP99Ms: 250, minRetries: 1, maxRetries: 4 })
  async getById(id: bigint) {
    return this.query("user", "user.getById", { id })
  }
}
```

The resilience runtime starts a wall-clock at the outermost wrapper, calls the inner work, and feeds `observe(durationMs, ok)` automatically when it returns or throws. The tuner state is keyed by `service:method`, so retries and parallel calls share one rolling histogram per method.

To read the tuner state at runtime, use `snapshotResilience().adaptive[key]` — see [resilience-decorators.md](./resilience-decorators.md).

## What is *still* not provided

- **No concurrency limiting.** The tuner adjusts retry/timeout, not in-flight count. For concurrency caps see [backpressure.md](./backpressure.md).
- **No `ErrorCode.CONCURRENCY_LIMIT`.** Calls that hit the tuner's bounds still get the same `TIMEOUT` or peer-reported error codes; the tuner only reshapes the parameters.

## See also

- [retry.md](./retry.md) — fixed retry policy
- [backpressure.md](./backpressure.md) — limit in-flight calls / pause subscriptions
- [circuit-breaker.md](./circuit-breaker.md) — open the circuit when adaptation is not enough
