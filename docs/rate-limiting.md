# Rate limiting

Token-bucket rate limiter with configurable keying. Used to throttle a single noisy caller, tenant, or method without affecting the rest.

## Real API

```ts
type RateLimiterScope = "request" | "subscription"
type RateLimitKeyDimension = "service" | "method" | "callerService" | "tenantId"

interface RateLimiterOptions {
  enabled?: boolean
  capacity?: number            // bucket size (max burst), default: 100
  refillPerSec?: number        // sustained rate, default: 50
  keyBy?: RateLimitKeyDimension[]   // dimensions to combine into the bucket key
  keyExtractor?: (ctx) => string    // or a custom key function (overrides keyBy)
  scopes?: RateLimiterScope[]       // default: ["request"]
  maxEntries?: number               // LRU cap on number of buckets, default: 10000
  idleEvictMs?: number              // evict bucket after this many ms idle, default: 600000
}

class RateLimiter {
  isEnabled(): boolean
  check(ctx: { service?, method?, callerService?, tenantId? }): {
    allowed: boolean
    remaining: number
    retryAfterMs?: number
  }
  stop(): void
  snapshot(): { entries: number, buckets: Array<{...}> }
}
```

The class is constructed via `resolveRateLimiter(opts)` which returns a singleton-style limiter, or you can `new RateLimiter(opts)` directly.

## Client-side rate limit

```ts
import { resolveRateLimiter } from "@riaskov/nevo-messaging"

createNevoNatsClient(["USER"], {
  clientIdPrefix: "frontend",
  rateLimit: {
    enabled: true,
    capacity: 200,
    refillPerSec: 100,
    keyBy: ["method", "tenantId"]    // one bucket per (method, tenant)
  }
})
```

A call that fails the bucket check rejects with `ErrorCode.RATE_LIMITED`.

## Per-method on a service (decorator)

```ts
import { Injectable, Inject } from "@nestjs/common"
import { NatsClientBase, NevoNatsClient, RateLimit } from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends NatsClientBase {
  constructor(@Inject("NEVO_NATS_CLIENT") c: NevoNatsClient) { super(c) }

  @RateLimit({ capacity: 50, refillPerSec: 10, keyBy: ["tenantId"] })
  async create(input: CreateInput) { ... }
}
```

The decorator stores its config in metadata; the framework reads it at dispatch time and applies the limit before the handler runs.

## Custom key extractor

```ts
rateLimit: {
  enabled: true,
  capacity: 100,
  refillPerSec: 50,
  keyExtractor: (ctx) => `${ctx.tenantId ?? "_"}::${ctx.method}`
}
```

`keyExtractor` is invoked per call. Make it cheap — it runs on every dispatch.

## What does `keyBy` do

`keyBy: ["method", "tenantId"]` means buckets are partitioned by the **combination** of method and tenant. With 5 methods and 100 tenants you can have up to 500 buckets. The LRU eviction (`maxEntries`, `idleEvictMs`) keeps memory bounded.

## Buckets

Each bucket is a classic token bucket:

- Holds up to `capacity` tokens
- Refills at `refillPerSec` tokens per second
- Each call consumes one token

So `capacity: 100, refillPerSec: 50` allows bursts up to 100 and a sustained 50/s.

## Scopes

`scopes: ["request"]` (default) limits incoming requests on the server side. `scopes: ["subscription"]` would gate subscription message dispatch. They are independent.

## Response when limited

The framework rejects with `ErrorCode.RATE_LIMITED`. The `check()` result includes `retryAfterMs` — clients (e.g. HTTP) can surface it as `Retry-After`.

## Metrics

The limiter increments:

- One counter per `(key, outcome)` — allowed vs rejected
- A gauge of active buckets

Inspect with `limiter.snapshot()` or via the [metrics](./metrics.md) registry.

## What is not provided

- **No distributed store.** The limiter is in-process per Node instance. For a cluster-wide budget you would write a shared store on top — it is not built in.
- **No "rules per method" config object.** Use either `keyBy: ["method"]` (one bucket per method, same capacity for all), or apply the `@RateLimit` decorator per method with different capacities.
- **No automatic mapping to HTTP `429`.** The HTTP transport reflects `ErrorCode.RATE_LIMITED` in the error envelope; your gateway / wrapper translates it to a `429` if needed.

## See also

- [method-decorators.md](./method-decorators.md) — `@RateLimit` and `@Cacheable`
- [backpressure.md](./backpressure.md) — pause-based, not token-bucket
- [adaptive.md](./adaptive.md) — auto-tune retries/timeouts
