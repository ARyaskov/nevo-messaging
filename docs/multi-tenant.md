# Multi-tenancy

Multi-tenant deployments isolate one tenant's traffic from another's: fair scheduling, per-tenant rate limits, per-tenant ACL.

## Propagating `tenantId`

Pass the tenant ID through `meta.tenantId` on every outbound call:

```ts
const user = await this.query(
  "user", "user.getById", { id: 1n },
  { meta: { tenantId: "t-42" } }
)
```

The rate limiter recognises `tenantId` as a first-class keying dimension (see below). For tracing/metrics, attach `tenantId` to your spans/metrics manually.

## Per-tenant rate limit

```ts
import { RateLimit } from "@riaskov/nevo-messaging"

@RateLimit({
  capacity: 100,
  refillPerSec: 50,
  keyBy: ["tenantId"]
})
async create(input: CreateDto) { ... }
```

Or as a client option:

```ts
createNevoNatsClient(["USER"], {
  clientIdPrefix: "frontend",
  rateLimit: {
    enabled: true,
    capacity: 100,
    refillPerSec: 50,
    keyBy: ["method", "tenantId"]
  }
})
```

See [rate-limiting.md](./rate-limiting.md). Buckets are partitioned by the combination of dimensions in `keyBy`.

## Per-tenant filters on subscriptions

```ts
await this.subscribe(
  "orders", "orders.placed",
  { filters: { meta: { tenantId: "t-42" } } },
  handler
)
```

See [subscription-filters.md](./subscription-filters.md).

## Per-tenant ACL

```ts
@NatsSignalRouter([UserService], {
  accessControl: {
    rules: [
      { topic: "*", method: "user.export", allow: ["paid-tier"] }
    ]
  }
})
```

The caller identity comes from `meta.callerService` (or a verified JWT `sub`). To express tenant tiers, encode them in the caller service name (`frontend-paid`, `frontend-free`) or in JWT claims and use them in your rules.

## Per-tenant circuit breakers

Not built in — the circuit breaker keys on `service:method`, not `(service, method, tenant)`. A noisy tenant trips the breaker for everyone.

To isolate, run **one breaker registry per tenant** in your application layer, or extend the framework's breaker to honor a tenant key. The framework does not provide a `keyBy` for breakers today.

## Topic partitioning

Kafka topic names are derived from service names — `topicMap` is not a feature. To shard by tenant, run a separate consumer group per tenant or include the tenant ID in the message and partition by key.

## What is NOT provided

- **No `defaultMeta` factory** on the client. Pass `meta` per call, or wrap the client in a small per-tenant helper.
- **No `runWithTenant` AsyncLocalStorage helper.** Use Node's own `AsyncLocalStorage` if you want context propagation across awaits.
- **No fair-scheduling concurrency limiter with tenant keys.** Use rate limiting for fairness; concurrency control today is a single `BackpressureLimiter`.

## Pattern: thin per-tenant facade

If you do most of your work per-tenant, wrap the client:

```ts
class TenantClient {
  constructor(private base: NatsClientBase, private tenantId: string) {}
  query<T>(service: string, method: string, params: unknown, opts?: Record<string, unknown>) {
    return this.base.query<T>(service, method, params, {
      ...opts,
      meta: { ...(opts as any)?.meta, tenantId: this.tenantId }
    })
  }
}
```

This is more reliable than depending on AsyncLocalStorage across many libraries.
