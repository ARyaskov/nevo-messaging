# Resilience decorators example

A self-contained NATS-backed microservice that exercises the four declarative
resilience decorators added in `@riaskov/nevo-messaging` v2.3, plus the
companion features:

| Feature | Where in `src/main.ts` |
|---|---|
| `@Hedge` (latency-driven duplicate) | `CatalogService.getProduct` |
| `@CircuitBreaker` (sliding-window) | `CatalogService.getProduct` |
| `@Adaptive` (p99-driven tuning) | `CatalogService.getProduct` |
| `@Backpressure` (high/low-watermark gate) | `CatalogService.onProductUpdated` |
| `RedisIdempotencyStore` (distributed L2 cache) | `buildRedisIdempotency()` |
| `ConsulDiscoveryProvider` | `wireDiscovery()` |
| `KubernetesDnsDiscoveryProvider` | `wireDiscovery()` |
| `snapshotResilience()` for live observation | `bootstrap()` `setInterval` |

## Run

```bash
# Brokers
docker run --rm -p 4222:4222 nats:2 -js &
docker run --rm -p 6379:6379 redis:7 &

# Install + start
cd examples/resilience-decorators
pnpm install
NATS_URL=nats://127.0.0.1:4222 REDIS_URL=redis://127.0.0.1:6379 pnpm start
```

To exercise discovery providers, also set:

```bash
CONSUL_URL=http://consul.service.consul:8500 CONSUL_TOKEN=...
# or
K8S_DISCOVER_SERVICES=catalog,billing
```

## What to look for

- Every five seconds the bootstrap logs a `resilience snapshot` line; you can
  watch the sliding-window breaker open/close, the adaptive tuner's
  `currentRetries`/`currentTimeoutMs` drift, and the backpressure inflight
  counter rise under load.
- Re-issuing the same NATS query with the same UUID returns instantly from
  Redis on every replica (L2 hit), avoiding a duplicate handler run.

See [docs/resilience-decorators.md](../../docs/resilience-decorators.md) for
the full feature reference.
