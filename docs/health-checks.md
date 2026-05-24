# Health checks — liveness & readiness

Kubernetes (and most orchestrators) distinguishes:

- **Liveness** — is the process functioning? If not, restart it.
- **Readiness** — is the process ready to receive traffic? If not, remove from rotation.

Nevo separates the two and ships pluggable check builders for common dependencies.

## API

```ts
import {
  HealthRegistry,
  NEVO_HEALTH_METHOD, NEVO_LIVENESS_METHOD, NEVO_READINESS_METHOD,
  pgPing, redisPing, kafkaProducerPing, natsPing, httpPing,
  memoryUsagePing, eventLoopLagPing
} from "@riaskov/nevo-messaging"

const reg = new HealthRegistry({
  serviceName: "user",
  instanceId: process.env.HOSTNAME,
  version: "2.0.0"
})

reg.register("eventLoop", eventLoopLagPing(100), { kind: "liveness" })
reg.register("memory",    memoryUsagePing(1024),  { kind: "liveness" })

reg.register("pg",    pgPing(pgClient),                                 { kind: "readiness" })
reg.register("redis", redisPing(redisClient),                           { kind: "readiness" })
reg.register("nats",  natsPing(natsConnection),                         { kind: "readiness", timeoutMs: 2_000 })
reg.register("kafka", kafkaProducerPing(producer, { topic: "__health" }), { kind: "readiness" })
reg.register("auth",  httpPing("https://auth/healthz"),                 { kind: "readiness" })
```

`register(name, fn, opts?)`:

- `fn` is a function returning `Promise<HealthCheckResult>` (or sync)
- `opts.kind`: `"liveness"` | `"readiness"` | `"both"` (default `"both"`)
- `opts.timeoutMs`: per-check timeout — a check that hangs marks itself `down`, not the entire registry

`reg.unregister(name)` removes a check.

## Reserved methods

The framework exposes three built-in signals on every transport:

```ts
NEVO_HEALTH_METHOD    = "nevo.health"
NEVO_LIVENESS_METHOD  = "nevo.live"
NEVO_READINESS_METHOD = "nevo.ready"
```

Note the short names — `"nevo.live"` and `"nevo.ready"`, NOT `"nevo.liveness"` / `"nevo.readiness"`.

When the registry is attached to a controller, the framework auto-wires these three signals. They return:

```ts
{
  status: "ok" | "down",
  service: "user",
  instanceId: "user-abcd",
  version: "2.0.0",
  checks: { eventLoop: { status: "ok", latencyMs: 2 }, ... }
}
```

`liveness()` filters to checks of kind `"liveness"` or `"both"`. `readiness()` filters to `"readiness"` or `"both"`. `report()` returns everything.

## Pluggable pings

| Builder | Behavior |
|---|---|
| `pgPing(client, { sql? })` | Default `SELECT 1` |
| `redisPing(client)` | `PING` |
| `kafkaProducerPing(producer, { topic })` | Sends a tombstone to the topic |
| `natsPing(nc)` | RTT roundtrip |
| `httpPing(url, opts?)` | HTTP 2xx check |
| `memoryUsagePing(thresholdMb = 1024)` | `rss < threshold` |
| `eventLoopLagPing(thresholdMs = 100)` | `monitorEventLoopDelay()` p99 |

## Custom check

```ts
reg.register("billing-credits", async () => {
  const balance = await fetchBalance()
  return balance > 100
    ? { status: "ok", message: `${balance} credits` }
    : { status: "down", message: "low credits" }
}, { kind: "readiness", timeoutMs: 1_000 })
```

## Exposing via HTTP

If your service speaks HTTP, add a side-car endpoint:

```ts
@Controller()
export class HealthController {
  constructor(private readonly reg: HealthRegistry) {}

  @Get("/healthz") async live()  { return this.reg.liveness() }
  @Get("/readyz")  async ready() { return this.reg.readiness() }
}
```

Or, since `nevo.live` / `nevo.ready` are real wire signals, any client can call them like any other RPC:

```ts
await client.query("user", "nevo.ready", {})
```

## Kubernetes manifest

```yaml
livenessProbe:
  httpGet:  { path: /healthz, port: 8086 }
  initialDelaySeconds: 10
  periodSeconds: 10
readinessProbe:
  httpGet:  { path: /readyz, port: 8086 }
  initialDelaySeconds: 5
  periodSeconds: 5
```

## During shutdown

When [graceful shutdown](./graceful-shutdown.md) starts, register a readiness check that returns `down`:

```ts
reg.register("not-draining", () => ({
  status: shutdown.isShuttingDown() ? "down" : "ok",
  message: shutdown.isShuttingDown() ? "draining" : "ready"
}), { kind: "readiness" })
```

Kubernetes removes the pod from endpoints; liveness keeps the pod alive while it drains.

## Recommendations

- **Liveness should never call out.** A flaky DB should not kill your pod.
- **Readiness should match traffic dependencies.** If handlers need Postgres, Postgres goes in readiness.
- **Set `timeoutMs` per check.** A hung Redis must not block the probe.
