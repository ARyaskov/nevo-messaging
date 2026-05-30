# Service discovery

Each Nevo client publishes a heartbeat envelope to a shared discovery topic. Other clients build a local registry from those heartbeats.

## Heartbeat shape

```ts
interface DiscoveryAnnouncement {
  serviceName: string
  instanceId: string
  clientId?: string
  transport: "nats" | "kafka" | "http" | "socket-io" | "websocket"
  ts: number
  host?: string
  port?: number
  version?: string
  capabilities?: string[]   // derived from @Signal declarations
  meta?: Record<string, unknown>
}
```

`capabilities` is built from each registered `@Signal("user.getById", ...)` plus its `@vN` versions.

## Defaults by transport

| Transport | Default | Topic / endpoint |
|---|---|---|
| NATS | enabled | `__nevo.discovery` |
| Kafka | enabled | `__nevo.discovery` (compacted topic recommended) |
| Socket.IO | opt-in | broadcast room `__nevo.discovery` |
| HTTP | opt-in via coordinator URL | per-client heartbeat to coordinator |
| WebSocket | opt-in | frame `__nevo.discovery` |

## Configuration

```ts
createNevoKafkaClient(["USER"], {
  clientIdPrefix: "frontend",
  discovery: {
    enabled: true,
    heartbeatIntervalMs: 5_000,
    ttlMs: 15_000          // instance considered stale after this
  }
})
```

For NATS the equivalent fields live on the underlying client options.

## Reading the registry

The transport client exposes registry accessors:

```ts
const services = client.getAvailableServices()        // string[]
const present  = client.isServiceAvailable("user")    // boolean
const all      = client.getDiscoveredServices()        // Map<service, instances>
```

For lower-level access:

```ts
import { DiscoveryRegistry } from "@riaskov/nevo-messaging"

const reg = new DiscoveryRegistry()
reg.update({ serviceName: "user", instanceId: "u-1", transport: "nats", ts: Date.now(), ... })
reg.startBackgroundPrune(15_000)
const entries = reg.list()
```

`DiscoveryRegistry.list()` returns `DiscoveryEntry[]` — same shape as the announcement plus `lastSeen`.

## Stale entries

`startBackgroundPrune(ttlMs)` deletes entries with `lastSeen` older than `ttlMs`. Stop the timer with `stopBackgroundPrune()` on shutdown — pair with [graceful-shutdown.md](./graceful-shutdown.md).

`heartbeatIntervalMs` shorter than `ttlMs / 3` avoids false-positive removals during transient packet loss.

## Capability negotiation

```ts
// Caller decides which version to request
const u = await this.query("user", "user.getById", { id: 1n }, {
  meta: { version: "v2" }
})

// Or check first
const entry = client.getDiscoveredServices().get("user")?.[0]
if (entry?.capabilities?.includes("user.getById@v2")) {
  // use v2
}
```

## HTTP discovery

There is no native broker for HTTP — the typical setup is a coordinator service that aggregates heartbeats. Clients post heartbeats to the coordinator and pull the registry from it.

```ts
createNevoHttpClient({ coordinator: "http://discovery.internal:8091" }, { clientIdPrefix: "frontend" })
```

## Privacy

Heartbeats may contain hostnames. In hostile environments, scrub the host or restrict the discovery topic with [ACL](./access-control.md).

## External providers — Consul, Kubernetes DNS

The broker-side heartbeat is fine for in-cluster discovery, but in a polyglot or k8s-native environment it may be redundant or even unwanted. Two pluggable `DiscoveryProvider` adapters ship out of the box. Both feed entries into the same `DiscoveryRegistry`, so the rest of the framework consumes them identically to heartbeats.

### Consul

```ts
import { DiscoveryRegistry, ConsulDiscoveryProvider, attachDiscoveryProvider } from "@riaskov/nevo-messaging"

const registry = new DiscoveryRegistry()
const provider = new ConsulDiscoveryProvider({
  url: "http://consul.service.consul:8500",
  serviceNames: ["user", "billing"],
  pollIntervalMs: 5_000,
  // Use Consul blocking-queries for push-style updates:
  waitMs: 30_000,
  token: process.env.CONSUL_TOKEN
})

const detach = await attachDiscoveryProvider(registry, provider)
// ... later, on shutdown:
await detach()
```

Each passing health check from `/v1/health/service/<name>?passing=true` becomes a `DiscoveryAnnouncement`. Service tags are exposed as `capabilities`, and meta keys land in `entry.meta`.

No `consul` npm dependency is required — the provider uses the built-in `fetch`.

### Kubernetes DNS

For headless services (`clusterIP: None`) DNS returns one A record per pod. The provider polls and turns each address into a registry entry.

```ts
import { KubernetesDnsDiscoveryProvider, attachDiscoveryProvider, DiscoveryRegistry } from "@riaskov/nevo-messaging"

const registry = new DiscoveryRegistry()
const provider = new KubernetesDnsDiscoveryProvider({
  services: [
    { name: "user", namespace: "prod", port: 8080 },
    // SRV-style: resolves _http._tcp.billing.prod.svc.cluster.local
    { name: "billing", namespace: "prod", portName: "http" }
  ],
  clusterDomain: "svc.cluster.local",
  pollIntervalMs: 10_000
})

await attachDiscoveryProvider(registry, provider)
```

Behavioural notes:

- A-record lookup (no `portName`) yields one announcement per pod IP, with `port` taken from the option.
- SRV lookup (`portName: "http"`) reads `_http._tcp.<svc>` and uses the port from the SRV record. `priority`/`weight` land in `entry.meta`.
- DNS failures are logged via the framework logger and surface as "no entries for this service" — callers still see the previous-cycle entries until they fall out of the TTL.

### Writing your own provider

```ts
interface DiscoveryProvider {
  readonly id: string
  start(sink: DiscoverySink): Promise<void> | void
  stop(): Promise<void> | void
}

interface DiscoverySink {
  replace(serviceName: string, entries: DiscoveryAnnouncement[]): void
  upsert(entry: DiscoveryAnnouncement): void
}
```

`replace` evicts entries the upstream source no longer reports; `upsert` is for streaming sources (watch-style APIs). Both go through the registry's public methods (`update`, `removeInstance`).
