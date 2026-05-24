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
