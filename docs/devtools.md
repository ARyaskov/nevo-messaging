# DevTools UI dashboard

The Nevo DevTools dashboard visualizes events flowing through the framework: requests, replies, errors, retries, circuit transitions, ACL denials, top-N slow methods.

It ships **inside this package** as prebuilt static assets plus a tiny `node:http` server, so any project that already depends on `@riaskov/nevo-messaging` can open it with no extra install and no source checkout.

## Running it

```bash
npx nevo-devtools
# http://localhost:3499
```

| Option | Default | Env |
| --- | --- | --- |
| `-p, --port <n>` | `3499` | `NEVO_DEVTOOLS_PORT` |
| `--host <addr>` | `127.0.0.1` | `NEVO_DEVTOOLS_HOST` |
| `-n, --nats <urls>` | `nats://127.0.0.1:4222` | `NEVO_DEVTOOLS_NATS_SERVERS`, then `NATS_URL` |
| `--subject <s>` | `__nevo.devtools` | — |
| `--no-nats` | — | — |

It binds to loopback by default: the dashboard exposes service topology, ACL rules and event bodies.

## Connecting to a stack

The dashboard reads events from a `DevToolsBus`. There are two ways to fill it.

### Cluster (the usual case)

Each service publishes to a NATS subject; `nevo-devtools` subscribes to it. Wire the bridge once per service, at startup:

```ts
import { wireDevToolsToNatsByConfig } from "@riaskov/nevo-messaging"

await wireDevToolsToNatsByConfig({
  servers: ["nats://127.0.0.1:4222"],
  bridgeLocalEvents: true
})
```

Then `npx nevo-devtools` against the same NATS. Events carry `origin = instanceId`, so replicas stay distinguishable.

Registration events (`/services`) are emitted **once, at service startup**, and NATS core does not replay them. Starting the dashboard after your services leaves that page empty until a service restarts; live traffic pages fill in immediately either way. `GET /api/health` reports whether the NATS bridge attached.

### In-process (single Node app)

Mount the same server inside your own process and it reads that process's bus directly — no NATS, no separate port to remember:

```ts
import { startDevToolsUiServer } from "@riaskov/nevo-messaging"

const devtools = await startDevToolsUiServer({ port: 3499 })
// ... later
await devtools.close()
```

Prefer raw access? `getDevToolsBus()` gives you `recent()`, `size()` and `on(handler)` to build your own endpoints.

## HTTP API

The UI is a static bundle talking to these endpoints; they are equally usable from `curl`.

| Endpoint | Use |
| --- | --- |
| `GET /api/events` | Server-Sent Events stream of live `DevToolsEvent`s |
| `GET /api/snapshot?limit=N` | Recent events buffer (default 500) |
| `GET /api/registry` | `{ services, circuits }` snapshot |
| `GET /api/circuits` | Circuit-breaker snapshot only |
| `GET /api/config?service=` | Registered service info, or all services |
| `POST /api/config` | Replace a service's ACL at runtime |
| `POST /api/replay` | Record a `replay-requested` custom event |
| `GET /api/health` | Whether the NATS bridge attached, and to which servers |

## `DevToolsBus` API

```ts
type DevToolsDropStrategy = "drop-oldest" | "drop-newest" | "back-pressure"

interface DevToolsRingOptions {
  maxEvents?: number          // ring size (default: 5000)
  originId?: string           // self-identifier
  batchFlushMs?: number       // batch flush cadence
  dropStrategy?: DevToolsDropStrategy
  onBackpressure?: (info: { dropped: number }) => void
}

class DevToolsBus {
  publish(event: DevToolsEvent): void
  ingestRemote(event: DevToolsEvent): void
  recent(limit?: number): DevToolsEvent[]
  size(): number
  capacityHint(): number
  on(handler: (e: DevToolsEvent) => void): () => void           // unsubscribe fn
  onLocal(handler: (e: DevToolsEvent) => void): () => void
  onWeak(holder: object, handler: (e: DevToolsEvent) => void): void
  drain(): DevToolsEvent[]
}
```

### Drop strategies

- `drop-oldest` (default) — newest event always recorded; the oldest is evicted
- `drop-newest` — preserves history under load
- `back-pressure` — caller blocks via `onBackpressure` callback (use only in development)

### WeakRef-style subscriptions

`bus.onWeak(holder, handler)` keeps a weak reference to the `holder` object. When `holder` is garbage-collected, the subscription is removed automatically (via `FinalizationRegistry`). Use this if you can't remember to unsubscribe.

## Event shape

```ts
interface DevToolsEvent {
  ts: number
  type: "request" | "response" | "error" | "circuit" | "discovery" | "rate-limit" | "custom"
  service?: string
  method?: string
  uuid?: string
  durationMs?: number
  status?: number | string
  error?: { code?: number; message?: string }
  origin?: string
  extra?: Record<string, unknown>
}
```

`publishClientEvent(bus, payload)` is a helper for transport drivers to emit `request` / `response` / `error` shapes.

## Registry

`DevToolsRegistry` holds a snapshot of currently-known services and circuit states:

```ts
import { getDevToolsRegistry } from "@riaskov/nevo-messaging"

const reg = getDevToolsRegistry()
reg.listServices()
reg.listCircuits()
```

The bus feeds the registry; the dashboard reads from the registry for "static" views (service list, current circuit states) and from the bus for streaming events.

## Dashboard pages

| Page | Purpose |
|---|---|
| `/` Overview | Throughput + error rate + top services |
| `/services/[name]` | Methods, latencies, recent calls |
| `/methods` | Top-N slow / failing methods |
| `/circuits` | Circuit state per service+method |
| `/acl` | Recent ACL denials |
| `/errors` | Error feed with replay buttons |
| `/trace/[uuid]` | In-process trace tree for a single uuid |

## What is NOT provided

- **No `configureDevTools(...)` global config function.** Drop strategy and buffer size go through the bus constructor, which is wired by the framework.
- **No `mountDevToolsApi(app, ...)` helper.** Expose the bus over HTTP yourself in a controller.
- **No live config edit endpoint.** The dashboard is read-only over the network; changing limits requires editing the service.

## Security

The bus contains redacted message envelopes — but still includes service names, methods, and timing. In production:

- Put the dashboard behind a reverse proxy that enforces auth
- Or restrict it to a private network
- Or skip running it in prod entirely and rely on metrics + traces

## See also

- [metrics.md](./metrics.md) — pull-based
- [observability.md](./observability.md) — OTel traces
- [redaction.md](./redaction.md) — what gets stripped before publish
