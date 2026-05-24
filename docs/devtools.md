# DevTools UI dashboard

The Nevo DevTools dashboard is a Next.js 16 app that visualizes events flowing through the framework: requests, replies, errors, retries, circuit transitions, ACL denials, top-N slow methods. The dashboard lives in `devtools/` of the repo.

## Running it

```bash
cd devtools
npm install
npm run dev
# http://localhost:3000
```

## Connecting to a stack

The dashboard reads events from a `DevToolsBus`. There are two ways to provide one:

### In-process (single Node app)

Use the shared bus from the same process:

```ts
import { getDevToolsBus, DevToolsBus } from "@riaskov/nevo-messaging"

const bus: DevToolsBus = getDevToolsBus()
```

Mount your own HTTP endpoints over `bus.recent()`, `bus.size()`, and a Server-Sent Events stream that calls `bus.on(handler)`. There is no `mountDevToolsApi(app, ...)` helper today — you wire up endpoints in your transport controller.

### Cluster (multiple replicas)

Run an adapter that publishes / ingests events over a transport. The framework exposes a `DevToolsAdapter` interface (`{ attach() }`) and you implement it for your transport. NATS, Kafka, and HTTP adapters live in transport-specific submodules; check `devtools-registry.ts` and the relevant transport for the attached adapter class.

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
