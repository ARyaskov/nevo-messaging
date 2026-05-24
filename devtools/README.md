# Nevo Messaging DevTools (Next.js 16)

Live dashboard for `@riaskov/nevo-messaging`. Streams events from the in-process `DevToolsBus` (or from NATS in distributed mode) and renders services, methods, errors, ACL and circuit-breaker pages in real time.

## Quick start

```bash
cd devtools
pnpm install
pnpm dev
# http://localhost:3499
```

## Pages

| Path | What's shown |
| --- | --- |
| `/` | Overview: totals, RPS, top-5 slowest, top-5 by errors, recent traffic |
| `/services` | All registered services with RPS, error rate, ACL badge |
| `/services/[name]` | Per-service drill-down: per-method p50/p95/p99/avg/rps, ACL rules, open circuits, recent errors |
| `/methods` | Top-N rankings: Slowest / Most called / Most errors / Worst error rate |
| `/errors` | Error timeline with service/code filters and top-error grouping |
| `/acl` | ACL inspector + interactive simulator — test how any (caller, method, topic) tuple is evaluated against a service's rules |
| `/circuits` | Live circuit-breaker dashboard with current state per (service, method) and recent transitions |

## API

| Endpoint | Use |
| --- | --- |
| `GET /api/events` | Server-Sent Events stream of live `DevToolsEvent`s |
| `GET /api/snapshot` | Recent events buffer (default last 500) |
| `GET /api/registry` | `{ services, circuits }` snapshot from `DevToolsRegistry` |
| `GET /api/circuits` | Circuit-breaker snapshot only |

## How it gets data

When you import the framework, controllers (`BaseMessageController`, signal-router decorator) and clients (`NevoNatsClient`, `NevoKafkaClient`, `NevoHttpClient`) publish `DevToolsEvent`s for every request. The DevTools UI consumes the same in-process `DevToolsBus`.

Each service that initializes a signal-router calls `DevToolsRegistry.registerService(...)` — so the `/services` page knows about everything without manual setup.

The circuit breaker emits `circuit` events on every state transition (`closed → open → half-open → closed`).

## Multi-process mode

If you run more than one pod, point the DevTools app at NATS:

```bash
# Install the NATS peer packages first (nats.js v3 was split):
pnpm add @nats-io/transport-node @nats-io/nats-core

NEVO_DEVTOOLS_NATS_SERVERS="nats://nats-1:4222,nats://nats-2:4222" pnpm dev
```

The app will auto-attach a `NatsDevToolsAdapter` (subject `__nevo.devtools`) and ingest events from every connected service. Make sure every service also wires the bridge:

```ts
import { wireDevToolsToNatsByConfig } from "@riaskov/nevo-messaging"
await wireDevToolsToNatsByConfig({ servers: ["nats://nats:4222"], bridgeLocalEvents: true })
```

Events stamp `origin = instanceId`, so each pod can be filtered.

## Embedded mode

If you want the DevTools UI inside your service process, mount the Next.js app on a sub-route or proxy it. Everything is plain Node-side React/Next, so it works under any reverse proxy.
