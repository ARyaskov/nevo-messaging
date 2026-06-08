# Transport capability matrix

Every transport exposes the same core surface (`query` / `emit` / `publish` / `subscribe` / `broadcast`) so application code is portable. But the transports differ in the *advanced* primitives they can offer — a request-reply broker like NATS can do fan-in `requestMany`; a request-only transport like HTTP/2 deliberately can't. This page is the accurate, per-client support matrix.

It reflects the actual client implementations under `src/transports/*/nevo-*.client.ts`, plus the in-memory transport in `src/transports/memory/`.

## At a glance

| Capability | NATS | Kafka | HTTP (SSE) | HTTP/2 | WebSocket | Socket.IO | Memory |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| `query` (request/reply) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `emit` (fire-and-forget) | ✅ | ✅ | ✅ | — | ✅ | ✅ | ✅ |
| `publish` (to subscribers) | ✅ | ✅ | ✅ | — | ✅ | ✅ | ✅ |
| `broadcast` | ✅ | ✅ | ✅ | — | ✅ | ✅ | ✅ |
| `subscribe` | ✅ | ✅ | ✅ (SSE) | — | ✅ | ✅ | ✅ |
| `subscribeWildcard` | ✅ | — | — | — | — | — | ✅ (pattern match) |
| `requestMany` (scatter/gather) | ✅ | — | — | — | — | — | — |
| Streaming | ✅ (JetStream pull) | ✅ (consumer groups) | ✅ (SSE one-way) | one-way response | ✅ (socket) | ✅ (socket) | — |
| JetStream | ✅ | — | — | — | — | — | — |
| Consumer `ack`/`nack` | JetStream only¹ | ✅ (offset commit)² | no-op³ | — | no-op³ | no-op³ | no-op³ |

¹ Core NATS `subscribe` exposes `ack`/`nack` on the context but they are **no-ops** (core NATS is fire-and-forget). Real acknowledgement comes from the JetStream helper. <br>
² Kafka `subscribe` performs a real offset commit on `ack` when you opt in with `{ ack: true }`; `nack` logs and lets the broker redeliver. <br>
³ The context still carries `ack`/`nack` for a uniform handler signature, but they do nothing — these transports don't model per-message acknowledgement.

## Per-transport notes

### NATS — the most capable transport

NATS is the reference transport and the only one with the full advanced surface:

- **`requestMany(serviceName, method, params, { maxResponses?, timeoutMs? })`** — scatter a request to every responder on a subject and gather up to `maxResponses` replies (or until the timeout). Use it for "ask all instances" patterns; the other transports have no equivalent.
- **`subscribeWildcard(pattern, handler, opts?)`** — subscribe with NATS subject wildcards (`*` for one token, `>` for the tail), e.g. `user.*` or `events.>`.
- **JetStream** — durable, at-least-once streaming via the `JetStreamHelper` (`getJetStreamHelper(nc)`): `ensureStream`, `ensureConsumer`, `publish` (with dedup `msgId` + optimistic `expect`), and `pullSubscribe`, whose message context exposes **real** `ack()` / `nack(delayMs?)` / `term()` / `working()` plus delivery metadata (`seq`, `numDelivered`, `numPending`). This is the path to choose when you need redelivery and acknowledgement.

### Kafka

- Full core surface. `subscribe` runs a consumer-group `eachMessage` loop.
- **Acknowledgement is real but opt-in.** With `subscribe(..., { ack: true })` the handler context's `ack()` commits the message offset (manual commit); without it, offsets follow kafkajs's default auto-commit. `nack(reason)` logs and relies on the broker to redeliver (no commit advances the offset).
- No `requestMany` / `subscribeWildcard` / JetStream — those are NATS-specific.

### HTTP (SSE)

- `query`/`emit`/`publish`/`broadcast` over HTTP POST; `subscribe` is a **one-way Server-Sent Events** stream. The handler context carries `ack`/`nack` for signature parity, but they are no-ops — SSE has no per-event acknowledgement.
- See [basics-http.md](./basics-http.md) for the endpoint layout (`POST /:service-events`, `GET /__nevo/subscribe`, etc.).

### HTTP/2

- **Request/reply only.** The HTTP/2 client implements `query` and nothing else — no `emit`, `publish`, `broadcast`, or `subscribe`. It is built for low-latency unary RPC over a multiplexed HTTP/2 session.
- If you need pub/sub or fire-and-forget on top of HTTP, use the SSE-based HTTP transport instead.

### WebSocket & Socket.IO

- Both implement the full core surface (`query`/`emit`/`publish`/`broadcast`/`subscribe`) over a persistent socket. `subscribe` delivers server-pushed messages live.
- `ack`/`nack` are best-effort no-ops; neither models per-message acknowledgement the way JetStream or a committed Kafka offset does.
- Socket.IO adds room/namespace plumbing on top of the same surface (see [basics-socket.md](./basics-socket.md)); plain WebSocket is the leaner option (see [basics-websocket.md](./basics-websocket.md)).

### Memory (testing)

- The [in-memory transport](./testing.md) mirrors the core surface — `query`/`emit`/`publish`/`subscribe`/`broadcast` — with **zero IO** (no brokers, no Docker), so unit tests exercise the same client code paths.
- It additionally offers `subscribeBroadcast(handler)` and a `MemoryHarness` for fault injection (`failNext`, `delayBy`, `advanceTime`) and call assertions.
- `publish` honours **wildcard** subscriber methods (`*` / `>` pattern matching) and [subscription filters](./subscription-filters.md), so fan-out semantics match the network transports closely enough for tests. `requestMany`, true JetStream, and real `ack`/`nack` are **not** modelled — its `ack`/`nack` are no-ops.

## Choosing a transport

| You need… | Reach for |
|---|---|
| Lowest-latency unary RPC | HTTP/2 or NATS |
| Durable streaming with redelivery + ack | NATS JetStream or Kafka |
| Scatter/gather across instances | NATS `requestMany` |
| Subject-wildcard subscriptions | NATS `subscribeWildcard` |
| Browser / edge clients | Socket.IO or WebSocket |
| Firewall-friendly server-push | HTTP + SSE |
| Fast, broker-free tests | Memory |

## See also

- [messaging-patterns.md](./messaging-patterns.md) — query/emit/publish/broadcast in depth
- [basics-nats.md](./basics-nats.md) · [basics-kafka.md](./basics-kafka.md) · [basics-http.md](./basics-http.md) · [basics-http2.md](./basics-http2.md) · [basics-websocket.md](./basics-websocket.md) · [basics-socket.md](./basics-socket.md)
- [testing.md](./testing.md) — the in-memory transport
- [subscription-filters.md](./subscription-filters.md) — server-side filtering on `subscribe`
