# Architecture overview

Nevo Messaging is a NestJS framework for inter-service communication across multiple transports. Every transport implements the same contract, so the application code is largely transport-agnostic.

## Layers

```
┌─────────────────────────────────────────────────────┐
│ Application services (your NestJS code)             │
│   - Extend *ClientBase (NatsClientBase, …)          │
│   - Annotate controllers with @*SignalRouter        │
│   - Methods exposed via @Signal                     │
├─────────────────────────────────────────────────────┤
│ BaseTransportClient                                 │
│   query · emit · publish · subscribe · broadcast    │
│   request-many (streaming)                          │
├─────────────────────────────────────────────────────┤
│ Cross-cutting features                              │
│   codec · compression · retry · circuit breaker     │
│   idempotency · replay-protection · rate-limit      │
│   ACL · JWT · metrics · OTel · logger · DLQ         │
├─────────────────────────────────────────────────────┤
│ Transport drivers                                   │
│   NATS · Kafka · HTTP/SSE · HTTP/2 · WS · Socket.IO │
└─────────────────────────────────────────────────────┘
```

## Core abstractions

### Clients

Each transport ships a low-level client (`NevoNatsClient`, `NevoKafkaClient`, ...) plus a NestJS-injectable base wrapper (`NatsClientBase`, `KafkaClientBase`, ...). Application services extend the base:

```ts
class UserService extends NatsClientBase { ... }
```

All bases expose the same surface: `query`, `emit`, `publish`, `subscribe`, `broadcast`, plus discovery helpers (`getAvailableServices`, `isServiceAvailable`).

### Signal routers

Decorators like `@NatsSignalRouter([UserService])` register controller-level routing. The `@Signal("user.getById", "getById", paramMap)` decorator maps a wire signal to a service method.

The router is also where cross-cutting features attach: `accessControl`, `before`/`after` hooks, `debug` mode, transport-specific options.

### Envelope

Every wire message carries a small envelope:

```ts
{
  uuid: string         // UUIDv7 (monotonic, time-sortable)
  method: string       // "user.getById" or "user.getById@v2"
  params: unknown
  meta?: {
    version?: string
    traceparent?: string
    tenantId?: string
    idempotencyKey?: string
    callerService?: string
    auth?: { token?: string }
    codec?: string
    encoding?: string
    [k: string]: unknown
  }
}
```

### Codec

Pluggable: `MessagePackCodec` (default), `JsonCodec`, `JsonCodecFast`, `FastJsonStringifyCodec`. See [codecs.md](./codecs.md).

### Method versioning

Append `@vN` to a method name to register a versioned variant:

```ts
@Signal("user.getById@v2", "getByIdV2", ...)
```

Clients select a version with `meta.version` or by suffixing the method name.

## Cross-cutting features

All optional, all configured via client/router options:

| Feature | Doc |
|---|---|
| Retry with jitter | [retry.md](./retry.md) |
| Circuit breaker | [circuit-breaker.md](./circuit-breaker.md) |
| Idempotency LRU | [idempotency.md](./idempotency.md) |
| Replay protection | [replay-protection.md](./replay-protection.md) |
| Rate limiting | [rate-limiting.md](./rate-limiting.md) |
| Hedging | [hedging.md](./hedging.md) |
| Adaptive tuner | [adaptive.md](./adaptive.md) |
| Backpressure | [backpressure.md](./backpressure.md) |
| ACL | [access-control.md](./access-control.md) |
| OpenTelemetry | [observability.md](./observability.md) |
| Metrics | [metrics.md](./metrics.md) |
| Health probes | [health-checks.md](./health-checks.md) |
| DLQ | [dlq.md](./dlq.md) |

## Reliable patterns

- [Outbox](./outbox.md) — at-least-once publish after DB commit
- [Inbox](./inbox.md) — consumer-side exactly-once dedup
- [Saga](./saga.md) — multi-step transactions with compensation
- [CQRS bridge & event store](./cqrs.md)

## DevTools

The Next.js 16 dashboard in `devtools/` reads from a `DevToolsBus`. See [devtools.md](./devtools.md).
