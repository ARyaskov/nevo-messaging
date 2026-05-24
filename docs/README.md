# Nevo Messaging — Documentation

This folder contains the full documentation for `@riaskov/nevo-messaging`. The top-level [README](../README.md) covers a minimal NATS quick start; everything else lives here.

## Getting started

- [Getting started](./getting-started.md) — install, run a service, send your first request
- [Service scaffolding (`nevo gen`)](./service-scaffolding.md) — generate a fresh microservice in one command
- [Architecture overview](./architecture.md) — clients, transports, codecs, decorators

## Transports

- [NATS](./basics-nats.md)
- [Kafka](./basics-kafka.md)
- [HTTP / SSE](./basics-http.md)
- [HTTP/2](./basics-http2.md)
- [WebSocket](./basics-websocket.md)
- [Socket.IO](./basics-socket.md)

## Messaging patterns

- [Query, emit, publish/subscribe, broadcast](./messaging-patterns.md)
- [Signal routing & method decorators](./method-decorators.md)
- [Subscription filters](./subscription-filters.md)
- [Discovery & registry](./discovery.md)

## Reliability & resilience

- [Retry policy](./retry.md)
- [Circuit breaker (sliding-window, cost-based)](./circuit-breaker.md)
- [Hedging](./hedging.md)
- [Adaptive concurrency](./adaptive.md)
- [Backpressure](./backpressure.md)
- [Rate limiting](./rate-limiting.md)
- [Idempotency cache](./idempotency.md)
- [Replay protection](./replay-protection.md)
- [Graceful shutdown](./graceful-shutdown.md)

## Reliable messaging patterns

- [Outbox](./outbox.md)
- [Inbox (exactly-once consumer)](./inbox.md)
- [Saga orchestrator](./saga.md)
- [CQRS bridge & event store](./cqrs.md)
- [Dead-letter queue (DLQ)](./dlq.md)

## Codec & wire format

- [Codecs (JSON, JsonFast, MessagePack, fast-json-stringify)](./codecs.md)
- [Compression (gzip, deflate, zstd, workers)](./compression.md)
- [BigInt handling](./bigint.md)
- [Schema validation (zod, class-validator)](./schema.md)
- [Type-safe contracts](./contracts.md)
- [OpenAPI / AsyncAPI generation](./openapi.md)

## Observability

- [Logger (pino)](./logger.md)
- [Metrics (Prometheus-style)](./metrics.md)
- [OpenTelemetry tracing](./observability.md)
- [Health checks — liveness & readiness](./health-checks.md)
- [PII redaction](./redaction.md)
- [DevTools UI dashboard](./devtools.md)

## Security

- [Access control (ACL)](./access-control.md)
- [JWT & JWKS verification](./security.md)
- [Error codes](./error-codes.md)

## Advanced

- [Multi-tenancy](./multi-tenant.md)
- [Performance tuning](./performance.md)
- [Environment variables](./environment.md)
