# Nevo Messaging

A microservices messaging framework for NestJS 11+ with multi-transport support (NATS, Kafka, HTTP/SSE, HTTP/2, WebSocket, Socket.IO), designed for building scalable distributed systems with type-safe inter-service communication.

> **Full documentation lives in [docs/](./docs/README.md).** This README only covers a minimal NATS setup. For every other feature — Kafka, HTTP/2, the saga orchestrator, outbox, sliding-window circuit breakers, idempotency, JWT, OpenTelemetry, the DevTools dashboard, and a hundred more — start at the [docs index](./docs/README.md).

## Highlights

- 🚀 Type-safe messaging across NATS, Kafka, HTTP/SSE, HTTP/2, WebSocket, Socket.IO
- 🔄 Query (request/response), emit (fire-and-forget), publish/subscribe, broadcast, streaming
- 🎯 Declarative routing via `@Signal` decorator with method versioning
- 🧱 Pluggable codecs — MessagePack default, JSON, fast-json-stringify
- 🪶 Compression (gzip, deflate, zstd) with worker-thread offload
- 🧯 Resilience: retry with jitter, sliding-window circuit breaker, hedging, adaptive concurrency, idempotency LRU, replay protection, rate limiting
- 🧰 Reliable patterns: transactional outbox, exactly-once inbox, saga with compensation, CQRS bridge, event store
- 📈 Observability: pino, Prometheus metrics, OpenTelemetry, structured DLQ
- 🪟 DevTools UI — live Next.js 16 dashboard with circuits/methods/errors/trace/replay
- 🔐 Security: ACL, JWT/JWKS, mTLS, PII redaction
- 🩺 Liveness + readiness probes with pluggable checks (pg/redis/nats/kafka/http)
- 🛡️ Graceful shutdown, BigInt support, did-you-mean suggestions

See [CHANGELOG.md](CHANGELOG.md) for version history.

## Requirements

- Node.js **≥ 24** (tested on 26)
- TypeScript **≥ 6**
- NestJS **≥ 11**

## Install

```bash
npm install @riaskov/nevo-messaging
npm install @nestjs/common @nestjs/core @nestjs/microservices @nestjs/config @nestjs/platform-fastify rxjs reflect-metadata
npm install @nats-io/transport-node @nats-io/nats-core   # for NATS — install only the transport(s) you use
```

## Fastest start: scaffold a service

```bash
npx nevo-gen user --transport nats --port 8086
cd user
npm install
npm run dev
```

See [docs/service-scaffolding.md](./docs/service-scaffolding.md).

## NATS quick start

A complete NATS-backed microservice is four small files.

### 1. Service

```ts
// user.service.ts
import { Injectable, Inject } from "@nestjs/common"
import { NatsClientBase, NevoNatsClient } from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends NatsClientBase {
  constructor(@Inject("NEVO_NATS_CLIENT") client: NevoNatsClient) {
    super(client)
  }

  async getById(id: bigint) {
    return { id, name: "John Doe", email: "john@example.com" }
  }

  async create(input: { name: string; email: string }) {
    return { id: 123n, ...input }
  }
}
```

### 2. Controller

```ts
// user.controller.ts
import { Controller, Inject } from "@nestjs/common"
import { NatsSignalRouter, Signal } from "@riaskov/nevo-messaging"
import { UserService } from "./user.service"

@Controller()
@NatsSignalRouter([UserService])
export class UserController {
  constructor(@Inject(UserService) private readonly users: UserService) {}

  @Signal("user.getById", "getById", (d: any) => [d.id])
  getById() {}

  @Signal("user.create", "create", (d: any) => [d])
  create() {}
}
```

### 3. Module

```ts
// user.module.ts
import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import { createNevoNatsClient } from "@riaskov/nevo-messaging"
import { UserController } from "./user.controller"
import { UserService } from "./user.service"

@Module({
  imports: [ConfigModule],
  controllers: [UserController],
  providers: [
    UserService,
    createNevoNatsClient(["COORDINATOR"], {
      clientIdPrefix: "user",
      servers: ["nats://127.0.0.1:4222"]
    })
  ]
})
export class UserModule {}
```

### 4. Bootstrap

```ts
// main.ts
import { createNatsMicroservice } from "@riaskov/nevo-messaging"
import { AppModule } from "./app.module"

createNatsMicroservice({
  microserviceName: "user",
  module: AppModule,
  port: 8086
})
```

### Calling it from another service

```ts
@Injectable()
export class OrderService extends NatsClientBase {
  constructor(@Inject("NEVO_NATS_CLIENT") client: NevoNatsClient) {
    super(client)
  }

  async place(userId: bigint) {
    const user = await this.query("user", "user.getById", { id: userId })
    await this.emit("notifications", "order.created", { userId: user.id })
  }
}
```

### Running a NATS broker

```bash
docker run --rm -p 4222:4222 nats:2 -js
```

JetStream is enabled with `-js` so subscriptions with `{ ack: true }` work out of the box.

## Communication patterns at a glance

```ts
// Request/response
const user = await this.query("user", "user.getById", { id: 123n })

// Fire-and-forget
await this.emit("notifications", "user.created", { userId: 123n })

// Publish / subscribe (durable JetStream consumer)
const sub = await this.subscribe(
  "user", "user.updated",
  { ack: true, durable: "audit" },
  async (msg, ctx) => { await project(msg); await ctx.ack() }
)

// Broadcast to every connected consumer
await this.broadcast("system.status", { ok: true })
```

See [docs/messaging-patterns.md](./docs/messaging-patterns.md).

## Where to go next

| Topic | Doc |
|---|---|
| Other transports (Kafka, HTTP, HTTP/2, WS, Socket.IO) | [docs/](./docs/README.md) |
| DevTools live dashboard | [docs/devtools.md](./docs/devtools.md) |
| Reliable patterns (outbox, inbox, saga, CQRS, DLQ) | [docs/outbox.md](./docs/outbox.md) |
| Resilience (retry, breaker, hedging, rate limit) | [docs/retry.md](./docs/retry.md) |
| Observability (OTel, metrics, health probes) | [docs/observability.md](./docs/observability.md) |
| Security (ACL, JWT/JWKS, mTLS) | [docs/security.md](./docs/security.md) |
| Performance tuning | [docs/performance.md](./docs/performance.md) |

Full index: **[docs/README.md](./docs/README.md)**.

## Examples

- `examples/nats-user` — NATS request/response + publish/subscribe + broadcast
- `examples/user` — standard Kafka microservice
- `examples/socket-user` — Socket.IO transport
- `examples/http-user` — HTTP query/emit + SSE subscribe + broadcast

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit (`git commit -m "Add amazing feature"`)
4. Push (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT — see [LICENSE](LICENSE).

## Support

- Issues: [github.com/ARyaskov/nevo-messaging/issues](https://github.com/ARyaskov/nevo-messaging/issues)
- Documentation: [docs/](./docs/README.md)
- Examples: [examples/](./examples/)
