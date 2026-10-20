# Getting started

This guide walks you through installing Nevo Messaging, writing your first service, and making a request across the wire.

## Requirements

- Node.js **≥ 24** (tested on 26)
- TypeScript **≥ 6**
- NestJS **11 or 12** (`@nestjs/config` 4 or 12; all `@nestjs/*` peers on the same major)
- One transport runtime: NATS, Kafka, HTTP, Socket.IO, or WebSocket

## Install

```bash
npm install @riaskov/nevo-messaging
npm install @nestjs/common @nestjs/core @nestjs/microservices @nestjs/config @nestjs/platform-fastify rxjs reflect-metadata
```

Install only the transport you use:

```bash
npm install @nats-io/transport-node @nats-io/nats-core  # NATS (+ @nats-io/jetstream for streams)
npm install kafkajs                                       # Kafka
npm install socket.io socket.io-client                    # Socket.IO
# HTTP / HTTP2 / WS use Node built-ins
```

Optional peers — improve perf when installed:

```bash
npm install @msgpack/msgpack       # MessagePack codec (recommended default)
npm install fast-json-stringify    # JSON fast path with precompiled schemas
npm install @napi-rs/zstd          # native zstd compression
npm install cacheable-lookup       # DNS cache for the HTTP client
npm install undici                 # alternative HTTP agent
```

## Fastest path: scaffold a service

```bash
npx nevo-gen user --transport nats --port 8086
cd user
npm install
npm run dev
```

This creates a NestJS microservice wired to NATS with sample `getById` / `create` signals. See [service-scaffolding.md](./service-scaffolding.md).

## Hand-rolled minimum

A complete service is four small files — see [basics-nats.md](./basics-nats.md) for a copy-paste example.

## Register `NevoModule`

Every application that declares a `@*SignalRouter` controller must import `NevoModule.forRoot(...)` (or `forRootAsync(...)`) once. It binds the routers at startup; without it the first call fails with "Nevo router … is not initialised".

Since v2.5 the router decorator only carries routing metadata (`eventPattern`, `serviceName`, `debug`, `before`, `after`) plus transport options (`servers`, `port`, `codec`, …). Request-processing options — `accessControl`, `idempotencyStore`, `rateLimit`, `security`, `auditLog`, `metrics`, `tracing`, `dlq`, `health`, … — go to `NevoModule`:

```ts
import { Module } from "@nestjs/common"
import { NevoModule, createNevoNatsClient } from "@riaskov/nevo-messaging"

@Module({
  imports: [
    NevoModule.forRoot({
      serviceName: "user",
      accessControl: {
        rules: [{ topic: "user-events", method: "*", allow: ["frontend"] }]
      }
    })
  ],
  controllers: [UserController],
  providers: [UserService, createNevoNatsClient(["COORDINATOR"], { clientIdPrefix: "user" })]
})
export class AppModule {}
```

Build the options from injected providers with `forRootAsync`:

```ts
NevoModule.forRootAsync({
  imports: [ConfigModule],
  inject: [ConfigService],
  useFactory: (config: ConfigService) => ({
    serviceName: "user",
    rateLimit: { enabled: true, capacity: config.get("RATE_LIMIT", 100), refillPerSec: 50 }
  })
})
```

`NevoModule.forFeature(options)` is a provider you add to a feature module to override root options for the controllers declared there (keys are replaced, not deep-merged).

## Make a request from another service

```ts
@Injectable()
export class OrderService extends NatsClientBase {
  constructor(@Inject("NEVO_NATS_CLIENT") client: NevoNatsClient) {
    super(client)
  }

  async place(userId: bigint) {
    const user = await this.query("user", "user.getById", { id: userId })
    return { ok: true, user }
  }
}
```

## What to read next

- [Messaging patterns](./messaging-patterns.md) — query / emit / publish / subscribe / broadcast
- [Architecture overview](./architecture.md) — how the layers fit together
- [DevTools UI](./devtools.md) — live dashboard for events and metrics
- [Schema validation](./schema.md) — make payloads type-safe and OpenAPI-renderable
