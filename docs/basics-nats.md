# NATS transport

NATS is the lowest-latency and simplest transport. It is the recommended default for most setups.

## Why NATS

- Single binary broker, runs anywhere
- Sub-millisecond request/response under load
- Subject namespace — no topic provisioning needed
- JetStream for durable streams (used when you `subscribe` with `ack: true`)

## Install

```bash
# nats.js v3 — runtime entry point plus the core types/helpers it transitively pulls in.
npm install @nats-io/transport-node @nats-io/nats-core
# Only if you use JetStream:
npm install @nats-io/jetstream
```

Run a broker locally:

```bash
docker run --rm -p 4222:4222 nats:2 -js
```

`-js` enables JetStream.

## Client factory

```ts
import { createNevoNatsClient } from "@riaskov/nevo-messaging"

createNevoNatsClient(["USER", "COORDINATOR"], {
  clientIdPrefix: "user",
  servers: ["nats://127.0.0.1:4222"],
  reconnect: {
    enabled: true,
    timeWaitMs: 5_000,
    maxAttempts: -1,        // infinite
    jitterMs: 100,
    jitterTlsMs: 500,
    waitOnFirstConnect: true,
    lazyConnect: true       // connect on first use, not at factory time
  },
  jetstream: { enabled: false },

  // Slow-consumer detection on subscriptions
  subscribeMaxPending: 1024,
  subscribeOnSlow: ({ subject, pending }) => {
    console.warn(`subscriber falling behind on ${subject}: ${pending} pending`)
  }
})
```

DI token: `"NEVO_NATS_CLIENT"` (exported as `NEVO_NATS_CLIENT_TOKEN`).

## Service

```ts
import { Injectable, Inject } from "@nestjs/common"
import { NatsClientBase, NevoNatsClient } from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends NatsClientBase {
  constructor(@Inject("NEVO_NATS_CLIENT") client: NevoNatsClient) {
    super(client)
  }

  async getById(id: bigint) {
    return { id, name: "Alice" }
  }
}
```

## Controller

```ts
import { Controller, Inject } from "@nestjs/common"
import { NatsSignalRouter, Signal } from "@riaskov/nevo-messaging"

@Controller()
@NatsSignalRouter([UserService])
export class UserController {
  constructor(@Inject(UserService) private readonly users: UserService) {}

  @Signal("user.getById", "getById", (d) => [d.id])
  getById() {}
}
```

## Bootstrap

```ts
import { createNatsMicroservice } from "@riaskov/nevo-messaging"
import { AppModule } from "./app.module"

createNatsMicroservice({
  microserviceName: "user",
  module: AppModule,
  port: 8086
})
```

## Subjects & wildcards

Subjects use dot-separated names (`user.created`, `orders.paid.eu-west`). The `@Signal` value is sent verbatim as the subject. NATS wildcards `*` (one token) and `>` (rest) are supported in subscription patterns:

```ts
await this.subscribe("user", "user.*", {}, (msg) => { ... })
await this.subscribeWildcard("orders.>", (msg, ctx) => { ... })
```

## JetStream-backed `ack: true`

When you subscribe with `{ ack: true }` Nevo upgrades the subscription to a durable JetStream consumer with manual ack. Use this for at-least-once semantics:

```ts
const sub = await this.subscribe(
  "user", "user.events",
  { ack: true, durable: "user-events-projector" },
  async (msg, ctx) => {
    await project(msg)
    await ctx.ack()
  }
)
```

`ctx.nack(delayMs)` and `ctx.term()` are also available.

## Reconnect behavior

The driver listens for `disconnect`, `reconnect`, and `error` events. Pending subscriptions are re-armed on reconnect. With `reconnect.maxAttempts: -1` the client retries forever; positive values cap attempts.

`reconnect.lazyConnect: true` defers the initial connection until the first call, which is useful when constructing the client in a unit test that doesn't have a broker.

## Connection multiplexing

The signal router decorator accepts a `reuseClient: true` option — when multiple controllers in the same process need a NATS connection, they share one underlying `NatsConnection`:

```ts
@NatsSignalRouter([UserService], { reuseClient: true })
```

## Slow-consumer detection

`subscribeMaxPending` plus `subscribeOnSlow` surface lagging subscribers before they fall over:

```ts
createNevoNatsClient(["USER"], {
  clientIdPrefix: "user",
  servers: ["nats://nats:4222"],
  subscribeMaxPending: 2048,
  subscribeOnSlow: ({ subject, pending }) => {
    logger.warn({ subject, pending }, "slow consumer detected")
  }
})
```

The callback fires whenever pending crosses the limit; combine with [adaptive](./adaptive.md) or pause-based [backpressure](./backpressure.md).

## Production tips

- Run NATS in cluster mode (3+ nodes) for HA
- Enable JetStream for any subscription that requires durability
- Use `subscribeMaxPending` + `subscribeOnSlow` to surface lag early
- Set `clientIdPrefix` to a stable name — metric labels use it
- Combine with [graceful shutdown](./graceful-shutdown.md) so the service drains in-flight messages on SIGTERM
