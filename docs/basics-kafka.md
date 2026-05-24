# Kafka transport

Kafka is the right choice when you need a durable log, partitioned ordering, or integration with an existing Kafka ecosystem.

## Install

```bash
npm install kafkajs
```

Local broker (KRaft mode):

```yaml
services:
  kafka:
    image: apache/kafka:4.0.0
    ports: ["9092:9092"]
    environment:
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_NODE_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

## Client factory

```ts
import { createNevoKafkaClient } from "@riaskov/nevo-messaging"

createNevoKafkaClient(["USER", "INVENTORY"], {
  clientIdPrefix: "order-service",
  groupIdPrefix: "order-consumer",
  kafkaHost: "localhost",        // or KAFKA_HOST env var
  kafkaPort: 9092,               // or KAFKA_PORT env var
  sessionTimeout: 30_000,
  allowAutoTopicCreation: true,
  retryAttempts: 5,
  brokerRetryTimeout: 2_000,
  timeoutMs: 25_000,
  discovery: {
    enabled: true,
    heartbeatIntervalMs: 5_000,
    ttlMs: 15_000
  }
})
```

DI token: `"NEVO_KAFKA_CLIENT"` (exported as `NEVO_KAFKA_CLIENT_TOKEN`).

## Service & controller

Identical shape to NATS — swap the base class and decorator:

```ts
import { KafkaClientBase, NevoKafkaClient, KafkaSignalRouter, Signal } from "@riaskov/nevo-messaging"

class UserService extends KafkaClientBase {
  constructor(@Inject("NEVO_KAFKA_CLIENT") client: NevoKafkaClient) { super(client) }
  async getById(id: bigint) { return { id, name: "Eddie" } }
}

@Controller()
@KafkaSignalRouter([UserService])
export class UserController {
  constructor(@Inject(UserService) private readonly users: UserService) {}

  @Signal("user.getById", "getById", (d) => [d.id])
  getUserById() {}
}
```

## Topic naming

The framework derives topic names automatically from service names: a service called `user` produces and consumes from topic `user-events`. There is no `topicMap` option — topics follow the convention `<lowercase-service-name>-events`.

If you need a different topic name, run kafkajs directly for that path; the framework's signal routing is tied to the `-events` suffix.

## Shared consumer pool

Multiple controllers in the same process share a single consumer per group ID. This avoids rebalance storms when one process exposes many controllers.

## DLQ on delivery exhaustion

When `retryAttempts` is exhausted on the consumer side, the failing envelope is forwarded to the configured DLQ:

```ts
createNevoKafkaClient(["USER"], {
  clientIdPrefix: "frontend",
  dlq: { enabled: true }
})
```

See [dlq.md](./dlq.md). The default DLQ suffix is `.dlq` (e.g. `user-events.dlq`).

## Authentication

```ts
createNevoKafkaClient(["USER"], {
  clientIdPrefix: "frontend",
  security: {
    sasl: {
      mechanism: "scram-sha-512",
      username: process.env.KAFKA_USER,
      password: process.env.KAFKA_PASS
    },
    ssl: true
  }
})
```

`security` is forwarded to `kafkajs` — see its docs for the full SASL/SSL shape.

## Discovery

Enabled by default — heartbeats publish to `__nevo.discovery`. Capabilities derived from `@Signal` declarations announce automatically. See [discovery.md](./discovery.md).

## Production tips

- Set `groupIdPrefix` per environment (`prod-`, `staging-`)
- Pre-create topics with the partition count you need; auto-creation defaults are rarely correct
- Pair with the [outbox](./outbox.md) for guaranteed delivery after a DB commit
- Enable [OpenTelemetry](./observability.md) — Kafka context propagation works out of the box
- Set `KAFKA_HOST` / `KAFKA_PORT` env vars to avoid baking the broker address into code
