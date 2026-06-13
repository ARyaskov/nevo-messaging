# Testing handlers

Spinning up NATS, Kafka, or Redis containers for every unit test gets tiresome. `nevo-messaging` ships an **in-memory transport** that satisfies the same `query` / `emit` / `publish` / `subscribe` / `broadcast` API as the network transports but with zero IO. Tests run as fast as pure-function tests and stay deterministic.

## Quick start

```ts
import { test } from "node:test"
import assert from "node:assert/strict"
import { createMemoryTransport, MemoryClientBase } from "@riaskov/nevo-messaging"

class UserService extends MemoryClientBase {
  async fetchOrder(id: bigint) {
    return this.query<{ id: bigint; total: number }>("order", "order.getById", { id })
  }
}

test("fetchOrder calls order.getById and returns its result", async () => {
  const transport = createMemoryTransport()
  transport.registerHandler("order", "order.getById", async (params: any) => {
    return { id: params.id, total: 1500 }
  })

  // Inject the fake transport instead of NATS / Kafka / HTTP.
  const svc = new (class extends UserService {
    constructor() { super(transport, { serviceName: "frontend" }) }
  })()

  const order = await svc.fetchOrder(42n)
  assert.equal(order.total, 1500)

  // The transport records every call. Assert against the recording:
  const [call] = transport.harness.calls
  assert.equal(call.kind, "query")
  assert.equal(call.method, "order.getById")
})
```

No Docker, no `nats:2`, no `kafka:7.5`. The handler is registered in-process; calls go through `setImmediate` boundaries to mirror the asynchronous behaviour of the real transports.

## Anatomy

| Piece | Purpose |
|---|---|
| `MemoryTransport` | The fake bus — holds handlers + subscribers, dispatches messages |
| `MemoryClientBase` | Drop-in replacement for `NatsClientBase` / `KafkaClientBase` / ... |
| `MemoryHarness` | Inspection + injection hooks (record, fail, delay, advance time) |
| `createMemoryTransport({ handlers })` | Sugar for tests that don't care about DI |

`MemoryTransport` lives in `src/transports/memory/memory.transport.ts`.

## Patterns

### Asserting that an emit fired

```ts
const transport = createMemoryTransport()
transport.registerHandler("audit", "user.deleted", async () => undefined)

await someService.deleteUser(7n)

assert.deepEqual(
  transport.harness.calls.filter((c) => c.method === "user.deleted"),
  [{ kind: "emit", serviceName: "audit", method: "user.deleted", /* … */ }]
)
```

### Wiring publish/subscribe across multiple services

```ts
const transport = createMemoryTransport()
const received: string[] = []

transport.subscribe("metrics", "ping", undefined, async (msg: any) => {
  received.push(msg.value)
})

class Producer extends MemoryClientBase {
  fire(value: string) { return this.publish("metrics", "ping", { value }) }
}

await new (class extends Producer {
  constructor() { super(transport, { serviceName: "p" }) }
})().fire("hi")

assert.deepEqual(received, ["hi"])
```

### Wildcard subscriptions

`*` matches one segment, `>` matches one-or-more — same as NATS.

```ts
transport.subscribe("user", "user.event.>", undefined, async () => { /* … */ })
await producer.fire("user.event.created")   // matches
await producer.fire("user.event.deleted")   // matches
await producer.fire("user.changed")         // does NOT match
```

### Injecting failures

```ts
transport.harness.failNext("order", "order.getById", new Error("DB outage"))
await assert.rejects(() => svc.fetchOrder(42n), /DB outage/)
// Single-shot — the next call succeeds again.
```

### Injecting latency

```ts
transport.harness.delayBy("order", "order.getById", 100)
// Subsequent queries to order.getById sleep 100ms before invoking the handler.
```

### Time control

`MemoryHarness.advanceTime(ms)` shifts `harness.now()` forward. Use this in tests that depend on `replay-window`, idempotency TTLs, or anything timestamp-driven. (Real `Date.now()` is unaffected — the framework's helpers honour `harness.now()` only where you wire it.)

## Combining with the rest of the framework

Memory-transport tests still benefit from the rest of the framework's primitives:

- **Schema validation** (`@Schema`) works as-is — `MemoryTransport` invokes handlers exactly like real transports.
- **Resilience decorators** (`@Hedge` / `@CircuitBreaker` / `@Adaptive` / `@Backpressure`) work too; the registries are per-process and the in-memory transport doesn't bypass them.
- **Idempotency** — pass `idempotencyKey` via `opts.meta.idempotencyKey` to test the cache hit path.
- **Tracing** — OpenTelemetry spans propagate through the in-memory bus the same way.

What the memory transport does *not* cover:

| Concern | Use a real broker |
|---|---|
| Codec / compression behaviour | Yes |
| Cross-process delivery semantics | Yes |
| Kafka rebalance, NATS reconnect, JetStream durables | Yes |
| Authentication (JWT/JWKS, mTLS) | Yes |

For these, run an integration test against the real transport. The recommendation: 90% memory-transport unit tests, 10% real-broker integration tests.

## Test recipes

### Jest

```ts
import { createMemoryTransport, MemoryTransport } from "@riaskov/nevo-messaging"

describe("UserService", () => {
  let transport: MemoryTransport

  beforeEach(() => {
    transport = createMemoryTransport()
  })

  afterEach(() => transport.reset())

  it("…", async () => { /* … */ })
})
```

### Vitest

Same as Jest — only the imports change.

### node:test

Already shown in Quick Start. The framework's own test suite uses `node:test` so all examples ported across.

## See also

- [messaging-patterns.md](./messaging-patterns.md) — query/emit/publish/subscribe/broadcast contract
- [idempotency.md](./idempotency.md) — testing the dedup path
- [resilience-decorators.md](./resilience-decorators.md) — testing resilient behaviour
