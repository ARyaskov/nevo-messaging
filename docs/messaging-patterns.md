# Messaging patterns

Nevo exposes four core communication patterns. They are the same across every transport.

## Query — request/response

Use for any operation that needs a result.

```ts
const user = await this.query<User>("user", "user.getById", { id: 123n })
```

- Single reply expected
- Honors retry, circuit breaker, idempotency
- Carries trace context via `meta.traceparent`

Per-call options:

```ts
await this.query("user", "user.getById", { id: 1n }, {
  timeoutMs: 5_000,
  idempotencyKey: "get-1",
  retry: { enabled: true, maxAttempts: 2 }
})
```

On timeout the call rejects with `ErrorCode.TIMEOUT`.

## Emit — fire and forget

Use for events you do not need to wait on.

```ts
await this.emit("notifications", "user.created", { userId: 123n })
```

- Returns once the local broker buffer accepts the message
- No remote ack
- Use for analytics, logs, side-effects
- For at-least-once, route through the [outbox](./outbox.md)

## Publish / subscribe

A topic + method pair behaves like a named channel. Multiple consumers can subscribe; each one receives every message (fan-out).

Publisher:

```ts
await this.publish("user", "user.updated", { userId: 123n })
```

Subscriber:

```ts
const sub = await this.subscribe(
  "user", "user.updated",
  { ack: true, durable: "audit-projector" },
  async (msg, ctx) => {
    await projectAudit(msg)
    await ctx.ack()
  }
)
// later
await sub.unsubscribe()
```

Common subscription options:

| Option | Behavior |
|---|---|
| `ack` | Enable JetStream / Kafka manual-ack semantics |
| `durable` | Durable consumer name (NATS JetStream) |
| `filters` | `headers` / `meta` predicates — see [subscription-filters.md](./subscription-filters.md) |

## Broadcast

Sends one message to every connected consumer regardless of subscription pattern.

```ts
await this.broadcast("system.status", { ok: true })
```

Receive on the reserved topic `__broadcast`:

```ts
await this.subscribe("__broadcast", "system.status", {}, (msg) => {
  console.log("broadcast", msg)
})
```

Use broadcast for cluster-wide signals: cache invalidation, kill switches, config reload.

## `requestMany` — streaming replies

For methods that produce multiple replies in series:

```ts
for await (const chunk of this.requestMany("search", "search.stream", { q: "foo" })) {
  console.log(chunk)
}
```

Server side: declare a handler that yields. NATS supports this natively (multiple `respond` calls on the same subject). Kafka/HTTP map it to a stream of records / SSE events.

## Method versioning

Append `@vN` to a method name to register a versioned variant:

```ts
@Signal("user.getById@v2", "getByIdV2", (d) => [d.id])
```

Clients can pin a version via `meta.version`. Discovery advertises every version so the caller can negotiate.

## Side-by-side

| Pattern | Reply | Fan-out | Use case |
|---|---|---|---|
| `query` | 1 | 1 receiver | RPC |
| `emit` | 0 | 1 receiver (queue group) | Async work |
| `publish`/`subscribe` | 0 | N receivers | Pub/sub, projections |
| `broadcast` | 0 | All receivers | System signals |
| `requestMany` | N | 1 receiver | Streaming |
