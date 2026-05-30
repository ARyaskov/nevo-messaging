# Idempotency cache

The idempotency cache stores recent successful results keyed by the caller-provided `idempotencyKey`. When the same key arrives twice the cached reply is returned without re-running the handler.

This makes safe retries possible.

## Real API

```ts
interface IdempotencyOptions {
  enabled?: boolean       // default: false
  maxEntries?: number     // LRU cap, default: 10000
  ttlMs?: number          // entry TTL, default: 60000
}

class LruIdempotencyCache<T = unknown> {
  isEnabled(): boolean
  size(): number
  has(key: string): boolean
  get(key: string): T | undefined
  set(key: string, value: T): void
  clear(): void
}
```

In-memory LRU with TTL. There is also a pluggable `IdempotencyStore<T>` interface (see below) for distributed deployments.

## Enable on a transport client

```ts
createNevoNatsClient(["PAYMENT"], {
  clientIdPrefix: "checkout",
  idempotency: {
    enabled: true,
    ttlMs: 300_000,  // 5 minutes
    maxEntries: 50_000
  }
})
```

## Per-call key

```ts
await this.query(
  "payment", "card.charge",
  { userId, amount },
  { idempotencyKey: `charge-${orderId}` }
)
```

When the server receives this envelope:

- If `idempotencyKey` is present and the cache has it → return cached result (no handler invocation)
- Otherwise → run the handler, cache the result on success, return it

The key is propagated end-to-end:

- Clients send it in `meta.idempotencyKey`
- HTTP transport accepts it as the `Idempotency-Key` header

## What gets cached

Only successful replies. Errors are not cached — a retry after an error retries the handler.

## Key derivation

There is no automatic key derivation in the framework. Either:

- Pass `idempotencyKey` explicitly per call, or
- Build a small helper on top that hashes `(method, params)` and uses that as the key

The latter is dangerous on parameters that contain timestamps or nonces — be explicit.

## Server-side dedupe and the request lifecycle

The cache is checked on the server before the handler runs, on **both** the live signal-router path (`@NatsSignalRouter`/`@KafkaSignalRouter`/…) and `BaseMessageController`. The effective key is `meta.idempotencyKey` when the client stamped one, otherwise the envelope `uuid` — so timeout-retries (which carry a fresh `uuid` but the same `idempotencyKey`) collapse to a single execution.

Dedupe is **claim-before-execute**, not check-then-act: the first caller atomically reserves the key (`SET NX` against the distributed store, plus an in-process leader election), runs the handler, then overwrites the reservation with the result. Concurrent duplicates — in the same process or across replicas — await the winner's result instead of re-running, so two handlers no longer both run under a race. The distributed write is awaited before the response is acked, closing the window where a peer re-executes before the result lands. On error / early-return the claim is released so a retry can re-execute.

## Interaction with hedging

The idempotency cache makes [hedging](./hedging.md) safe. Pass the same `idempotencyKey` to every hedged copy and only one handler invocation will run on the peer.

## Memory

LRU eviction keeps the cache bounded. With `maxEntries: 10000, ttlMs: 60000` and 100 byte average payloads, the cache is ~1 MB.

For long-lived entries the framework already detaches buffer views in place — large payloads stored in the cache don't pin a multi-MB pool slab. (Implementation detail, not user-facing.)

## Distributed store — `IdempotencyStore`

A single-replica LRU is fine for stateful services, but pointless for fleets where the same retry can hit any pod. The `IdempotencyStore<T>` interface lets you swap in a shared backend; a ready-to-use Redis adapter is included.

```ts
interface IdempotencyStore<T = unknown> {
  isEnabled(): boolean
  has(key: string): boolean | Promise<boolean>
  get(key: string): T | undefined | Promise<T | undefined>
  set(key: string, value: T): void | Promise<void>
  // Claim-before-execute primitives (optional — stores without them fall back to
  // the racy check-then-act get/set):
  claim?(key: string, opts?: { ttlMs?: number }): Promise<{ acquired: boolean; existing?: T }>
  awaitResult?(key: string, opts?: { timeoutMs?: number; pollMs?: number }): Promise<T | undefined>
  delete?(key: string): void | Promise<void>
}
```

### Redis adapter

```ts
import Redis from "ioredis"
import {
  RedisIdempotencyStore,
  createNatsMicroservice,
  type IdempotencyRedisLike
} from "@riaskov/nevo-messaging"

// Thin wrapper around your favourite Redis client.
const ioredis = new Redis(process.env.REDIS_URL!)
const client: IdempotencyRedisLike = {
  async get(key)              { return ioredis.get(key) },
  async set(key, value, opts) {
    if (opts.ifNotExists) return ioredis.set(key, value, "PX", opts.ttlMs, "NX")
    return ioredis.set(key, value, "PX", opts.ttlMs)
  },
  async del(key) { return ioredis.del(key) }
}

const store = new RedisIdempotencyStore({
  client,
  enabled: true,
  ttlMs: 5 * 60_000,
  keyPrefix: "myapp:idem:"
})

// Wire it on the controller side (handlers reuse cached results across replicas).
// The store is honoured by the signal-router DECORATOR, not by
// `createNatsMicroservice` — that factory only knows `microserviceName`,
// `module`, `port?`, `host?`, `debug?`, `onInit?` and ignores anything else.
@Controller()
@NatsSignalRouter([PaymentService], { idempotencyStore: store })
export class PaymentController {
  // …@Signal handlers…
}

// `createNatsMicroservice` just boots the Nest app that hosts the controller:
createNatsMicroservice({
  microserviceName: "payment",
  module: AppModule,
  port: 8090
})
```

Behavioural notes:

- The store keeps an internal in-process L1 LRU (default `maxEntries: 1024`, TTL = `min(ttlMs, 60s)`) so repeated lookups on the same replica are sub-millisecond and don't round-trip to Redis.
- Reads are read-through: L1 miss → `client.get` → warm L1 → return (the in-progress sentinel written by `claim()` is treated as absent).
- Writes are write-through and **awaited**: `set()` updates L1 immediately, then overwrites the Redis key with `PX <ttlMs>` (no `NX` — it must replace the claimer's own in-progress sentinel; cross-replica races are prevented by the atomic `claim()`, not by the result write).
- Read failures emit a high-severity metric (`nevo_messaging_store_errors_total`) and an error log, then follow `readErrorPolicy`: `"open"` (default) treats the failure as a miss and proceeds (at-least-once; may re-execute), `"closed"` rethrows so the caller fails the request rather than risk a duplicate. Write failures stay soft — the L1 absorbed the entry.

### Implementing your own store (Memcached, Dynamo, …)

The shape is intentionally narrow. Any backend that supports a TTL'd `get/set` is enough. The store implementation can return Promises freely; the controller awaits transparently.

## What is *still* not provided

- **No autoKey config** — you provide the key explicitly.
- **No `cacheErrors` option** — only successful results are cached.

## See also

- [replay-protection.md](./replay-protection.md) — rejects duplicate UUIDs (security), not safe-retry caching
- [hedging.md](./hedging.md) — depends on this for safe duplicate calls
- [inbox.md](./inbox.md) — durable consumer-side dedup that survives restarts
