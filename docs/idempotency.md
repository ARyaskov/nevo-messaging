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

In-memory LRU with TTL. There is no pluggable store interface — the cache is process-local.

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

The cache is checked on the server before the handler runs. A second concurrent call with the same key races to win the cache slot — the loser sees the cached result of the winner once the winner completes. (There is no in-flight "single flight" promise behind the cache today; in pathological races two handlers may both run. The cache's purpose is correctness across retries, not concurrent dedupe.)

## Interaction with hedging

The idempotency cache makes [hedging](./hedging.md) safe. Pass the same `idempotencyKey` to every hedged copy and only one handler invocation will run on the peer.

## Memory

LRU eviction keeps the cache bounded. With `maxEntries: 10000, ttlMs: 60000` and 100 byte average payloads, the cache is ~1 MB.

For long-lived entries the framework already detaches buffer views in place — large payloads stored in the cache don't pin a multi-MB pool slab. (Implementation detail, not user-facing.)

## What is not provided

- **No distributed store** (Redis-backed cache). The cache is per Node instance. For a cluster-wide cache, build one on top using the same `IdempotencyOptions` shape.
- **No autoKey config** — you provide the key explicitly.
- **No `cacheErrors` option** — only successful results are cached.

## See also

- [replay-protection.md](./replay-protection.md) — rejects duplicate UUIDs (security), not safe-retry caching
- [hedging.md](./hedging.md) — depends on this for safe duplicate calls
- [inbox.md](./inbox.md) — durable consumer-side dedup that survives restarts
