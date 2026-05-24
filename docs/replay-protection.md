# Replay protection

Replay protection rejects envelopes whose UUID has already been seen within a configurable window. It is a defense against:

- Replay attacks (an attacker captures a signed envelope and re-sends it)
- Buggy retry loops that fire the same envelope thousands of times
- Operator mistakes (restoring a Kafka topic backup that contains old messages)

## Real API

```ts
interface ReplayGuardOptions {
  enabled?: boolean
  windowMs?: number                              // dedup window, default: 60000
  maxEntries?: number                            // LRU cap, default: 10000
  sharedCache?: LruIdempotencyCache              // optional: reuse the idempotency cache
}

class ReplayGuard {
  constructor(opts?: ReplayGuardOptions)
  check(uuid: string | undefined, ts: number | undefined): void  // throws on replay
}
```

The guard is a thin wrapper around an LRU; it `throws` when a duplicate UUID arrives within `windowMs`.

## Enable

```ts
import { ReplayGuard } from "@riaskov/nevo-messaging"

const guard = new ReplayGuard({
  enabled: true,
  windowMs: 300_000,
  maxEntries: 100_000
})

// On every inbound envelope:
guard.check(envelope.uuid, envelope.ts)  // throws ErrorCode.REPLAY_DETECTED on hit
```

On a transport client:

```ts
createNevoNatsClient(["PAYMENT"], {
  clientIdPrefix: "service",
  replayProtection: {
    enabled: true,
    windowMs: 300_000,
    maxEntries: 100_000
  }
})
```

## How it works

Every envelope carries a UUIDv7 in `uuid`. UUIDv7 embeds a millisecond timestamp. The guard:

- Reads the UUID's embedded timestamp (or the envelope `ts` if provided)
- Compares against `now - windowMs` — older envelopes are rejected as out-of-window
- Otherwise checks the LRU; on a hit, throws `ErrorCode.REPLAY_DETECTED`; on a miss, stores it

## Memory & sizing

`maxEntries` caps the LRU. With `windowMs: 300_000` and a steady 1 000 RPS you need at least 300 000 entries to avoid evictions. If the LRU evicts an entry before its window expires, a replay of that UUID won't be caught — pick a `maxEntries` ≥ `RPS * (windowMs / 1000)`.

## Sharing with the idempotency cache

Pass `sharedCache: idempotencyCache` to reuse the same LRU instance. This is a memory optimisation when you want both protections covering the same window and don't need separate budgets.

## Difference from the idempotency cache

| | Replay protection | Idempotency cache |
|---|---|---|
| Purpose | Security & integrity | Safe retries |
| On hit | Reject with `REPLAY_DETECTED` | Return cached reply |
| Key | Envelope `uuid` (UUIDv7) | Caller-provided `idempotencyKey` |
| Default | Off (security feature; only enable where needed) | Recommended for mutating methods |

You typically enable **both** on high-stakes paths (payment / order).

## What is not provided

- **No separate clock-skew tolerance** — `windowMs` is used for the future-too-old check as well. Pick a value that absorbs realistic clock drift in your fleet.
- **No distributed store.** The guard is per Node instance. An attacker that alternates replicas can replay each one independently. For shared state, build a store on top — it is not in the framework.

## See also

- [idempotency.md](./idempotency.md) — the related but different mechanism
- [security.md](./security.md) — JWT/JWKS verification (the upstream defense)
- [error-codes.md](./error-codes.md) — `REPLAY_DETECTED` and friends
