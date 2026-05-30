# Hedging

Hedging fires a duplicate request **before** the first one completes, in the hope that the parallel attempt finishes faster. The first successful reply wins.

Use it when the latency distribution has a long tail and the call is **idempotent**.

## Real API

```ts
interface HedgingOptions {
  enabled?: boolean    // default: false
  copies?: number      // number of extra parallel attempts, default: 1
  delayMs?: number     // wait this long before firing each extra copy, default: 25
}
```

```ts
import { hedge } from "@riaskov/nevo-messaging"

const result = await hedge(
  (attempt, signal) => callPeer({ attempt, signal }),
  { enabled: true, copies: 1, delayMs: 50 }
)
```

`hedge(fn, opts)` returns the first non-error result. The other attempts receive an aborted `AbortSignal` and should bail out if they are still running.

So with `copies: 1, delayMs: 50`:

- Attempt 1 fires at `t=0`
- If attempt 1 has not resolved by `t=50ms`, attempt 2 fires
- Whichever resolves first wins; the other is cancelled

With `copies: 2, delayMs: 100`:

- Attempt 1 fires at `t=0`
- Attempt 2 fires at `t=100ms`
- Attempt 3 fires at `t=200ms`
- First success wins

## When this helps

- p99 dominated by a single slow tail (head-of-line blocking, GC pause on the peer)
- Idempotent reads — a duplicate call is safe
- Spare capacity downstream

## When this hurts

- Mutating, non-idempotent operations — never hedge `payment.charge` without idempotency on the peer
- The peer is already overloaded — hedging multiplies the pressure
- Per-caller rate limits — hedging counts as multiple calls against the limit

## Combining with idempotency

When the call goes through Nevo's [idempotency cache](./idempotency.md), all hedged copies share the same idempotency key. The first one to reach the server populates the cache; the others get the cached reply.

To force the same idempotency key across copies, set it explicitly:

```ts
await hedge(
  (attempt, signal) => client.query("user", "user.getById", { id }, {
    idempotencyKey: "fixed-key-shared-across-copies",
    signal
  }),
  { enabled: true, copies: 1, delayMs: 50 }
)
```

## Bounding the cost

Hedging adds roughly `copies * tail-probability` extra calls. If your tail is at the 99th percentile, one extra copy adds ~1% overhead on average. Two copies add ~2%. There is no built-in budget cap — keep `copies` small (1 or 2) and only enable hedging on hot, idempotent reads.

## Declarative form — `@Hedge`

Wrapping every call site with `hedge()` gets old. The same options can be declared once on the service method:

```ts
import { Hedge } from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends NatsClientBase {
  @Hedge({ copies: 1, delayMs: 50 })
  async getById(id: bigint) {
    return this.query("user", "user.getById", { id })
  }
}
```

The runtime in [`resilience-runtime.ts`](../src/common/resilience-runtime.ts) reads the metadata, picks up the same `HedgingOptions`, and applies the hedge around `query()` automatically. Combine with `@CircuitBreaker` and `@Adaptive` on the same method — the runtime layers them as `circuit → hedge → invoke` with adaptive feedback after the call.

See [resilience-decorators.md](./resilience-decorators.md) for the full picture.

## What is *still* not provided

- A "cancel on first response" flag — cancellation is the default; non-cancellable callers are responsible for their own cleanup.
- A `delays: number[]` array — each subsequent copy uses the same `delayMs` interval.

## See also

- [retry.md](./retry.md) — failure-driven (hedging is latency-driven)
- [circuit-breaker.md](./circuit-breaker.md) — the breaker still applies to every hedged attempt
- [idempotency.md](./idempotency.md) — what makes hedging safe
