# Backpressure

`BackpressureLimiter` is a watermark-based gate that pauses an upstream subscription when too many handler calls are in flight. The point is to keep the process from accumulating an unbounded queue when the broker is faster than the handler.

## Real API

```ts
interface BackpressureOptions {
  maxInflight?: number     // hard cap on concurrent handlers, default: 100
  highWatermark?: number   // pause when inflight crosses this, default: 0.8 * maxInflight
  lowWatermark?: number    // resume when inflight falls below this, default: 0.4 * maxInflight
}

interface PausableSubscription {
  unsubscribe(): Promise<void>
  pause(): void
  resume(): void
  isPaused(): boolean
}

class BackpressureLimiter {
  constructor(opts: BackpressureOptions, callbacks: {
    onPause: () => void
    onResume: () => void
  })
  begin(): boolean      // returns false if at cap
  end(): void
  getInflight(): number
  isPaused(): boolean
}
```

## Usage

```ts
import { BackpressureLimiter } from "@riaskov/nevo-messaging"

const sub: PausableSubscription = await client.subscribe(...)

const limiter = new BackpressureLimiter(
  { maxInflight: 200, highWatermark: 160, lowWatermark: 80 },
  {
    onPause:  () => sub.pause(),
    onResume: () => sub.resume()
  }
)

async function handle(message) {
  if (!limiter.begin()) {
    // at cap — drop, or wait, depending on policy
    return
  }
  try {
    await processMessage(message)
  } finally {
    limiter.end()
  }
}
```

When `getInflight()` crosses `highWatermark`, `onPause()` fires once. When it falls below `lowWatermark`, `onResume()` fires once. The hysteresis prevents flap.

## Pausable subscriptions

`PausableSubscription` is what every Nevo `subscribe()` call returns when the underlying transport supports server-side pause:

- **NATS JetStream**: pulls are paused via `consumer.delete()` of the active pull and re-creation
- **Kafka**: `consumer.pause()` / `consumer.resume()` on the partition
- **Socket.IO / WebSocket**: stop pulling from the message queue

Use `sub.isPaused()` for diagnostics.

## When to use this

- Handler throughput is much lower than broker throughput
- Memory is constrained — you cannot afford an unbounded queue
- You can afford a slight extra latency in exchange for backpressure to flow upstream

## When not to use this

- Request-reply paths — there's no subscription to pause, just queue rejection
- Short-running handlers where in-flight count never gets above the watermark

## Automatic wiring — `@Backpressure` + `wrapSubscriptionHandler`

You no longer have to thread `begin()/end()` through every handler. Two equivalent paths exist now:

### 1. Declarative `@Backpressure` decorator

```ts
import { Backpressure } from "@riaskov/nevo-messaging"

@Injectable()
export class AuditService extends NatsClientBase {
  @Backpressure({ maxInflight: 200, highWatermark: 160, lowWatermark: 80, onOverflow: "nack" })
  async onUserUpdated(msg: { id: bigint }) {
    await project(msg)
  }
}
```

Combined with the resilience runtime, the call site fans through `applyResilience` which calls `begin()` before, `end()` after, and either rejects or nacks/drops when the cap is hit.

### 2. Explicit `wrapSubscriptionHandler`

```ts
import { wrapSubscriptionHandler } from "@riaskov/nevo-messaging"

const wrapped = wrapSubscriptionHandler(
  async (msg, ctx) => { await project(msg); await ctx.ack() },
  () => subscription,                                   // late-bound so it picks up the real Subscription
  { maxInflight: 200, highWatermark: 160, lowWatermark: 80, onOverflow: "reject" }
)

const sub = await client.subscribe("user", "user.updated", { ack: true }, wrapped)
```

`onOverflow` accepts:

- `"reject"` (default) — throw `RATE_LIMITED`; upstream retry/circuit reacts
- `"nack"` — call `ctx.nack("backpressure overflow")` so the broker redelivers
- `"drop"` — silently skip; useful for telemetry fan-out where loss is acceptable

The wrapper auto-pauses the subscription on high-watermark and auto-resumes on low-watermark, so transport-side flow control follows the in-process gate.

## What is *still* not provided

- No "executor with maxQueue" — `BackpressureLimiter` only gates new work via `begin()`.
- No per-tenant keying — one limiter per declaration. Use multiple subscriptions/decorators to partition.

## See also

- [adaptive.md](./adaptive.md) — adjust retry/timeout based on latency (a different axis)
- [rate-limiting.md](./rate-limiting.md) — token-bucket inbound rate cap
- [graceful-shutdown.md](./graceful-shutdown.md) — drain in-flight handlers cleanly
