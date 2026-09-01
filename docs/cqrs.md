# CQRS bridge & event store

Nevo ships two small primitives for CQRS-style architectures:

- `CqrsBridge` — proxies commands and events between a local `@nestjs/cqrs` bus and remote services over Nevo
- `InMemoryEventStore` — append-only event log for projection-source / time-travel use cases

Both are intentionally minimal; the framework does the wiring, you keep the domain.

For production, `PgEventStore` is the durable backend — see [Commit visibility](#commit-visibility-pgeventstore) for the one detail that matters when several processes append concurrently.

## `InMemoryEventStore`

```ts
import { InMemoryEventStore, type DomainEvent } from "@riaskov/nevo-messaging"

const store = new InMemoryEventStore()

const event = await store.append({
  type: "OrderPlaced",
  aggregateId: "order-123",
  payload: { items, total },
  meta: { tenantId: "t-42" }
})
// event.sequence and event.id and event.ts are assigned

const history = await store.read({ aggregateId: "order-123" })

const sub = await store.subscribe(0, async (e) => {
  // called for every event from sequence 0 onwards, plus all new events
  await project(e)
})

## Commit visibility (`PgEventStore`)

`sequence` comes from a `BIGSERIAL`, assigned when the row is **inserted**. Transactions
commit in a different order, so a poller that simply orders by `sequence` can read
event 6 (committed), advance its cursor to 7, and never see event 5 when its slower
transaction commits a moment later.

Every row therefore records its inserting transaction in a `txid xid8` column, and the
subscriber reads with `committedOnly: true`, which adds:

```sql
WHERE txid < pg_snapshot_xmin(pg_current_snapshot())
```

`pg_snapshot_xmin` is the lowest transaction id still running, so a row below it was
written by a transaction that has already finished. No later commit can introduce a
lower `sequence` behind one that has been delivered.

The practical consequences:

- **Postgres 13+ is required** (`xid8`, `pg_current_xact_id`). `migrate()` adds the
  column and a `(txid, sequence)` index.
- **A long-running transaction holds the tail back.** Delivery waits for the oldest
  in-flight writer, so keep `append` out of transactions that stay open for minutes.
  Latency is bounded by your longest write transaction, not by the poll interval.
- **Appends are no longer serialised.** Earlier versions took a per-table advisory lock
  on every append to keep `sequence` gap-free; that capped the entire store at one
  writer and still broke when `append` ran inside a caller's transaction.

A plain `read()` is unaffected and returns everything the calling transaction can see —
use it for aggregate history and projections rebuilt on demand.

### Poison events

`subscribe` retries a throwing handler with exponential backoff and, after
`maxAttemptsPerEvent` (default 5), hands the event to `onPoison` and moves on, so one
bad event cannot park the stream:

```ts
await store.subscribe(0, project, {
  maxAttemptsPerEvent: 5,
  onPoison: (event, err) => dlq.route({ topic: "projections", reason: "poison-event", error: { message: String(err) }, rawPayload: event, ts: Date.now() })
})
```

Without an `onPoison` sink the event is logged and dropped.
await sub.unsubscribe()
```

API:

```ts
interface EventStore {
  append(input: {
    type: string
    aggregateId?: string
    payload: unknown
    meta?: unknown
  }): Promise<DomainEvent>

  read(range?: {
    from?: number
    to?: number
    type?: string
    aggregateId?: string
    limit?: number
  }): Promise<DomainEvent[]>

  subscribe?(from: number, handler: (e: DomainEvent) => void | Promise<void>): Promise<{ unsubscribe(): void }>
}

interface DomainEvent {
  id: string
  type: string
  aggregateId?: string
  payload: unknown
  meta?: unknown
  sequence: number   // monotonic, store-assigned
  ts: number
}
```

Only the in-memory implementation ships. For durable storage, write your own `EventStore` against Postgres / EventStoreDB / etc. — it is a 3-method interface.

Snapshotting is not built-in; if you need it, store snapshots in a separate table indexed by `aggregateId` and `sequence`, and start reads at `(snapshot.sequence + 1)`.

## `CqrsBridge`

The bridge bolts `@nestjs/cqrs` commands and events onto Nevo:

```ts
import { CqrsBridge } from "@riaskov/nevo-messaging"
import { CommandBus, EventBus } from "@nestjs/cqrs"

@Injectable()
export class OrderCqrsModule {
  private bridge: CqrsBridge

  constructor(
    @Inject("NEVO_NATS_CLIENT") private nevo: NevoNatsClient,
    private commandBus: CommandBus,
    private eventBus: EventBus
  ) {
    this.bridge = new CqrsBridge({
      service: "order",
      client: { query: nevo.query.bind(nevo), emit: nevo.emit.bind(nevo) },
      remoteCommands: ["payment.charge", "shipping.book"],
      remoteEvents: ["order.placed", "order.cancelled"]
    })
    this.bridge.attachToCommandBus(this.commandBus)
    this.bridge.attachToEventBus(this.eventBus)
  }
}
```

What this does:

- Commands dispatched to the local `CommandBus` whose name appears in `remoteCommands` are forwarded as `query(service, name, payload)` over Nevo.
- Events published on the local `EventBus` whose name appears in `remoteEvents` are forwarded as `emit(service, name, payload)`.
- Commands/events NOT in those lists are handled by your local `@CommandHandler` / `@EventHandler` classes as usual.

Constructor options:

```ts
interface CqrsBridgeOptions {
  service: string                            // remote service name to forward to
  client: { query: ...; emit: ... }          // any Nevo client base
  remoteCommands?: string[]                  // command names to forward
  remoteEvents?: string[]                    // event names to forward
  commandKey?: (cmd: unknown) => string      // default: cmd.constructor.name
  eventKey?: (ev: unknown) => string         // default: ev.constructor.name
}
```

Direct usage without attaching to buses is also fine:

```ts
const result = await bridge.executeRemote(new ChargeCardCommand({ amount, userId }))
await bridge.publishRemote(new OrderPlacedEvent({ orderId, items }))
```

`shouldForwardCommand(cmd)` / `shouldForwardEvent(ev)` are exposed for inspection.

## What is NOT provided

- No SQL `EventStore` implementation — write your own.
- No snapshotting helpers.
- No `forFeature(...)` NestJS module helper. Instantiate `CqrsBridge` directly.
- No "CQRS read model" sync engine — combine the event store's `subscribe()` with the [inbox](./inbox.md) for exactly-once projections.

## When to use each piece

| Need | Tool |
|---|---|
| Stateless RPC | Plain `query()` |
| Fire-and-forget integration event | Plain `emit()` |
| Audit log of state changes | `EventStore` |
| Reconstruct read models from history | `EventStore.subscribe(0, …)` |
| Time-travel queries | `EventStore.read({ to: oldSeq })` |
| Mix local CQRS + remote service | `CqrsBridge` |

The event store and bridge are opt-in; most services do not need them.
