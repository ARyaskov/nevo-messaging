# CQRS bridge & event store

Nevo ships two small primitives for CQRS-style architectures:

- `CqrsBridge` — proxies commands and events between a local `@nestjs/cqrs` bus and remote services over Nevo
- `InMemoryEventStore` — append-only event log for projection-source / time-travel use cases

Both are intentionally minimal; the framework does the wiring, you keep the domain.

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
