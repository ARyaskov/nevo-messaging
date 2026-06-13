# Chain context (request correlation)

A **chain id** is a per-conversation correlation key that ties together every request in a fan-out: when service A queries B, and B queries C, all three share the same chain id. It propagates implicitly across `await` boundaries via `AsyncLocalStorage`, so you get distributed-trace-style correlation in the [DevTools UI](./devtools.md) without deploying OpenTelemetry.

Source: `src/common/chain-context.ts`.

```
┌──────────┐  query     ┌──────────┐  query     ┌──────────┐
│ frontend │ ─────────► │   user   │ ─────────► │ contract │
│ chain=X  │            │ chain=X  │            │ chain=X  │
└──────────┘            └──────────┘            └──────────┘
```

## The context

```ts
interface ChainContext {
  chainId: string
  parentUuid?: string   // envelope uuid of the inbound message that triggered this handler
}
```

`chainId` is a [UUIDv7](./bigint.md), so it embeds a timestamp and sorts naturally by start time when grouped in the dashboard. `parentUuid` lets DevTools render the fan-out as a **tree** (parent → children) rather than a flat list: each outbound call inside a handler points back at the inbound message that triggered it.

## Implicit propagation

The mechanism is a single module-level `AsyncLocalStorage<ChainContext>`. Anything that runs inside `runInChain(ctx, fn)` — handlers, awaited continuations, microtasks, timers tracked by async hooks — sees the same context with no parameter threading.

```ts
import { runInChain, getCurrentChainId } from "@riaskov/nevo-messaging"

runInChain({ chainId: "X" }, async () => {
  getCurrentChainId()        // "X" — here and in everything it awaits
  await doWork()             // doWork() and its descendants also see "X"
})
getCurrentChainId()          // undefined — outside the run scope
```

| Function | Description |
|---|---|
| `runInChain(ctx, fn)` | Run `fn` (and all async descendants) within `ctx`. Returns `fn`'s result. |
| `getCurrentChainContext()` | The active `ChainContext`, or `undefined` when no handler is running. |
| `getCurrentChainId()` | Just the active chain id (or `undefined`). |
| `newChainId()` | Mint a fresh UUIDv7 chain id. |
| `resolveInboundChainId(metaChainId)` | Pick the chain id for a freshly-arrived message (see below). |
| `resolveOutboundChainId(explicit?)` | Pick the chain id for an outbound envelope (see below). |
| `getChainStorage()` | The underlying `AsyncLocalStorage`, for tests/advanced composition. |

## How the runtime wires it

You rarely call these directly — the transports do it for you. The chain id travels on the message meta as `nevoChainId`.

**Inbound (every signal-router).** Before running your handler, the router resolves the chain id from the inbound meta and establishes the ALS context:

```ts
const chainId = resolveInboundChainId(meta?.nevoChainId)
return runInChain({ chainId, parentUuid: inboundUuid }, () => handler(...))
```

`resolveInboundChainId(metaChainId)` resolution order:

1. **Honor the caller's chain id** if the inbound meta carries one (the common A → B → C path).
2. Otherwise inherit from the current ALS context (rare — mostly nested handlers in tests).
3. Otherwise **mint a new one** — this message is the entry-point of a chain.

**Outbound (every Nevo client).** When building an outbound envelope's meta, the client stamps `nevoChainId` using:

`resolveOutboundChainId(explicit?)` resolution order:

1. An explicit override, if you pass one (rare).
2. Otherwise **inherit from the ALS context** if we're inside a handler — this is what links the child call to its parent.
3. Otherwise mint a new chain id (start of a chain).

The net effect: an inbound request establishes a chain id; any outbound calls the handler makes pick it up automatically and forward it on the wire; the next service honors it; and so on down the fan-out.

## The DevTools trace view

Each client and controller emits a DevTools event carrying `{ service, method, uuid, chainId, parentUuid, durationMs, status, transport, origin }`. Because every hop in a conversation shares the chain id (and points at its parent uuid), the [DevTools UI](./devtools.md) groups events by `chainId` and nests them by `parentUuid` to reconstruct the full request tree — who called whom, in what order, and how long each hop took — across services and transports, without an OpenTelemetry collector.

If you *do* run OpenTelemetry, the two coexist: the chain id is an additional, lightweight correlation key, and the trace `traceparent` still flows in meta alongside it (see [observability.md](./observability.md)).

## Manual use

Outside a transport handler (a cron job, a CLI, a test) you can open a chain scope yourself so any Nevo calls inside it are correlated:

```ts
import { runInChain, newChainId } from "@riaskov/nevo-messaging"

await runInChain({ chainId: newChainId() }, async () => {
  await userClient.query("user", "user.get", { id })       // outbound inherits the chain id
  await auditClient.emit("audit", "audit.write", { ... })  // same chain id
})
```

## See also

- [devtools.md](./devtools.md) — the trace view that consumes chain ids
- [observability.md](./observability.md) — OpenTelemetry tracing alongside chain ids
- [architecture.md](./architecture.md) — where chain context sits in the request lifecycle
