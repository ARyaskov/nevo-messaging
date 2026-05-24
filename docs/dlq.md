# Dead-letter queue (DLQ)

A dead-letter queue captures envelopes that could not be delivered or processed after all retries.

## Real API

```ts
import {
  DlqRouter,
  InMemoryDlqStore,
  DEFAULT_DLQ_SUFFIX,         // ".dlq"
  type DlqEntry,
  type DlqQuery,
  type DlqStats,
  type DlqStore,
  type DlqSink
} from "@riaskov/nevo-messaging"

interface DlqEntry {
  id?: string
  topic: string
  reason: string             // "handler_error", "delivery_exhausted", ...
  error?: { code?: number; message?: string }
  meta?: Record<string, unknown>
  rawPayload?: unknown
  ts: number
  attempts?: number
}

interface DlqStore {
  push(entry: DlqEntry): Promise<void>
  list(limit?: number): Promise<DlqEntry[]>
  query?(q?: DlqQuery): Promise<DlqEntry[]>
  stats?(): Promise<DlqStats>
  remove(id: string): Promise<void>
  clear?(): Promise<void>
}

class DlqRouter {
  constructor(opts?: DlqRouterOptions)
  isEnabled(): boolean
  addSink(sink: DlqSink): void
  route(entry: DlqEntry): Promise<void>
  startReplay(): void
  stopReplay(): void
  getStore(): DlqStore | undefined
  query(q?: DlqQuery): Promise<DlqEntry[]>
  stats(): Promise<DlqStats>
  replayOnce(): Promise<{ replayed: number; failed: number }>
}
```

## Wiring

```ts
const store = new InMemoryDlqStore()
const dlq = new DlqRouter({
  enabled: true,
  store,
  redactPaths: ["password", "token"]
})

// Send everything to the store:
dlq.addSink(async (entry) => { await store.push(entry) })

// Plus: forward critical failures to ops:
dlq.addSink(async (entry) => {
  if (entry.error?.code === 9 /* CIRCUIT_OPEN */) await notifySlack(entry)
})
```

Routing happens automatically from inside the framework's retry/circuit/handler paths. You can also push entries manually:

```ts
await dlq.route({ topic: "user-events", reason: "manual", ts: Date.now(), rawPayload: payload })
```

## Per-transport built-in DLQ

When you enable `dlq: { enabled: true }` on a transport client (Kafka especially), the framework's retry-exhaustion path writes failing envelopes to a sibling topic with the `.dlq` suffix (default `DEFAULT_DLQ_SUFFIX`). Example: `user-events.dlq`.

## Querying

```ts
const recent = await dlq.query({
  method: "order.place",
  reason: "handler_error",
  since: Date.now() - 3_600_000,
  limit: 100
})
```

`DlqQuery` filters:

| Field | Match |
|---|---|
| `topic` | exact |
| `method` | exact |
| `reason` | exact |
| `code` | exact (numeric `ErrorCode`) |
| `since` / `until` | unix-ms range |
| `limit` | max rows |

## Aggregated stats

```ts
const stats: DlqStats = await dlq.stats()
// → { total, byReason: {...}, byCode: {...}, byMethod: {...}, oldestTs?, newestTs? }
```

The [DevTools dashboard](./devtools.md) renders this on the Errors page.

## Replay

```ts
const result = await dlq.replayOnce()
// → { replayed: N, failed: M }
```

Configure replay via `replay`:

```ts
new DlqRouter({
  enabled: true,
  store,
  replay: {
    intervalMs: 30_000,           // periodic replay if you call startReplay()
    maxAttempts: 5,
    policy: (entry) => entry.error?.code === 2 /* TIMEOUT */, // pick what to retry
    handler: async (entry) => {
      await republishToTopic(entry.topic, entry.rawPayload)
    }
  }
})

dlq.startReplay()  // start the periodic loop
// later:
dlq.stopReplay()
```

`replay.handler` is required if you want automated replay — it tells the router *how* to re-publish each entry. Most setups call back into the appropriate transport client.

## Sinks

A sink is `(entry: DlqEntry) => Promise<void> | void`. Multiple sinks can be attached — they run in registration order. Use them to:

- Persist to the store
- Page on-call
- Forward to a SIEM
- Move to another topic for human triage

## Redaction

`redactPaths` is forwarded to the framework's `redactObject` for sink payloads — keep secrets out of DLQ logs.

## Custom store

`InMemoryDlqStore` is fine for short-lived dev work and tests. For production, implement `DlqStore` against your DB (4 methods, 2 optional). Postgres with a JSONB column for `rawPayload` and indices on `(reason, ts)` works well.

## Alerting suggestions

- `rate(nevo_dlq_added_total[5m]) > 10` — sudden poison-pill surge
- `nevo_dlq_size > 10_000` — operator attention needed
- `nevo_dlq_oldest_age_seconds > 86_400` — operator forgot to drain

(Names are illustrative — wire whatever you emit via your metrics helper.)
