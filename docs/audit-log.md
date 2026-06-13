# Audit log

`AuditLog` is an append-only record of every request a service handles: who called, which method, the (redacted) params and result, the outcome, and how long it took. One entry per request is written by the controller, fire-and-forget, so auditing never blocks or fails a request. Sinks are pluggable — in-memory, NDJSON file, Postgres, or a tee that fans out to several at once.

Source: `src/common/audit-log.ts`.

## The entry shape

```ts
interface AuditEntry {
  uuid: string
  ts: number
  service: string
  method: string
  caller: string | null
  tenantId?: string
  outcome: "ok" | "error"
  durationMs: number
  params: unknown
  result?: unknown                              // omitted on error
  error?: { code: number; message: string }     // present on error
  meta?: Partial<MessageMeta>                    // traceparent, callerService, instanceId, ts, version
}
```

## Wiring via the router `auditLog` option

You almost never construct `AuditEntry` by hand. Build an `AuditLog`, pass it to your signal-router (or controller), and the framework records one redacted entry per call automatically.

```ts
import { AuditLog, FileAuditSink, NatsSignalRouter } from "@riaskov/nevo-messaging"

const auditLog = new AuditLog({
  sink: new FileAuditSink({ path: "/var/log/nevo/audit.ndjson" }),
  redactPaths: ["params.password", "params.card.number", "result.token"]
})

@NatsSignalRouter([UserService], { auditLog })
export class UserRouter {}
```

Internally the router and `BaseMessageController` call `auditLog.recordFromResponse(...)` after the handler returns, deriving `outcome`/`error`/`result` from the response envelope (a `result === "error"` envelope becomes `outcome: "error"` and the `result` field is dropped). The call is **fire-and-forget** — wrapped so a sink failure can never block, slow, or fail request completion; failures are logged and the entry is dropped. The `caller`, `tenantId`, and trace fields are pulled from the inbound message meta.

`recordFromResponse` is also the right entry point if you build a custom router:

```ts
await auditLog.recordFromResponse({
  service, method, uuid, startedAt, params, response, meta, caller
})
```

To disable auditing without unwiring it, construct with `{ enabled: false }` — `isEnabled()` returns false and `record`/`recordFromResponse` become no-ops.

## Redaction

Every entry is redacted before it reaches a sink, using the same path-based [redaction](./redaction.md) engine as the rest of the framework:

- Pass `redactPaths` (dotted paths like `"params.password"`) to mask sensitive fields. They are applied to both `params` and `result`.
- **Size guard.** Entries are bounded by `maxEntryBytes` (default 32 KB). The normaliser estimates the redacted serialized size in a single non-allocating pass and, if it exceeds the budget, replaces `params` (and `result`, if present) with a `{ __dropped: "oversize", maxBytes }` marker — *before* paying for a deep redaction or building a throwaway JSON string. Within budget, it deep-redacts exactly once.

This keeps oversized payloads from bloating your audit store while still recording that the call happened, with whom, and its outcome.

## Sinks

A sink implements `write(entry)` and optionally `flush()` / `close()`:

```ts
interface AuditSink {
  write(entry: AuditEntry): Promise<void> | void
  flush?(): Promise<void> | void
  close?(): Promise<void> | void
}
```

### `InMemoryAuditSink`

A ring buffer (default cap 10,000 entries) for tests and the DevTools view. `list()` returns a copy; `clear()` empties it. This is the **default** sink when none is supplied.

### `FileAuditSink`

Append-only **NDJSON** (one JSON object per line). Rotation is external — point `logrotate` at the file.

```ts
new FileAuditSink({
  path: "/var/log/nevo/audit.ndjson",
  fsync: true,        // fsync after each write (default true) — survives crashes
  batchSize: 1,       // entries buffered before a forced flush (default 1)
  flushIntervalMs: 250
})
```

With `fsync: true` each batch is durably on disk before `write` resolves; this is the right setting for compliance because the file survives a DB outage and the OS append is atomic per line. Raise `batchSize` to trade durability granularity for throughput.

### `PgAuditSink`

Writes to a Postgres table (queryable JSONB). You supply a `query(text, values)`-shaped client — a 4-line wrapper around `pg`, `postgres`, or `pg-promise`.

```ts
new PgAuditSink({ client, table: "nevo_audit", bufferCap: 1000 })
```

Schema (also in the source doc comment):

```sql
CREATE TABLE nevo_audit (
  uuid        TEXT PRIMARY KEY,
  ts          TIMESTAMPTZ NOT NULL,
  service     TEXT NOT NULL,
  method      TEXT NOT NULL,
  caller      TEXT,
  tenant_id   TEXT,
  outcome     TEXT NOT NULL,
  duration_ms INT  NOT NULL,
  entry       JSONB NOT NULL
);
CREATE INDEX nevo_audit_ts     ON nevo_audit (ts);
CREATE INDEX nevo_audit_method ON nevo_audit (method);
CREATE INDEX nevo_audit_tenant ON nevo_audit (tenant_id);
```

Inserts are idempotent (`ON CONFLICT (uuid) DO NOTHING`). On a connection error the entry is **soft-buffered** (cap `bufferCap`, oldest dropped first) and retried on the next `write`/`flush`, so a transient DB blip doesn't lose recent audit history.

### `TeeAuditSink`

Fan-out to several sinks at once. Each `write`/`flush`/`close` is `Promise.allSettled`-ed, so one failing sink doesn't stop the others.

```ts
const auditLog = new AuditLog({
  sink: new TeeAuditSink([
    new PgAuditSink({ client }),                                  // queryable
    new FileAuditSink({ path: "/var/log/nevo/audit.ndjson" })     // durable, outage-proof
  ])
})
```

This pg-plus-file tee is the recommended setup for SOC2 / HIPAA / PCI: the file survives a database outage while Postgres gives you ad-hoc queries.

## Flushing & shutdown

`AuditLog#flush()` flushes the sink; `AuditLog#close()` flushes then closes it. Call `close()` from your [graceful shutdown](./graceful-shutdown.md) hook so buffered file/pg entries are persisted before exit.

## See also

- [redaction.md](./redaction.md) — the path-based redaction engine and `redactPaths` syntax
- [storage-matrix.md](./storage-matrix.md) — picking an audit backend
- [security.md](./security.md) — auth context that populates `caller`
- [devtools.md](./devtools.md) — viewing recent entries from the `InMemoryAuditSink`
