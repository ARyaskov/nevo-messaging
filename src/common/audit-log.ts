import { promises as fs } from "node:fs"
import { dirname } from "node:path"
import { redactObject, jsonByteSize } from "./redact"
import type { MessageMeta, MessageResponse } from "./types"
import { getDefaultLogger, type NevoLogger } from "./logger"

// Append-only audit log. Pluggable sinks (in-memory, file, pg, tee).
// Integrated into BaseMessageController: one entry per request, redacted.

export type AuditOutcome = "ok" | "error"

export interface AuditEntry {
  uuid: string
  ts: number
  service: string
  method: string
  caller: string | null
  tenantId?: string
  outcome: AuditOutcome
  durationMs: number
  params: unknown
  result?: unknown
  error?: { code: number; message: string }
  meta?: Partial<MessageMeta>
}

export interface AuditSink {
  write(entry: AuditEntry): Promise<void> | void
  flush?(): Promise<void> | void
  close?(): Promise<void> | void
}

export interface AuditLogOptions {
  enabled?: boolean
  redactPaths?: string[]
  /** Drop params/result above this serialized size. Default 32 KB. */
  maxEntryBytes?: number
  sink?: AuditSink
  logger?: NevoLogger
}

export class AuditLog {
  private readonly enabled: boolean
  private readonly redactPaths?: string[]
  private readonly maxEntryBytes: number
  private readonly sink: AuditSink
  private readonly logger: NevoLogger

  constructor(opts?: AuditLogOptions) {
    this.enabled = opts?.enabled !== false
    this.redactPaths = opts?.redactPaths
    this.maxEntryBytes = opts?.maxEntryBytes ?? 32 * 1024
    this.sink = opts?.sink ?? new InMemoryAuditSink()
    this.logger = (opts?.logger ?? getDefaultLogger()).child({ component: "audit" })
  }

  isEnabled(): boolean { return this.enabled }

  /** Record one request/response. Never throws — sink errors are logged. */
  async record(entry: Omit<AuditEntry, "params" | "result"> & { params: unknown; result?: unknown }): Promise<void> {
    if (!this.enabled) return
    const normalised = this.normalise(entry as AuditEntry)
    try {
      await this.sink.write(normalised)
    } catch (err) {
      this.logger.warn(
        { event: "audit.sink.failed", uuid: entry.uuid, err: (err as Error)?.message },
        "Audit sink write failed; entry dropped"
      )
    }
  }

  /** Convenience for the controller: build an entry from current call state. */
  async recordFromResponse(args: {
    service: string
    method: string
    uuid: string
    startedAt: number
    params: unknown
    response: MessageResponse
    meta?: MessageMeta
    caller?: string | null
  }): Promise<void> {
    if (!this.enabled) return
    const { service, method, uuid, startedAt, params, response, meta, caller } = args
    const errored = (response.params as { result: unknown }).result === "error"
    const err = errored ? (response.params as { error?: { code: number; message: string } }).error : undefined
    await this.record({
      uuid,
      ts: Date.now(),
      service,
      method,
      caller: caller ?? meta?.service ?? null,
      tenantId: meta?.tenantId,
      outcome: errored ? "error" : "ok",
      durationMs: Math.max(0, Date.now() - startedAt),
      params,
      result: errored ? undefined : (response.params as { result: unknown }).result,
      error: errored ? { code: err?.code ?? 0, message: err?.message ?? "" } : undefined,
      meta: meta
        ? {
            traceparent: (meta.trace as any)?.traceparent,
            callerService: (meta as any).callerService,
            instanceId: meta.instanceId,
            ts: meta.ts,
            version: meta.version
          }
        : undefined
    })
  }

  async flush(): Promise<void> {
    if (this.sink.flush) await this.sink.flush()
  }

  async close(): Promise<void> {
    if (this.sink.flush) await this.sink.flush()
    if (this.sink.close) await this.sink.close()
  }

  private normalise(entry: AuditEntry): AuditEntry {
    // Size-guard FIRST: a single non-allocating pass that estimates the redacted
    // serialized size and bails as soon as the budget is exceeded. Oversized
    // payloads short-circuit before we pay for deep redaction, and we never build
    // a throwaway JSON string to measure.
    if (jsonByteSize(entry, this.maxEntryBytes, this.redactPaths) > this.maxEntryBytes) {
      const dropped = { __dropped: "oversize" as const, maxBytes: this.maxEntryBytes }
      return {
        ...entry,
        params: dropped,
        result: entry.result === undefined ? undefined : dropped
      }
    }
    // Within budget: deep-redact once. No second serialization pass.
    return {
      ...entry,
      params: redactObject(entry.params, this.redactPaths),
      result: entry.result === undefined ? undefined : redactObject(entry.result, this.redactPaths)
    }
  }
}

/** In-memory ring-buffer sink for tests and DevTools. */
export class InMemoryAuditSink implements AuditSink {
  private readonly entries: AuditEntry[] = []
  private readonly max: number
  constructor(maxEntries = 10_000) { this.max = maxEntries }
  write(entry: AuditEntry): void {
    this.entries.push(entry)
    if (this.entries.length > this.max) this.entries.shift()
  }
  list(): AuditEntry[] { return [...this.entries] }
  clear(): void { this.entries.length = 0 }
}

export interface FileAuditSinkOptions {
  path: string
  /** fsync after each write. Default true. */
  fsync?: boolean
  /** Buffered entries before forced flush. Default 1. */
  batchSize?: number
  flushIntervalMs?: number
}

/** NDJSON append-only file sink. Rotation is external (logrotate). */
export class FileAuditSink implements AuditSink {
  private buffer: AuditEntry[] = []
  private handle: fs.FileHandle | null = null
  private opening: Promise<void> | null = null
  private timer?: NodeJS.Timeout
  private readonly opts: Required<FileAuditSinkOptions>

  constructor(opts: FileAuditSinkOptions) {
    if (!opts.path) throw new Error("FileAuditSink: `path` is required")
    this.opts = {
      path: opts.path,
      fsync: opts.fsync !== false,
      batchSize: Math.max(1, opts.batchSize ?? 1),
      flushIntervalMs: Math.max(50, opts.flushIntervalMs ?? 250)
    }
  }

  private async ensureOpen(): Promise<void> {
    if (this.handle) return
    if (this.opening) return this.opening
    this.opening = (async () => {
      await fs.mkdir(dirname(this.opts.path), { recursive: true }).catch(() => {})
      this.handle = await fs.open(this.opts.path, "a")
    })()
    await this.opening
  }

  async write(entry: AuditEntry): Promise<void> {
    this.buffer.push(entry)
    if (this.buffer.length >= this.opts.batchSize) {
      await this.flush()
    } else if (!this.timer) {
      this.timer = setTimeout(() => { void this.flush() }, this.opts.flushIntervalMs)
      if (typeof this.timer.unref === "function") this.timer.unref()
    }
  }

  async flush(): Promise<void> {
    if (this.timer) { clearTimeout(this.timer); this.timer = undefined }
    if (this.buffer.length === 0) return
    await this.ensureOpen()
    if (!this.handle) return
    const lines = this.buffer.map((e) => JSON.stringify(e)).join("\n") + "\n"
    this.buffer = []
    await this.handle.write(lines)
    if (this.opts.fsync) await this.handle.sync()
  }

  async close(): Promise<void> {
    await this.flush()
    if (this.handle) {
      const h = this.handle
      this.handle = null
      await h.close()
    }
  }
}

// Postgres sink. Caller supplies a `query(text, values)`-shaped client —
// 4-line wrapper around `pg`, `postgres`, or `pg-promise`.
//
// Schema:
//   CREATE TABLE nevo_audit (
//     uuid        TEXT PRIMARY KEY,
//     ts          TIMESTAMPTZ NOT NULL,
//     service     TEXT NOT NULL,
//     method      TEXT NOT NULL,
//     caller      TEXT,
//     tenant_id   TEXT,
//     outcome     TEXT NOT NULL,
//     duration_ms INT  NOT NULL,
//     entry       JSONB NOT NULL
//   );
//   CREATE INDEX nevo_audit_ts     ON nevo_audit (ts);
//   CREATE INDEX nevo_audit_method ON nevo_audit (method);
//   CREATE INDEX nevo_audit_tenant ON nevo_audit (tenant_id);
export interface AuditPgClient {
  query(text: string, values?: unknown[]): Promise<unknown>
}

export interface PgAuditSinkOptions {
  client: AuditPgClient
  table?: string
  /** Soft-fail buffer for connection errors. Default 1000. */
  bufferCap?: number
  logger?: NevoLogger
}

export class PgAuditSink implements AuditSink {
  private readonly client: AuditPgClient
  private readonly table: string
  private readonly bufferCap: number
  private readonly logger: NevoLogger
  private readonly buffer: AuditEntry[] = []

  constructor(opts: PgAuditSinkOptions) {
    if (!opts.client) throw new Error("PgAuditSink: `client` is required")
    this.client = opts.client
    this.table = opts.table ?? "nevo_audit"
    this.bufferCap = Math.max(1, opts.bufferCap ?? 1000)
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "audit.pg" })
  }

  async write(entry: AuditEntry): Promise<void> {
    try {
      if (this.buffer.length > 0) {
        const pending = this.buffer.splice(0, this.buffer.length)
        for (const p of pending) await this.insertOne(p)
      }
      await this.insertOne(entry)
    } catch (err) {
      this.buffer.push(entry)
      while (this.buffer.length > this.bufferCap) this.buffer.shift()
      this.logger.warn({ event: "audit.pg.buffered", size: this.buffer.length, err: (err as Error)?.message })
    }
  }

  private async insertOne(entry: AuditEntry): Promise<void> {
    const sql = `INSERT INTO ${this.table}
      (uuid, ts, service, method, caller, tenant_id, outcome, duration_ms, entry)
      VALUES ($1, to_timestamp($2 / 1000.0), $3, $4, $5, $6, $7, $8, $9::jsonb)
      ON CONFLICT (uuid) DO NOTHING`
    await this.client.query(sql, [
      entry.uuid,
      entry.ts,
      entry.service,
      entry.method,
      entry.caller,
      entry.tenantId ?? null,
      entry.outcome,
      entry.durationMs,
      JSON.stringify(entry)
    ])
  }

  async flush(): Promise<void> {
    if (this.buffer.length === 0) return
    const pending = this.buffer.splice(0, this.buffer.length)
    for (const p of pending) {
      try { await this.insertOne(p) } catch { this.buffer.unshift(p); break }
    }
  }
}

/** Fan-out: forward each entry to all wrapped sinks. */
export class TeeAuditSink implements AuditSink {
  constructor(private readonly sinks: AuditSink[]) {}
  async write(entry: AuditEntry): Promise<void> {
    await Promise.allSettled(this.sinks.map((s) => Promise.resolve(s.write(entry))))
  }
  async flush(): Promise<void> {
    await Promise.allSettled(this.sinks.map((s) => Promise.resolve(s.flush?.())))
  }
  async close(): Promise<void> {
    await Promise.allSettled(this.sinks.map((s) => Promise.resolve(s.close?.())))
  }
}
