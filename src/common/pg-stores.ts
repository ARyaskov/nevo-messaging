import type { OutboxStore, OutboxRecord, OutboxMarkResult } from "./outbox"
import type { InboxStore } from "./inbox"
import type { IdempotencyClaim } from "./idempotency-store"
import type { SagaStore, SagaSnapshot } from "./saga"
import type { EventStore, DomainEvent, EventStoreReadRange } from "./event-store"
import type { DlqStore, DlqEntry, DlqQuery, DlqStats } from "./dlq"
import type { ScheduledTask, ScheduledTaskStore } from "./scheduler"
import { uuidv7 } from "./uuid"
import { getDefaultLogger, type NevoLogger } from "./logger"
import { stringifyWithBigInt, deserializeBigInt } from "./bigint.utils"

/** Postgres backends for outbox, inbox, saga, event store, DLQ, and scheduled tasks. */

export interface PgQueryResult<T = unknown> {
  rows: T[]
  rowCount?: number
}

export interface PgClient {
  query<T = unknown>(text: string, values?: unknown[]): Promise<PgQueryResult<T>>
}

export interface PgStoreOptions {
  client: PgClient
  schema?: string
  logger?: NevoLogger
}

function qident(schema: string | undefined, table: string): string {
  if (!schema || schema === "public") return `"${table}"`
  return `"${schema}"."${table}"`
}

// ===========================================================================
// PgOutboxStore
// ===========================================================================

export interface PgOutboxStoreOptions extends PgStoreOptions {
  table?: string
  /** How long a claim is honoured before another worker can steal it. Default 60s. */
  claimTtlMs?: number
}

/** Postgres outbox store with claim-fenced publish (FOR UPDATE SKIP LOCKED). */
export class PgOutboxStore implements OutboxStore {
  private readonly client: PgClient
  private readonly table: string
  private readonly claimTtlMs: number
  private readonly workerId: string
  private readonly logger: NevoLogger

  constructor(opts: PgOutboxStoreOptions) {
    if (!opts.client) throw new Error("PgOutboxStore: `client` is required")
    this.client = opts.client
    this.table = qident(opts.schema, opts.table ?? "nevo_outbox")
    this.claimTtlMs = opts.claimTtlMs ?? 60_000
    this.workerId = `worker-${uuidv7().slice(0, 12)}`
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "outbox.pg" })
  }

  async migrate(): Promise<void> {
    await this.client.query(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id            TEXT PRIMARY KEY,
        service_name  TEXT NOT NULL,
        method        TEXT NOT NULL,
        params        JSONB NOT NULL,
        partition_key TEXT,
        status        TEXT NOT NULL DEFAULT 'pending',
        attempts      INT  NOT NULL DEFAULT 0,
        last_error    TEXT,
        created_at    TIMESTAMPTZ NOT NULL,
        claimed_at    TIMESTAMPTZ,
        claimed_by    TEXT,
        published_at  TIMESTAMPTZ
      );
    `)
    await this.client.query(`ALTER TABLE ${this.table} ADD COLUMN IF NOT EXISTS partition_key TEXT;`)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_outbox_pending_idx ON ${this.table} (status, created_at)
        WHERE status = 'pending';
    `)
    // Backs the correlated partition-ordering subqueries in listPending().
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_outbox_partition_idx ON ${this.table} (partition_key, status, created_at, id)
        WHERE partition_key IS NOT NULL;
    `)
  }

  /** Persist a pending record; pass `tx` to write in the caller's transaction. */
  async save(record: OutboxRecord, tx?: PgClient): Promise<void> {
    const client = tx ?? this.client
    await client.query(
      `INSERT INTO ${this.table} (id, service_name, method, params, partition_key, status, attempts, created_at)
       VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, to_timestamp($8 / 1000.0))
       ON CONFLICT (id) DO NOTHING`,
      [
        record.id,
        record.serviceName,
        record.method,
        stringifyWithBigInt(record.params),
        record.partitionKey ?? null,
        record.status,
        record.attempts,
        record.createdAt
      ]
    )
  }

  async markPublished(id: string): Promise<OutboxMarkResult> {
    const res = await this.client.query<{ status: string; attempts: number }>(
      `UPDATE ${this.table}
          SET status = 'published', published_at = NOW()
        WHERE id = $1 AND claimed_by = $2 AND status = 'pending'
       RETURNING status, attempts`,
      [id, this.workerId]
    )
    const row = res.rows[0]
    if (!row) {
      this.logger.warn(
        { id, worker: this.workerId },
        "outbox.markPublished: row not owned (claim stolen) or already finalized — not counting as published"
      )
      return { owned: false, status: "published", attempts: 0 }
    }
    return { owned: true, status: row.status as OutboxRecord["status"], attempts: row.attempts }
  }

  async markFailed(id: string, error: string, maxAttempts: number): Promise<OutboxMarkResult> {
    const res = await this.client.query<{ status: string; attempts: number }>(
      `UPDATE ${this.table}
          SET status = CASE WHEN attempts + 1 >= $3 THEN 'failed' ELSE 'pending' END,
              attempts = attempts + 1,
              last_error = $2,
              claimed_at = NULL,
              claimed_by = NULL
        WHERE id = $1 AND claimed_by = $4 AND status = 'pending'
       RETURNING status, attempts`,
      [id, error.slice(0, 4000), maxAttempts, this.workerId]
    )
    const row = res.rows[0]
    if (!row) {
      this.logger.warn({ id, worker: this.workerId }, "outbox.markFailed: row not owned (claim stolen) or already finalized — ignoring")
      return { owned: false, status: "pending", attempts: 0 }
    }
    return { owned: true, status: row.status as OutboxRecord["status"], attempts: row.attempts }
  }

  async listPending(limit: number): Promise<OutboxRecord[]> {
    const res = await this.client.query<{
      id: string
      service_name: string
      method: string
      params: unknown
      partition_key: string | null
      attempts: number
      status: string
      last_error: string | null
      created_at: Date
    }>(
      `WITH cte AS (
         SELECT id FROM ${this.table} o
          WHERE o.status = 'pending'
            AND (o.claimed_at IS NULL OR o.claimed_at < NOW() - ($2 || ' milliseconds')::interval)
            AND (o.partition_key IS NULL OR (
              NOT EXISTS (
                SELECT 1 FROM ${this.table} f
                 WHERE f.partition_key = o.partition_key
                   AND f.status = 'failed'
                   AND (f.created_at < o.created_at OR (f.created_at = o.created_at AND f.id < o.id))
              )
              AND NOT EXISTS (
                SELECT 1 FROM ${this.table} c
                 WHERE c.partition_key = o.partition_key
                   AND c.status = 'pending'
                   AND c.claimed_by IS NOT NULL
                   AND c.claimed_by <> $3
                   AND c.claimed_at >= NOW() - ($2 || ' milliseconds')::interval
              )
            ))
          ORDER BY o.created_at ASC
          LIMIT $1
          FOR UPDATE SKIP LOCKED
       )
       UPDATE ${this.table}
          SET claimed_at = NOW(), claimed_by = $3
         FROM cte
        WHERE ${this.table}.id = cte.id
       RETURNING ${this.table}.id, service_name, method, params, partition_key, attempts, status, last_error, created_at`,
      [limit, String(this.claimTtlMs), this.workerId]
    )
    return res.rows.map((r) => ({
      id: r.id,
      serviceName: r.service_name,
      method: r.method,
      params: deserializeBigInt(r.params),
      partitionKey: r.partition_key ?? undefined,
      attempts: r.attempts,
      status: r.status as OutboxRecord["status"],
      lastError: r.last_error ?? undefined,
      createdAt: r.created_at instanceof Date ? r.created_at.getTime() : Number(r.created_at)
    }))
  }
}

// ===========================================================================
// PgInboxStore
// ===========================================================================

export interface PgInboxStoreOptions extends PgStoreOptions {
  table?: string
  /** TTL after which dedup state is purged. Default 24h. */
  ttlMs?: number
  /** How long a `pending` claim is honoured before another worker may steal it. Default 60s. */
  claimTtlMs?: number
}

/** Postgres inbox (dedup) store with an atomic cross-replica claim. */
export class PgInboxStore implements InboxStore {
  private readonly client: PgClient
  private readonly table: string
  private readonly ttlMs: number
  private readonly claimTtlMs: number

  constructor(opts: PgInboxStoreOptions) {
    if (!opts.client) throw new Error("PgInboxStore: `client` is required")
    this.client = opts.client
    this.table = qident(opts.schema, opts.table ?? "nevo_inbox")
    this.ttlMs = opts.ttlMs ?? 24 * 60 * 60_000
    this.claimTtlMs = opts.claimTtlMs ?? 60_000
  }

  async migrate(): Promise<void> {
    await this.client.query(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        uuid    TEXT PRIMARY KEY,
        result  JSONB,
        status  TEXT NOT NULL DEFAULT 'done',
        seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `)
    await this.client.query(`ALTER TABLE ${this.table} ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'done';`)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_inbox_seen_at_idx ON ${this.table} (seen_at);
    `)
  }

  async hasSeen(uuid: string): Promise<boolean> {
    const res = await this.client.query<{ exists: boolean }>(
      `SELECT EXISTS(SELECT 1 FROM ${this.table} WHERE uuid = $1 AND status = 'done') AS exists`,
      [uuid]
    )
    return Boolean(res.rows[0]?.exists)
  }

  /** Single-winner reservation; a stale `pending` claim (crashed peer) is stolen after `claimTtlMs`. */
  async claim(uuid: string, opts?: { ttlMs?: number }): Promise<IdempotencyClaim<unknown>> {
    const ttl = opts?.ttlMs ?? this.claimTtlMs
    const res = await this.client.query<{ uuid: string }>(
      `INSERT INTO ${this.table} AS t (uuid, result, status) VALUES ($1, NULL, 'pending')
       ON CONFLICT (uuid) DO UPDATE SET seen_at = NOW()
         WHERE t.status = 'pending' AND t.seen_at < NOW() - ($2 || ' milliseconds')::interval
       RETURNING uuid`,
      [uuid, String(ttl)]
    )
    if (res.rows.length > 0) return { acquired: true }
    const existing = await this.client.query<{ result: unknown; status: string }>(`SELECT result, status FROM ${this.table} WHERE uuid = $1`, [uuid])
    const row = existing.rows[0]
    if (row?.status === "done" && row.result !== null && row.result !== undefined) {
      return { acquired: false, existing: deserializeBigInt(row.result) }
    }
    return { acquired: false }
  }

  async isDone(uuid: string): Promise<boolean> {
    return this.hasSeen(uuid)
  }

  async markSeen(uuid: string, result?: unknown): Promise<void> {
    await this.client.query(
      `INSERT INTO ${this.table} AS t (uuid, result, status) VALUES ($1, $2::jsonb, 'done')
       ON CONFLICT (uuid) DO UPDATE SET result = EXCLUDED.result, status = 'done', seen_at = NOW()
         WHERE t.status = 'pending'`,
      [uuid, result === undefined ? null : stringifyWithBigInt(result)]
    )
  }

  async getResult(uuid: string): Promise<unknown | undefined> {
    const res = await this.client.query<{ result: unknown }>(`SELECT result FROM ${this.table} WHERE uuid = $1 AND status = 'done'`, [uuid])
    const stored = res.rows[0]?.result
    return stored === undefined || stored === null ? undefined : deserializeBigInt(stored)
  }

  /** Delete rows older than `ttlMs`. Call from a daily cron. */
  async prune(): Promise<number> {
    const res = await this.client.query(`DELETE FROM ${this.table} WHERE seen_at < NOW() - ($1 || ' milliseconds')::interval`, [String(this.ttlMs)])
    return res.rowCount ?? 0
  }
}

// ===========================================================================
// PgSagaStore
// ===========================================================================

export interface PgSagaStoreOptions extends PgStoreOptions {
  table?: string
}

/** Postgres saga snapshot store. */
export class PgSagaStore implements SagaStore {
  private readonly client: PgClient
  private readonly table: string

  constructor(opts: PgSagaStoreOptions) {
    if (!opts.client) throw new Error("PgSagaStore: `client` is required")
    this.client = opts.client
    this.table = qident(opts.schema, opts.table ?? "nevo_saga")
  }

  async migrate(): Promise<void> {
    await this.client.query(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        saga_id    TEXT PRIMARY KEY,
        type       TEXT NOT NULL DEFAULT 'default',
        status     TEXT NOT NULL,
        steps      JSONB NOT NULL,
        executed   JSONB NOT NULL,
        ctx        JSONB NOT NULL,
        error      TEXT,
        updated_at TIMESTAMPTZ NOT NULL
      );
    `)
    await this.client.query(`
      ALTER TABLE ${this.table} ADD COLUMN IF NOT EXISTS type TEXT NOT NULL DEFAULT 'default';
    `)
    await this.client.query(`ALTER TABLE ${this.table} ADD COLUMN IF NOT EXISTS compensated JSONB NOT NULL DEFAULT '[]'::jsonb;`)
    await this.client.query(`ALTER TABLE ${this.table} ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;`)
    await this.client.query(`ALTER TABLE ${this.table} ADD COLUMN IF NOT EXISTS claimed_by TEXT;`)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_saga_pending_idx ON ${this.table} (status)
        WHERE status IN ('pending', 'compensating');
    `)
  }

  async save(s: SagaSnapshot): Promise<void> {
    await this.client.query(
      `INSERT INTO ${this.table} (saga_id, type, status, steps, executed, compensated, ctx, error, updated_at)
       VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb, $8, to_timestamp($9 / 1000.0))
       ON CONFLICT (saga_id) DO UPDATE SET
         type        = EXCLUDED.type,
         status      = EXCLUDED.status,
         steps       = EXCLUDED.steps,
         executed    = EXCLUDED.executed,
         compensated = EXCLUDED.compensated,
         ctx         = EXCLUDED.ctx,
         error       = EXCLUDED.error,
         updated_at  = EXCLUDED.updated_at`,
      [
        s.sagaId,
        s.type ?? "default",
        s.status,
        stringifyWithBigInt(s.steps),
        stringifyWithBigInt(s.executed),
        stringifyWithBigInt(s.compensated ?? []),
        stringifyWithBigInt(s.ctx),
        s.error?.message ?? null,
        s.updatedAt
      ]
    )
  }

  /** Recovery lease: single winner per saga until the lease expires. */
  async claim(sagaId: string, workerId: string, leaseMs: number): Promise<boolean> {
    const res = await this.client.query<{ saga_id: string }>(
      `UPDATE ${this.table}
          SET claimed_at = NOW(), claimed_by = $2
        WHERE saga_id = $1
          AND status IN ('pending', 'compensating')
          AND (claimed_at IS NULL OR claimed_by = $2 OR claimed_at < NOW() - ($3 || ' milliseconds')::interval)
       RETURNING saga_id`,
      [sagaId, workerId, String(leaseMs)]
    )
    return res.rows.length > 0
  }

  async load(sagaId: string): Promise<SagaSnapshot | null> {
    const res = await this.client.query<{
      saga_id: string
      type: string | null
      status: string
      steps: string[]
      executed: string[]
      compensated: string[] | null
      ctx: unknown
      error: string | null
      updated_at: Date
    }>(
      `SELECT saga_id, type, status, steps, executed, compensated, ctx, error, updated_at
         FROM ${this.table} WHERE saga_id = $1`,
      [sagaId]
    )
    const row = res.rows[0]
    if (!row) return null
    return {
      sagaId: row.saga_id,
      type: row.type ?? "default",
      status: row.status as SagaSnapshot["status"],
      steps: row.steps,
      executed: row.executed,
      compensated: row.compensated ?? [],
      ctx: deserializeBigInt(row.ctx),
      error: row.error ? { message: row.error } : undefined,
      updatedAt: row.updated_at instanceof Date ? row.updated_at.getTime() : Number(row.updated_at)
    }
  }

  async listPending(): Promise<SagaSnapshot[]> {
    const res = await this.client.query<{
      saga_id: string
      type: string | null
      status: string
      steps: string[]
      executed: string[]
      compensated: string[] | null
      ctx: unknown
      error: string | null
      updated_at: Date
    }>(
      `SELECT saga_id, type, status, steps, executed, compensated, ctx, error, updated_at
         FROM ${this.table}
        WHERE status IN ('pending', 'compensating')`
    )
    return res.rows.map((row) => ({
      sagaId: row.saga_id,
      type: row.type ?? "default",
      status: row.status as SagaSnapshot["status"],
      steps: row.steps,
      executed: row.executed,
      compensated: row.compensated ?? [],
      ctx: deserializeBigInt(row.ctx),
      error: row.error ? { message: row.error } : undefined,
      updatedAt: row.updated_at instanceof Date ? row.updated_at.getTime() : Number(row.updated_at)
    }))
  }

  async delete(sagaId: string): Promise<void> {
    await this.client.query(`DELETE FROM ${this.table} WHERE saga_id = $1`, [sagaId])
  }
}

// ===========================================================================
// PgEventStore
// ===========================================================================

export interface PgEventStoreOptions extends PgStoreOptions {
  table?: string
}

/** Postgres event store. */
export class PgEventStore implements EventStore {
  private readonly client: PgClient
  private readonly table: string
  private readonly logger: NevoLogger | undefined

  constructor(opts: PgEventStoreOptions) {
    if (!opts.client) throw new Error("PgEventStore: `client` is required")
    this.client = opts.client
    this.table = qident(opts.schema, opts.table ?? "nevo_events")
    this.logger = opts.logger ? opts.logger.child({ component: "event-store.pg" }) : undefined
  }

  async migrate(): Promise<void> {
    await this.client.query(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        sequence     BIGSERIAL PRIMARY KEY,
        id           TEXT NOT NULL UNIQUE,
        type         TEXT NOT NULL,
        aggregate_id TEXT,
        payload      JSONB NOT NULL,
        meta         JSONB,
        ts           TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_events_aggregate_idx ON ${this.table} (aggregate_id, sequence);
    `)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_events_type_idx ON ${this.table} (type);
    `)
  }

  /**
   * Appends one event. A per-table advisory lock serialises appends so the
   * `sequence` column is strictly gap-free (required by the poll-based
   * `subscribe`/`read` cursor). This caps append throughput to one writer at a
   * time for the whole store; if you need higher write concurrency, shard the
   * store per aggregate or switch consumers to a gap-tolerant cursor.
   */
  async append(input: Omit<DomainEvent, "id" | "sequence" | "ts">): Promise<DomainEvent> {
    const id = uuidv7()
    const res = await this.client.query<{ sequence: string | number; ts: Date }>(
      `INSERT INTO ${this.table} (id, type, aggregate_id, payload, meta)
       SELECT $1, $2, $3, $4::jsonb, $5::jsonb
         FROM (SELECT pg_advisory_xact_lock(hashtext($6))) AS seq_lock
       RETURNING sequence, ts`,
      [id, input.type, input.aggregateId ?? null, stringifyWithBigInt(input.payload), input.meta ? stringifyWithBigInt(input.meta) : null, this.table]
    )
    const row = res.rows[0]!
    return {
      id,
      type: input.type,
      aggregateId: input.aggregateId,
      payload: input.payload,
      meta: input.meta,
      sequence: typeof row.sequence === "string" ? Number(row.sequence) : row.sequence,
      ts: row.ts instanceof Date ? row.ts.getTime() : Number(row.ts)
    }
  }

  async read(range: EventStoreReadRange = {}): Promise<DomainEvent[]> {
    const filters: string[] = []
    const params: unknown[] = []
    if (range.from !== undefined) {
      params.push(range.from)
      filters.push(`sequence >= $${params.length}`)
    }
    if (range.to !== undefined) {
      params.push(range.to)
      filters.push(`sequence <= $${params.length}`)
    }
    if (range.type) {
      params.push(range.type)
      filters.push(`type = $${params.length}`)
    }
    if (range.aggregateId) {
      params.push(range.aggregateId)
      filters.push(`aggregate_id = $${params.length}`)
    }
    const where = filters.length ? `WHERE ${filters.join(" AND ")}` : ""
    let limitClause = ""
    if (range.limit !== undefined) {
      params.push(range.limit)
      limitClause = `LIMIT $${params.length}`
    }
    const sql = `SELECT sequence, id, type, aggregate_id, payload, meta, ts
                   FROM ${this.table} ${where}
                   ORDER BY sequence ASC ${limitClause}`
    const res = await this.client.query<{
      sequence: string | number
      id: string
      type: string
      aggregate_id: string | null
      payload: unknown
      meta: unknown
      ts: Date
    }>(sql, params)
    return res.rows.map((r) => ({
      sequence: typeof r.sequence === "string" ? Number(r.sequence) : r.sequence,
      id: r.id,
      type: r.type,
      aggregateId: r.aggregate_id ?? undefined,
      payload: deserializeBigInt(r.payload),
      meta: r.meta == null ? undefined : (deserializeBigInt(r.meta) as Record<string, unknown>),
      ts: r.ts instanceof Date ? r.ts.getTime() : Number(r.ts)
    }))
  }

  async subscribe(
    from: number,
    handler: (event: DomainEvent) => Promise<void> | void,
    opts?: { pollIntervalMs?: number; batchSize?: number }
  ): Promise<{ unsubscribe(): Promise<void> }> {
    let cursor = from
    let stopped = false
    const interval = Math.max(50, opts?.pollIntervalMs ?? 200)
    const batchSize = Math.max(1, opts?.batchSize ?? 500)
    const tick = async () => {
      if (stopped) return
      try {
        let more = true
        while (more && !stopped) {
          const events = await this.read({ from: cursor, limit: batchSize })
          more = events.length === batchSize
          for (const e of events) {
            if (stopped) break
            try {
              await handler(e)
            } catch (err) {
              // Handler threw: stop without advancing the cursor so the event retries next tick.
              this.logger?.warn(
                { sequence: e.sequence, id: e.id, type: e.type, err: (err as Error)?.message ?? String(err) },
                "event-store.subscribe: handler failed; retrying event next tick (cursor not advanced)"
              )
              more = false
              break
            }
            cursor = e.sequence + 1
          }
        }
      } catch {}
      if (!stopped) timer = setTimeout(tick, interval)
      if (timer && typeof timer.unref === "function") timer.unref()
    }
    let timer: NodeJS.Timeout | undefined = setTimeout(tick, 0)
    if (typeof timer.unref === "function") timer.unref()
    return {
      unsubscribe: async () => {
        stopped = true
        if (timer) clearTimeout(timer)
      }
    }
  }
}

// ===========================================================================
// PgDlqStore
// ===========================================================================

export interface PgDlqStoreOptions extends PgStoreOptions {
  table?: string
}

/** Postgres DLQ store. */
export class PgDlqStore implements DlqStore {
  private readonly client: PgClient
  private readonly table: string

  constructor(opts: PgDlqStoreOptions) {
    if (!opts.client) throw new Error("PgDlqStore: `client` is required")
    this.client = opts.client
    this.table = qident(opts.schema, opts.table ?? "nevo_dlq")
  }

  async migrate(): Promise<void> {
    await this.client.query(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id         TEXT PRIMARY KEY,
        topic      TEXT NOT NULL,
        reason     TEXT NOT NULL,
        method     TEXT,
        error_code INT,
        ts         TIMESTAMPTZ NOT NULL,
        entry      JSONB NOT NULL
      );
    `)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_dlq_topic_ts_idx ON ${this.table} (topic, ts);
    `)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_dlq_method_idx ON ${this.table} (method);
    `)
  }

  async push(entry: DlqEntry): Promise<void> {
    const id = entry.id ?? uuidv7()
    const method = (entry.meta as { method?: string })?.method
    await this.client.query(
      `INSERT INTO ${this.table} (id, topic, reason, method, error_code, ts, entry)
       VALUES ($1, $2, $3, $4, $5, to_timestamp($6 / 1000.0), $7::jsonb)
       ON CONFLICT (id) DO NOTHING`,
      [id, entry.topic, entry.reason, method ?? null, entry.error?.code ?? null, entry.ts, stringifyWithBigInt({ ...entry, id })]
    )
  }

  async list(limit = 100): Promise<DlqEntry[]> {
    const res = await this.client.query<{ entry: DlqEntry }>(`SELECT entry FROM ${this.table} ORDER BY ts DESC LIMIT $1`, [limit])
    return res.rows.map((r) => deserializeBigInt(r.entry) as DlqEntry)
  }

  async query(q: DlqQuery): Promise<DlqEntry[]> {
    const filters: string[] = []
    const params: unknown[] = []
    if (q.topic) {
      params.push(q.topic)
      filters.push(`topic = $${params.length}`)
    }
    if (q.method) {
      params.push(q.method)
      filters.push(`method = $${params.length}`)
    }
    if (q.reason) {
      params.push(q.reason)
      filters.push(`reason = $${params.length}`)
    }
    if (q.code !== undefined) {
      params.push(q.code)
      filters.push(`error_code = $${params.length}`)
    }
    if (q.since !== undefined) {
      params.push(q.since)
      filters.push(`ts >= to_timestamp($${params.length} / 1000.0)`)
    }
    if (q.until !== undefined) {
      params.push(q.until)
      filters.push(`ts <= to_timestamp($${params.length} / 1000.0)`)
    }
    const where = filters.length ? `WHERE ${filters.join(" AND ")}` : ""
    params.push(q.limit ?? 100)
    const res = await this.client.query<{ entry: DlqEntry }>(
      `SELECT entry FROM ${this.table} ${where} ORDER BY ts DESC LIMIT $${params.length}`,
      params
    )
    return res.rows.map((r) => deserializeBigInt(r.entry) as DlqEntry)
  }

  /** Aggregate DLQ stats in a single round-trip; `sinceMs` restricts the time window. */
  async stats(sinceMs?: number): Promise<DlqStats> {
    const params: unknown[] = []
    let windowClause = ""
    if (sinceMs !== undefined) {
      params.push(sinceMs)
      windowClause = `WHERE ts >= to_timestamp($${params.length} / 1000.0)`
    }
    const res = await this.client.query<{
      kind: string
      key: string | null
      count: string
      oldest: Date | null
      newest: Date | null
    }>(
      `WITH f AS (
         SELECT reason, error_code, method, ts FROM ${this.table} ${windowClause}
       )
       SELECT 'total'  AS kind, NULL::text AS key, COUNT(*) AS count, MIN(ts) AS oldest, MAX(ts) AS newest FROM f
       UNION ALL
       SELECT 'reason' AS kind, reason AS key, COUNT(*) AS count, NULL::timestamptz AS oldest, NULL::timestamptz AS newest FROM f GROUP BY reason
       UNION ALL
       SELECT 'code'   AS kind, error_code::text AS key, COUNT(*) AS count, NULL::timestamptz AS oldest, NULL::timestamptz AS newest FROM f GROUP BY error_code
       UNION ALL
       SELECT 'method' AS kind, method AS key, COUNT(*) AS count, NULL::timestamptz AS oldest, NULL::timestamptz AS newest FROM f GROUP BY method`,
      params
    )
    const byReason: Record<string, number> = {}
    const byCode: Record<string, number> = {}
    const byMethod: Record<string, number> = {}
    let total = 0
    let oldestTs: number | undefined
    let newestTs: number | undefined
    const toMs = (v: Date | null): number | undefined => (v ? (v instanceof Date ? v.getTime() : Number(v)) : undefined)
    for (const r of res.rows) {
      if (r.kind === "total") {
        total = Number(r.count ?? 0)
        oldestTs = toMs(r.oldest)
        newestTs = toMs(r.newest)
      } else if (r.kind === "reason") {
        byReason[r.key ?? "unknown"] = Number(r.count)
      } else if (r.kind === "code") {
        byCode[r.key === null ? "unknown" : String(r.key)] = Number(r.count)
      } else if (r.kind === "method") {
        byMethod[r.key ?? "unknown"] = Number(r.count)
      }
    }
    return { total, byReason, byCode, byMethod, oldestTs, newestTs }
  }

  async remove(id: string): Promise<void> {
    await this.client.query(`DELETE FROM ${this.table} WHERE id = $1`, [id])
  }

  async clear(): Promise<void> {
    await this.client.query(`TRUNCATE ${this.table}`)
  }
}

// ===========================================================================
// PgScheduledTaskStore
// ===========================================================================

export interface PgScheduledTaskStoreOptions extends PgStoreOptions {
  table?: string
}

export class PgScheduledTaskStore implements ScheduledTaskStore {
  private readonly client: PgClient
  private readonly table: string

  constructor(opts: PgScheduledTaskStoreOptions) {
    if (!opts.client) throw new Error("PgScheduledTaskStore: `client` is required")
    this.client = opts.client
    this.table = qident(opts.schema, opts.table ?? "nevo_scheduled")
  }

  async migrate(): Promise<void> {
    await this.client.query(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id           TEXT PRIMARY KEY,
        name         TEXT NOT NULL,
        payload      JSONB,
        run_at       TIMESTAMPTZ NOT NULL,
        cron         TEXT,
        timezone     TEXT,
        attempts     INT  NOT NULL DEFAULT 0,
        max_attempts INT  NOT NULL DEFAULT 5,
        status       TEXT NOT NULL DEFAULT 'pending',
        last_error   TEXT,
        claimed_at   TIMESTAMPTZ,
        claimed_by   TEXT,
        completed_at TIMESTAMPTZ,
        created_at   TIMESTAMPTZ NOT NULL
      );
    `)
    await this.client.query(`
      ALTER TABLE ${this.table} ADD COLUMN IF NOT EXISTS timezone TEXT;
    `)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_scheduled_due_idx ON ${this.table} (status, run_at)
        WHERE status IN ('pending', 'running');
    `)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_scheduled_name_idx ON ${this.table} (name);
    `)
  }

  async enqueue(task: ScheduledTask): Promise<void> {
    await this.client.query(
      `INSERT INTO ${this.table} AS scheduled (id, name, payload, run_at, cron, timezone, attempts, max_attempts, status, created_at)
       VALUES ($1, $2, $3::jsonb, to_timestamp($4 / 1000.0), $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))
       ON CONFLICT (id) DO UPDATE
         SET name = EXCLUDED.name,
             payload = EXCLUDED.payload,
             run_at = EXCLUDED.run_at,
             cron = EXCLUDED.cron,
             timezone = EXCLUDED.timezone,
             attempts = 0,
             max_attempts = EXCLUDED.max_attempts,
             status = 'pending',
             last_error = NULL,
             claimed_at = NULL,
             claimed_by = NULL,
             completed_at = NULL
       WHERE EXCLUDED.cron IS NOT NULL
         AND (scheduled.cron IS DISTINCT FROM EXCLUDED.cron
           OR scheduled.timezone IS DISTINCT FROM EXCLUDED.timezone
           OR scheduled.name IS DISTINCT FROM EXCLUDED.name)`,
      [
        task.id,
        task.name,
        task.payload === undefined ? null : stringifyWithBigInt(task.payload),
        task.runAt,
        task.cron ?? null,
        task.timezone ?? null,
        task.attempts,
        task.maxAttempts,
        task.status,
        task.createdAt
      ]
    )
  }

  async claimDue(workerId: string, now: number, limit: number, claimTtlMs: number): Promise<ScheduledTask[]> {
    const res = await this.client.query<{
      id: string
      name: string
      payload: unknown
      run_at: Date
      cron: string | null
      timezone: string | null
      attempts: number
      max_attempts: number
      status: string
      last_error: string | null
      created_at: Date
    }>(
      // Claims pending tasks and reclaims 'running' tasks past their lease (DB clock).
      `WITH cte AS (
         SELECT id FROM ${this.table}
          WHERE run_at <= to_timestamp($1 / 1000.0)
            AND (
              status = 'pending'
              OR (status = 'running'
                  AND claimed_at IS NOT NULL
                  AND claimed_at < NOW() - ($2 || ' milliseconds')::interval)
            )
          ORDER BY run_at ASC
          LIMIT $3
          FOR UPDATE SKIP LOCKED
       )
       UPDATE ${this.table}
          SET status = 'running', claimed_at = NOW(), claimed_by = $4
         FROM cte
        WHERE ${this.table}.id = cte.id
       RETURNING ${this.table}.id, name, payload, run_at, cron, timezone, attempts, max_attempts,
                 status, last_error, created_at`,
      [now, String(claimTtlMs), limit, workerId]
    )
    return res.rows.map((r) => ({
      id: r.id,
      name: r.name,
      payload: deserializeBigInt(r.payload),
      runAt: r.run_at instanceof Date ? r.run_at.getTime() : Number(r.run_at),
      cron: r.cron ?? undefined,
      timezone: r.timezone ?? undefined,
      attempts: r.attempts,
      maxAttempts: r.max_attempts,
      status: r.status as ScheduledTask["status"],
      lastError: r.last_error ?? undefined,
      createdAt: r.created_at instanceof Date ? r.created_at.getTime() : Number(r.created_at)
    }))
  }

  // markCompleted/markFailed/reschedule are fenced by `claimed_by` and `status = 'running'`.
  async markCompleted(id: string, workerId: string): Promise<void> {
    await this.client.query(
      `UPDATE ${this.table} SET status = 'completed', completed_at = NOW()
        WHERE id = $1 AND claimed_by = $2 AND status = 'running'`,
      [id, workerId]
    )
  }

  async markFailed(id: string, error: string, workerId: string): Promise<void> {
    await this.client.query(
      `UPDATE ${this.table}
          SET attempts = attempts + 1,
              last_error = $2,
              claimed_at = NULL,
              claimed_by = NULL,
              status = CASE WHEN attempts + 1 >= max_attempts THEN 'failed' ELSE 'pending' END
        WHERE id = $1 AND claimed_by = $3 AND status = 'running'`,
      [id, error.slice(0, 4000), workerId]
    )
  }

  async reschedule(id: string, nextRunAt: number, workerId: string, error?: string): Promise<void> {
    await this.client.query(
      `UPDATE ${this.table}
          SET status = 'pending', run_at = to_timestamp($2 / 1000.0),
              attempts = 0, claimed_at = NULL, claimed_by = NULL,
              completed_at = NULL, last_error = COALESCE($4, last_error)
        WHERE id = $1 AND claimed_by = $3 AND status = 'running'`,
      [id, nextRunAt, workerId, error?.slice(0, 4000) ?? null]
    )
  }

  /** Heartbeat: refresh the lease of tasks this worker is still executing. */
  async extendLease(ids: string[], workerId: string): Promise<void> {
    if (ids.length === 0) return
    await this.client.query(`UPDATE ${this.table} SET claimed_at = NOW() WHERE id = ANY($1::text[]) AND claimed_by = $2 AND status = 'running'`, [
      ids,
      workerId
    ])
  }

  async cancel(id: string): Promise<void> {
    await this.client.query(`UPDATE ${this.table} SET status = 'cancelled' WHERE id = $1`, [id])
  }

  async list(filter?: { status?: ScheduledTask["status"]; limit?: number }): Promise<ScheduledTask[]> {
    const params: unknown[] = []
    let where = ""
    if (filter?.status) {
      params.push(filter.status)
      where = `WHERE status = $${params.length}`
    }
    params.push(filter?.limit ?? 100)
    const res = await this.client.query<{
      id: string
      name: string
      payload: unknown
      run_at: Date
      cron: string | null
      timezone: string | null
      attempts: number
      max_attempts: number
      status: string
      last_error: string | null
      created_at: Date
    }>(
      `SELECT id, name, payload, run_at, cron, timezone, attempts, max_attempts, status, last_error, created_at
         FROM ${this.table} ${where}
         ORDER BY run_at ASC LIMIT $${params.length}`,
      params
    )
    return res.rows.map((r) => ({
      id: r.id,
      name: r.name,
      payload: deserializeBigInt(r.payload),
      runAt: r.run_at instanceof Date ? r.run_at.getTime() : Number(r.run_at),
      cron: r.cron ?? undefined,
      timezone: r.timezone ?? undefined,
      attempts: r.attempts,
      maxAttempts: r.max_attempts,
      status: r.status as ScheduledTask["status"],
      lastError: r.last_error ?? undefined,
      createdAt: r.created_at instanceof Date ? r.created_at.getTime() : Number(r.created_at)
    }))
  }
}

// ===========================================================================
// One-shot migrate helper
// ===========================================================================

export async function migrateAllPgStores(client: PgClient, schema?: string): Promise<void> {
  await new PgOutboxStore({ client, schema }).migrate()
  await new PgInboxStore({ client, schema }).migrate()
  await new PgSagaStore({ client, schema }).migrate()
  await new PgEventStore({ client, schema }).migrate()
  await new PgDlqStore({ client, schema }).migrate()
  await new PgScheduledTaskStore({ client, schema }).migrate()
}
