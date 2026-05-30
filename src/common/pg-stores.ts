import type { OutboxStore, OutboxRecord, OutboxMarkResult } from "./outbox"
import type { InboxStore } from "./inbox"
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

/**
 * Postgres outbox.
 *
 * Schema:
 *   CREATE TABLE nevo_outbox (
 *     id            TEXT PRIMARY KEY,
 *     service_name  TEXT NOT NULL,
 *     method        TEXT NOT NULL,
 *     params        JSONB NOT NULL,
 *     partition_key TEXT,
 *     status        TEXT NOT NULL DEFAULT 'pending',
 *     attempts      INT  NOT NULL DEFAULT 0,
 *     last_error    TEXT,
 *     created_at    TIMESTAMPTZ NOT NULL,
 *     claimed_at    TIMESTAMPTZ,
 *     claimed_by    TEXT,
 *     published_at  TIMESTAMPTZ
 *   );
 *   CREATE INDEX nevo_outbox_pending ON nevo_outbox (status, created_at)
 *     WHERE status = 'pending';
 *
 * Ownership: `listPending` claims rows with FOR UPDATE SKIP LOCKED, stamping
 * `claimed_by`/`claimed_at`. `markPublished`/`markFailed` only mutate a row that
 * is still `claimed_by` THIS worker and still `pending`, so a worker whose claim
 * was stolen after its TTL expired cannot re-finalize the row (no double publish
 * of the outbox state). Re-delivery to the broker can still happen — that is the
 * at-least-once contract; pair with the consumer-side inbox/idempotency cache.
 *
 * `claimed_by` (a per-instance unique worker id) is the fencing identity here.
 * A monotonic claim epoch would be strictly stronger (it survives worker-id
 * reuse), but each `PgOutboxStore` mints a fresh uuid-derived `workerId`, so the
 * id already serves as a unique fence without an extra column.
 */
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
    // Forward-migrate tables created before partition_key existed.
    await this.client.query(`ALTER TABLE ${this.table} ADD COLUMN IF NOT EXISTS partition_key TEXT;`)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_outbox_pending_idx ON ${this.table} (status, created_at)
        WHERE status = 'pending';
    `)
  }

  /**
   * Persist a pending record. Pass `tx` (a live connection running your
   * business transaction) to write the outbox row in the SAME BEGIN/COMMIT as
   * the state change. Without it the write uses the store's own connection,
   * which is unsafe — see {@link OutboxStore.save} and `withOutboxTransaction`.
   */
  async save(record: OutboxRecord, tx?: PgClient): Promise<void> {
    const client = tx ?? this.client
    await client.query(
      `INSERT INTO ${this.table} (id, service_name, method, params, partition_key, status, attempts, created_at)
       VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, to_timestamp($8 / 1000.0))
       ON CONFLICT (id) DO NOTHING`,
      [record.id, record.serviceName, record.method, stringifyWithBigInt(record.params),
       record.partitionKey ?? null, record.status, record.attempts, record.createdAt]
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
      this.logger.warn({ id, worker: this.workerId },
        "outbox.markPublished: row not owned (claim stolen) or already finalized — not counting as published")
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
      this.logger.warn({ id, worker: this.workerId },
        "outbox.markFailed: row not owned (claim stolen) or already finalized — ignoring")
      return { owned: false, status: "pending", attempts: 0 }
    }
    return { owned: true, status: row.status as OutboxRecord["status"], attempts: row.attempts }
  }

  async listPending(limit: number): Promise<OutboxRecord[]> {
    const res = await this.client.query<{
      id: string; service_name: string; method: string; params: unknown;
      partition_key: string | null; attempts: number; status: string;
      last_error: string | null; created_at: Date
    }>(
      `WITH cte AS (
         SELECT id FROM ${this.table}
          WHERE status = 'pending'
            AND (claimed_at IS NULL OR claimed_at < NOW() - ($2 || ' milliseconds')::interval)
          ORDER BY created_at ASC
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
}

/**
 * Postgres inbox.
 *
 * Schema:
 *   CREATE TABLE nevo_inbox (
 *     uuid        TEXT PRIMARY KEY,
 *     result      JSONB,
 *     seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
 *   );
 *   CREATE INDEX nevo_inbox_seen_at ON nevo_inbox (seen_at);
 */
export class PgInboxStore implements InboxStore {
  private readonly client: PgClient
  private readonly table: string
  private readonly ttlMs: number

  constructor(opts: PgInboxStoreOptions) {
    if (!opts.client) throw new Error("PgInboxStore: `client` is required")
    this.client = opts.client
    this.table = qident(opts.schema, opts.table ?? "nevo_inbox")
    this.ttlMs = opts.ttlMs ?? 24 * 60 * 60_000
  }

  async migrate(): Promise<void> {
    await this.client.query(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        uuid    TEXT PRIMARY KEY,
        result  JSONB,
        seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_inbox_seen_at_idx ON ${this.table} (seen_at);
    `)
  }

  async hasSeen(uuid: string): Promise<boolean> {
    const res = await this.client.query<{ exists: boolean }>(
      `SELECT EXISTS(SELECT 1 FROM ${this.table} WHERE uuid = $1) AS exists`,
      [uuid]
    )
    return Boolean(res.rows[0]?.exists)
  }

  async markSeen(uuid: string, result?: unknown): Promise<void> {
    await this.client.query(
      `INSERT INTO ${this.table} (uuid, result) VALUES ($1, $2::jsonb)
       ON CONFLICT (uuid) DO NOTHING`,
      [uuid, result === undefined ? null : stringifyWithBigInt(result)]
    )
  }

  async getResult(uuid: string): Promise<unknown | undefined> {
    const res = await this.client.query<{ result: unknown }>(
      `SELECT result FROM ${this.table} WHERE uuid = $1`,
      [uuid]
    )
    const stored = res.rows[0]?.result
    return stored === undefined || stored === null ? undefined : deserializeBigInt(stored)
  }

  /** Delete rows older than `ttlMs`. Call from a daily cron. */
  async prune(): Promise<number> {
    const res = await this.client.query(
      `DELETE FROM ${this.table} WHERE seen_at < NOW() - ($1 || ' milliseconds')::interval`,
      [String(this.ttlMs)]
    )
    return res.rowCount ?? 0
  }
}

// ===========================================================================
// PgSagaStore
// ===========================================================================

export interface PgSagaStoreOptions extends PgStoreOptions {
  table?: string
}

/**
 * Postgres saga snapshot store.
 *
 * Schema:
 *   CREATE TABLE nevo_saga (
 *     saga_id     TEXT PRIMARY KEY,
 *     type        TEXT NOT NULL DEFAULT 'default',
 *     status      TEXT NOT NULL,
 *     steps       JSONB NOT NULL,
 *     executed    JSONB NOT NULL,
 *     ctx         JSONB NOT NULL,
 *     error       TEXT,
 *     updated_at  TIMESTAMPTZ NOT NULL
 *   );
 *   CREATE INDEX nevo_saga_pending ON nevo_saga (status)
 *     WHERE status IN ('pending', 'compensating');
 */
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
    // Idempotently add `type` to tables created before crash recovery existed.
    await this.client.query(`
      ALTER TABLE ${this.table} ADD COLUMN IF NOT EXISTS type TEXT NOT NULL DEFAULT 'default';
    `)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_saga_pending_idx ON ${this.table} (status)
        WHERE status IN ('pending', 'compensating');
    `)
  }

  async save(s: SagaSnapshot): Promise<void> {
    await this.client.query(
      `INSERT INTO ${this.table} (saga_id, type, status, steps, executed, ctx, error, updated_at)
       VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7, to_timestamp($8 / 1000.0))
       ON CONFLICT (saga_id) DO UPDATE SET
         type       = EXCLUDED.type,
         status     = EXCLUDED.status,
         steps      = EXCLUDED.steps,
         executed   = EXCLUDED.executed,
         ctx        = EXCLUDED.ctx,
         error      = EXCLUDED.error,
         updated_at = EXCLUDED.updated_at`,
      [s.sagaId, s.type ?? "default", s.status, stringifyWithBigInt(s.steps), stringifyWithBigInt(s.executed),
       stringifyWithBigInt(s.ctx), s.error?.message ?? null, s.updatedAt]
    )
  }

  async load(sagaId: string): Promise<SagaSnapshot | null> {
    const res = await this.client.query<{
      saga_id: string; type: string | null; status: string; steps: string[]; executed: string[]; ctx: unknown;
      error: string | null; updated_at: Date
    }>(
      `SELECT saga_id, type, status, steps, executed, ctx, error, updated_at
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
      ctx: deserializeBigInt(row.ctx),
      error: row.error ? { message: row.error } : undefined,
      updatedAt: row.updated_at instanceof Date ? row.updated_at.getTime() : Number(row.updated_at)
    }
  }

  async listPending(): Promise<SagaSnapshot[]> {
    const res = await this.client.query<{
      saga_id: string; type: string | null; status: string; steps: string[]; executed: string[]; ctx: unknown;
      error: string | null; updated_at: Date
    }>(
      `SELECT saga_id, type, status, steps, executed, ctx, error, updated_at
         FROM ${this.table}
        WHERE status IN ('pending', 'compensating')`
    )
    return res.rows.map((row) => ({
      sagaId: row.saga_id,
      type: row.type ?? "default",
      status: row.status as SagaSnapshot["status"],
      steps: row.steps,
      executed: row.executed,
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

/**
 * Postgres event store.
 *
 * Schema:
 *   CREATE TABLE nevo_events (
 *     sequence     BIGSERIAL PRIMARY KEY,
 *     id           TEXT NOT NULL UNIQUE,
 *     type         TEXT NOT NULL,
 *     aggregate_id TEXT,
 *     payload      JSONB NOT NULL,
 *     meta         JSONB,
 *     ts           TIMESTAMPTZ NOT NULL DEFAULT NOW()
 *   );
 *   CREATE INDEX nevo_events_aggregate ON nevo_events (aggregate_id, sequence);
 *   CREATE INDEX nevo_events_type      ON nevo_events (type);
 */
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

  async append(input: Omit<DomainEvent, "id" | "sequence" | "ts">): Promise<DomainEvent> {
    const id = uuidv7()
    const res = await this.client.query<{ sequence: string | number; ts: Date }>(
      `INSERT INTO ${this.table} (id, type, aggregate_id, payload, meta)
       VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
       RETURNING sequence, ts`,
      [id, input.type, input.aggregateId ?? null, stringifyWithBigInt(input.payload),
       input.meta ? stringifyWithBigInt(input.meta) : null]
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
    if (range.from !== undefined) { params.push(range.from); filters.push(`sequence >= $${params.length}`) }
    if (range.to !== undefined)   { params.push(range.to);   filters.push(`sequence <= $${params.length}`) }
    if (range.type) { params.push(range.type); filters.push(`type = $${params.length}`) }
    if (range.aggregateId) { params.push(range.aggregateId); filters.push(`aggregate_id = $${params.length}`) }
    const where = filters.length ? `WHERE ${filters.join(" AND ")}` : ""
    let limitClause = ""
    if (range.limit !== undefined) { params.push(range.limit); limitClause = `LIMIT $${params.length}` }
    const sql = `SELECT sequence, id, type, aggregate_id, payload, meta, ts
                   FROM ${this.table} ${where}
                   ORDER BY sequence ASC ${limitClause}`
    const res = await this.client.query<{
      sequence: string | number; id: string; type: string; aggregate_id: string | null;
      payload: unknown; meta: unknown; ts: Date
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
    opts?: { pollIntervalMs?: number }
  ): Promise<{ unsubscribe(): Promise<void> }> {
    let cursor = from
    let stopped = false
    const interval = Math.max(50, opts?.pollIntervalMs ?? 200)
    const tick = async () => {
      if (stopped) return
      try {
        const events = await this.read({ from: cursor })
        for (const e of events) {
          if (stopped) break
          try {
            await handler(e)
          } catch (err) {
            // Do NOT advance the cursor past an event whose handler threw —
            // that would silently drop it. Stop this tick and retry the SAME
            // event on the next one, preserving at-least-once delivery.
            this.logger?.warn(
              { sequence: e.sequence, id: e.id, type: e.type, err: (err as Error)?.message ?? String(err) },
              "event-store.subscribe: handler failed; retrying event next tick (cursor not advanced)"
            )
            break
          }
          cursor = e.sequence + 1
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

/**
 * Postgres DLQ.
 *
 * Schema:
 *   CREATE TABLE nevo_dlq (
 *     id          TEXT PRIMARY KEY,
 *     topic       TEXT NOT NULL,
 *     reason      TEXT NOT NULL,
 *     method      TEXT,
 *     error_code  INT,
 *     ts          TIMESTAMPTZ NOT NULL,
 *     entry       JSONB NOT NULL
 *   );
 *   CREATE INDEX nevo_dlq_topic_ts ON nevo_dlq (topic, ts);
 *   CREATE INDEX nevo_dlq_method   ON nevo_dlq (method);
 */
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
      [id, entry.topic, entry.reason, method ?? null, entry.error?.code ?? null, entry.ts,
       stringifyWithBigInt({ ...entry, id })]
    )
  }

  async list(limit = 100): Promise<DlqEntry[]> {
    const res = await this.client.query<{ entry: DlqEntry }>(
      `SELECT entry FROM ${this.table} ORDER BY ts DESC LIMIT $1`,
      [limit]
    )
    return res.rows.map((r) => deserializeBigInt(r.entry) as DlqEntry)
  }

  async query(q: DlqQuery): Promise<DlqEntry[]> {
    const filters: string[] = []
    const params: unknown[] = []
    if (q.topic) { params.push(q.topic); filters.push(`topic = $${params.length}`) }
    if (q.method) { params.push(q.method); filters.push(`method = $${params.length}`) }
    if (q.reason) { params.push(q.reason); filters.push(`reason = $${params.length}`) }
    if (q.code !== undefined) { params.push(q.code); filters.push(`error_code = $${params.length}`) }
    if (q.since !== undefined) { params.push(q.since); filters.push(`ts >= to_timestamp($${params.length} / 1000.0)`) }
    if (q.until !== undefined) { params.push(q.until); filters.push(`ts <= to_timestamp($${params.length} / 1000.0)`) }
    const where = filters.length ? `WHERE ${filters.join(" AND ")}` : ""
    params.push(q.limit ?? 100)
    const res = await this.client.query<{ entry: DlqEntry }>(
      `SELECT entry FROM ${this.table} ${where} ORDER BY ts DESC LIMIT $${params.length}`,
      params
    )
    return res.rows.map((r) => deserializeBigInt(r.entry) as DlqEntry)
  }

  /**
   * Aggregate DLQ stats in a SINGLE round-trip. A base CTE applies the optional
   * `sinceMs` time window once; the grand total / oldest / newest and each
   * GROUP BY breakdown are computed with FILTER and stitched together with
   * UNION ALL, tagged by a `kind` discriminator. Pass `sinceMs` to restrict the
   * whole computation to rows with `ts >= sinceMs`.
   */
  async stats(sinceMs?: number): Promise<DlqStats> {
    const params: unknown[] = []
    let windowClause = ""
    if (sinceMs !== undefined) {
      params.push(sinceMs)
      windowClause = `WHERE ts >= to_timestamp($${params.length} / 1000.0)`
    }
    const res = await this.client.query<{
      kind: string; key: string | null; count: string; oldest: Date | null; newest: Date | null
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
    const toMs = (v: Date | null): number | undefined =>
      v ? (v instanceof Date ? v.getTime() : Number(v)) : undefined
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
    // Backfill the timezone column on tables created before it existed.
    await this.client.query(`
      ALTER TABLE ${this.table} ADD COLUMN IF NOT EXISTS timezone TEXT;
    `)
    // Covers the claimDue scan for both pending tasks and running tasks whose
    // lease has expired (reclaimed by the reaper after a worker crash).
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_scheduled_due_idx ON ${this.table} (status, run_at)
        WHERE status IN ('pending', 'running');
    `)
    await this.client.query(`
      CREATE INDEX IF NOT EXISTS nevo_scheduled_name_idx ON ${this.table} (name);
    `)
  }

  async enqueue(task: ScheduledTask): Promise<void> {
    // ON CONFLICT (id) DO NOTHING dedups across replicas: cron tasks share a
    // deterministic id, so the first replica to enqueue wins and the job has a
    // single row (one firing per tick cluster-wide).
    await this.client.query(
      `INSERT INTO ${this.table} (id, name, payload, run_at, cron, timezone, attempts, max_attempts, status, created_at)
       VALUES ($1, $2, $3::jsonb, to_timestamp($4 / 1000.0), $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))
       ON CONFLICT (id) DO NOTHING`,
      [task.id, task.name, task.payload === undefined ? null : stringifyWithBigInt(task.payload),
       task.runAt, task.cron ?? null, task.timezone ?? null, task.attempts, task.maxAttempts, task.status, task.createdAt]
    )
  }

  async claimDue(workerId: string, now: number, limit: number, claimTtlMs: number): Promise<ScheduledTask[]> {
    const res = await this.client.query<{
      id: string; name: string; payload: unknown; run_at: Date; cron: string | null;
      timezone: string | null; attempts: number; max_attempts: number; status: string;
      last_error: string | null; created_at: Date
    }>(
      // Claims pending tasks AND reclaims tasks stuck in 'running' past their
      // lease — the worker that held them likely crashed before
      // markCompleted/markFailed. Without this a stuck task (and any cron behind
      // it) is stranded forever. Lease age uses the DB clock (NOW()), the
      // authoritative reference across replicas.
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

  // markCompleted/markFailed/reschedule are fenced by `claimed_by` and
  // `status = 'running'`, exactly like PgOutboxStore.markPublished/markFailed.
  // claimDue's lease reaper can hand a stalled-but-alive worker's task to a
  // second worker; without the fence the original worker, on finally finishing,
  // would double-bump `attempts` or re-reschedule the row the reaper now owns.
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

  async reschedule(id: string, nextRunAt: number, workerId: string): Promise<void> {
    await this.client.query(
      `UPDATE ${this.table}
          SET status = 'pending', run_at = to_timestamp($2 / 1000.0),
              attempts = 0, claimed_at = NULL, claimed_by = NULL
        WHERE id = $1 AND claimed_by = $3 AND status = 'running'`,
      [id, nextRunAt, workerId]
    )
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
      id: string; name: string; payload: unknown; run_at: Date; cron: string | null;
      timezone: string | null; attempts: number; max_attempts: number; status: string;
      last_error: string | null; created_at: Date
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
