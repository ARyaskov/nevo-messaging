import { createRequire } from "node:module"
import type { OutboxRecord, OutboxStore, OutboxMarkResult } from "./outbox"
import { stringifyWithBigInt, parseWithBigInt } from "./bigint.utils"

const nodeRequire = createRequire(__filename)

type SqliteModule = typeof import("node:sqlite")

let sqliteCache: SqliteModule | null = null

function getSqlite(): SqliteModule {
  if (sqliteCache) return sqliteCache
  try {
    sqliteCache = nodeRequire("node:sqlite") as SqliteModule
    return sqliteCache
  } catch (err: any) {
    throw new Error(
      `node:sqlite is unavailable: ${err?.message ?? err}. Requires Node 23+ with --experimental-sqlite, or Node 24+ where it is stable.`,
      { cause: err }
    )
  }
}

export interface SqliteOutboxStoreOptions {
  path?: string
  tableName?: string
  pragma?: string[]
  /** Share an existing `node:sqlite` `DatabaseSync` so the outbox write can commit in the same transaction as the state change. When set, `path` and `pragma` are ignored. */
  db?: any
}

export class SqliteOutboxStore implements OutboxStore {
  private readonly db: any
  private readonly ownsDb: boolean
  private readonly table: string
  private readonly stmts: {
    save: any
    markPublished: any
    markFailed: any
    listPending: any
  }

  constructor(opts: SqliteOutboxStoreOptions = {}) {
    this.table = opts.tableName ?? "nevo_outbox"

    if (opts.db) {
      this.db = opts.db
      this.ownsDb = false
    } else {
      const sqlite = getSqlite()
      this.db = new sqlite.DatabaseSync(opts.path ?? ":memory:")
      this.ownsDb = true
      for (const p of opts.pragma ?? ["journal_mode = WAL", "synchronous = NORMAL", "busy_timeout = 5000"]) {
        try {
          this.db.exec(`PRAGMA ${p}`)
        } catch {}
      }
    }

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id TEXT PRIMARY KEY,
        service_name TEXT NOT NULL,
        method TEXT NOT NULL,
        params_json TEXT NOT NULL,
        partition_key TEXT,
        created_at INTEGER NOT NULL,
        published_at INTEGER,
        attempts INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending',
        last_error TEXT
      );
      CREATE INDEX IF NOT EXISTS ${this.table}_status_idx ON ${this.table}(status, created_at);
    `)
    // Forward-migrate pre-partition_key tables; duplicate-column error is expected and ignored.
    try {
      this.db.exec(`ALTER TABLE ${this.table} ADD COLUMN partition_key TEXT`)
    } catch {}

    this.stmts = {
      save: this.db.prepare(
        `INSERT OR IGNORE INTO ${this.table} (id, service_name, method, params_json, partition_key, created_at, attempts, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      ),
      markPublished: this.db.prepare(
        `UPDATE ${this.table} SET status = 'published', published_at = ? WHERE id = ? AND status = 'pending' RETURNING status, attempts`
      ),
      markFailed: this.db.prepare(
        `UPDATE ${this.table} SET attempts = attempts + 1, last_error = ?, status = CASE WHEN attempts + 1 >= ? THEN 'failed' ELSE 'pending' END WHERE id = ? AND status = 'pending' RETURNING status, attempts`
      ),
      listPending: this.db.prepare(`
        SELECT o.id, o.service_name, o.method, o.params_json, o.partition_key,
               o.created_at, o.published_at, o.attempts, o.status, o.last_error
          FROM ${this.table} o
         WHERE o.status = 'pending'
           AND (
             o.partition_key IS NULL
             OR NOT EXISTS (
               SELECT 1
                 FROM ${this.table} failed
                WHERE failed.partition_key = o.partition_key
                  AND failed.status = 'failed'
                  AND (
                    failed.created_at < o.created_at
                    OR (failed.created_at = o.created_at AND failed.id < o.id)
                  )
             )
           )
         ORDER BY o.created_at, o.id
         LIMIT ?
      `)
    }
  }

  /** Persist a pending record. Pass `tx` (a `node:sqlite` `DatabaseSync` in a transaction) to enlist the write in your business transaction. */
  async save(record: OutboxRecord, tx?: any): Promise<void> {
    if (tx && tx !== this.db && typeof tx.prepare === "function") {
      tx.prepare(
        `INSERT OR IGNORE INTO ${this.table} (id, service_name, method, params_json, partition_key, created_at, attempts, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      ).run(
        record.id,
        record.serviceName,
        record.method,
        stringifyWithBigInt(record.params),
        record.partitionKey ?? null,
        record.createdAt,
        record.attempts,
        record.status
      )
      return
    }
    this.stmts.save.run(
      record.id,
      record.serviceName,
      record.method,
      stringifyWithBigInt(record.params),
      record.partitionKey ?? null,
      record.createdAt,
      record.attempts,
      record.status
    )
  }

  async markPublished(id: string): Promise<OutboxMarkResult> {
    const row = this.stmts.markPublished.get(Date.now(), id) as { status: string; attempts: number } | undefined
    if (!row) return { owned: false, status: "published", attempts: 0 }
    return { owned: true, status: "published", attempts: row.attempts }
  }

  async markFailed(id: string, error: string, maxAttempts = 5): Promise<OutboxMarkResult> {
    const row = this.stmts.markFailed.get(error, maxAttempts, id) as { status: string; attempts: number } | undefined
    if (!row) return { owned: false, status: "pending", attempts: 0 }
    return { owned: true, status: row.status as OutboxRecord["status"], attempts: row.attempts }
  }

  async listPending(limit: number): Promise<OutboxRecord[]> {
    const rows = this.stmts.listPending.all(limit) as any[]
    return rows.map((r) => ({
      id: r.id,
      serviceName: r.service_name,
      method: r.method,
      params: parseWithBigInt(r.params_json),
      partitionKey: r.partition_key ?? undefined,
      createdAt: r.created_at,
      publishedAt: r.published_at ?? undefined,
      attempts: r.attempts,
      status: r.status,
      lastError: r.last_error ?? undefined
    }))
  }

  close(): void {
    // Never close a borrowed connection.
    if (!this.ownsDb) return
    try {
      this.db.close()
    } catch {}
  }
}
