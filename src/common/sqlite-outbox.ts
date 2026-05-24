import type { OutboxRecord, OutboxStore } from "./outbox"

type SqliteModule = typeof import("node:sqlite")

let sqliteCache: SqliteModule | null = null

function getSqlite(): SqliteModule {
  if (sqliteCache) return sqliteCache
  try {
    sqliteCache = require("node:sqlite") as SqliteModule
    return sqliteCache
  } catch (err: any) {
    throw new Error(`node:sqlite is unavailable: ${err?.message ?? err}. Requires Node 23+ with --experimental-sqlite, or Node 24+ where it is stable.`)
  }
}

export interface SqliteOutboxStoreOptions {
  path?: string
  tableName?: string
  pragma?: string[]
}

export class SqliteOutboxStore implements OutboxStore {
  private readonly db: any
  private readonly table: string
  private readonly stmts: {
    save: any
    markPublished: any
    markFailed: any
    listPending: any
  }

  constructor(opts: SqliteOutboxStoreOptions = {}) {
    const sqlite = getSqlite()
    this.db = new sqlite.DatabaseSync(opts.path ?? ":memory:")
    this.table = opts.tableName ?? "nevo_outbox"

    for (const p of opts.pragma ?? ["journal_mode = WAL", "synchronous = NORMAL", "busy_timeout = 5000"]) {
      try { this.db.exec(`PRAGMA ${p}`) } catch {}
    }

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id TEXT PRIMARY KEY,
        service_name TEXT NOT NULL,
        method TEXT NOT NULL,
        params_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        published_at INTEGER,
        attempts INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending',
        last_error TEXT
      );
      CREATE INDEX IF NOT EXISTS ${this.table}_status_idx ON ${this.table}(status, created_at);
    `)

    this.stmts = {
      save: this.db.prepare(`INSERT OR REPLACE INTO ${this.table} (id, service_name, method, params_json, created_at, attempts, status) VALUES (?, ?, ?, ?, ?, ?, ?)`),
      markPublished: this.db.prepare(`UPDATE ${this.table} SET status = 'published', published_at = ? WHERE id = ?`),
      markFailed: this.db.prepare(`UPDATE ${this.table} SET attempts = attempts + 1, last_error = ?, status = CASE WHEN attempts + 1 >= ? THEN 'failed' ELSE 'pending' END WHERE id = ?`),
      listPending: this.db.prepare(`SELECT id, service_name, method, params_json, created_at, published_at, attempts, status, last_error FROM ${this.table} WHERE status = 'pending' ORDER BY created_at LIMIT ?`)
    }
  }

  async save(record: OutboxRecord): Promise<void> {
    this.stmts.save.run(record.id, record.serviceName, record.method, JSON.stringify(record.params), record.createdAt, record.attempts, record.status)
  }

  async markPublished(id: string): Promise<void> {
    this.stmts.markPublished.run(Date.now(), id)
  }

  async markFailed(id: string, error: string, maxAttempts = 5): Promise<void> {
    this.stmts.markFailed.run(error, maxAttempts, id)
  }

  async listPending(limit: number): Promise<OutboxRecord[]> {
    const rows = this.stmts.listPending.all(limit) as any[]
    return rows.map((r) => ({
      id: r.id,
      serviceName: r.service_name,
      method: r.method,
      params: JSON.parse(r.params_json),
      createdAt: r.created_at,
      publishedAt: r.published_at ?? undefined,
      attempts: r.attempts,
      status: r.status,
      lastError: r.last_error ?? undefined
    }))
  }

  close(): void {
    try { this.db.close() } catch {}
  }
}
