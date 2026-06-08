import { uuidv7 } from "./uuid"

export interface OutboxRecord {
  id: string
  serviceName: string
  method: string
  params: unknown
  createdAt: number
  publishedAt?: number
  attempts: number
  status: "pending" | "published" | "failed"
  lastError?: string
  /** Optional ordering key; records sharing one are relayed in `createdAt` order and halt at the first failure. */
  partitionKey?: string
}

/** Outcome of finalizing a claimed record via `markPublished` / `markFailed`. */
export interface OutboxMarkResult {
  /** True if this worker still owned the record and the update applied; false if the claim was stolen or already finalized. */
  owned: boolean
  /** The record's status after the update. Only meaningful when `owned`. */
  status: OutboxRecord["status"]
  /** The attempt count after the update. Only meaningful when `owned`. */
  attempts: number
}

export interface OutboxStore {
  /** Persist a pending record. Pass `tx` to write it in the same transaction as the business change; see {@link withOutboxTransaction}. */
  save(record: OutboxRecord, tx?: unknown): Promise<void>
  /** Mark a claimed record published. Returns `owned: false` if the claim was stolen. */
  markPublished(id: string): Promise<OutboxMarkResult>
  /** Record a failed attempt; parks as `failed` once `attempts >= maxAttempts`, else leaves `pending`. Returns `owned: false` if the claim was stolen. */
  markFailed(id: string, error: string, maxAttempts: number): Promise<OutboxMarkResult>
  listPending(limit: number): Promise<OutboxRecord[]>
}

/** Staging buffer used by {@link withOutboxTransaction} for the in-memory store; records are held until `commit()`. */
export class InMemoryOutboxTx {
  readonly staged: OutboxRecord[] = []
  constructor(private readonly onCommit: (records: OutboxRecord[]) => void) {}
  stage(record: OutboxRecord): void { this.staged.push(record) }
  commit(): void { this.onCommit(this.staged.splice(0)) }
  rollback(): void { this.staged.length = 0 }
}

export class InMemoryOutboxStore implements OutboxStore {
  private readonly records = new Map<string, OutboxRecord>()

  /** Open a staging transaction for {@link withOutboxTransaction}. */
  beginTx(): InMemoryOutboxTx {
    return new InMemoryOutboxTx((records) => {
      for (const r of records) this.records.set(r.id, r)
    })
  }

  async save(record: OutboxRecord, tx?: unknown): Promise<void> {
    if (tx instanceof InMemoryOutboxTx) { tx.stage(record); return }
    this.records.set(record.id, record)
  }

  async markPublished(id: string): Promise<OutboxMarkResult> {
    const r = this.records.get(id)
    if (!r || r.status !== "pending") return { owned: false, status: r?.status ?? "published", attempts: r?.attempts ?? 0 }
    r.status = "published"
    r.publishedAt = Date.now()
    return { owned: true, status: "published", attempts: r.attempts }
  }

  async markFailed(id: string, error: string, maxAttempts: number): Promise<OutboxMarkResult> {
    const r = this.records.get(id)
    if (!r || r.status !== "pending") return { owned: false, status: r?.status ?? "failed", attempts: r?.attempts ?? 0 }
    r.attempts++
    r.lastError = error
    r.status = r.attempts >= maxAttempts ? "failed" : "pending"
    return { owned: true, status: r.status, attempts: r.attempts }
  }

  async listPending(limit: number): Promise<OutboxRecord[]> {
    return this.records.values().filter((r) => r.status === "pending").take(limit).toArray()
  }
}

/** A single message handed to the publisher. */
export interface OutboxEmitItem {
  serviceName: string
  method: string
  params: unknown
  /** Mirrors {@link OutboxRecord.partitionKey} so brokers can key partitions. */
  partitionKey?: string
}

/** Per-item outcome from {@link OutboxPublisher.emitBatch}, aligned to the input order. */
export interface OutboxBatchResult {
  ok: boolean
  error?: string
}

export interface OutboxPublisher {
  emit(serviceName: string, method: string, params: unknown): Promise<void>
  /** Optional batch fast path. Return a per-item result array for partial success, or `void` for all-or-nothing. */
  emitBatch?(items: OutboxEmitItem[]): Promise<OutboxBatchResult[] | void>
}

export class Outbox {
  private timer?: NodeJS.Timeout
  private stopped = false
  constructor(
    private readonly store: OutboxStore,
    private readonly publisher: OutboxPublisher,
    private readonly opts: { batch?: number; intervalMs?: number; maxAttempts?: number } = {}
  ) {}

  /** Append an event to the outbox. Pass `opts.tx` to enlist the write in your business transaction; see {@link withOutboxTransaction}. */
  async enqueue(
    serviceName: string,
    method: string,
    params: unknown,
    opts: { tx?: unknown; partitionKey?: string } = {}
  ): Promise<string> {
    const id = uuidv7()
    await this.store.save({
      id,
      serviceName,
      method,
      params,
      partitionKey: opts.partitionKey,
      createdAt: Date.now(),
      attempts: 0,
      status: "pending"
    }, opts.tx)
    return id
  }

  async flushOnce(): Promise<{ published: number; failed: number }> {
    const batch = this.opts.batch ?? 50
    const maxAttempts = this.opts.maxAttempts ?? 5
    const pending = await this.store.listPending(batch)
    if (pending.length === 0) return { published: 0, failed: 0 }

    const { ordered, independent } = partitionRecords(pending)
    let published = 0
    let failed = 0

    if (independent.length > 0) {
      const r = await this.relayIndependent(independent, maxAttempts)
      published += r.published
      failed += r.failed
    }

    for (const part of ordered) {
      const r = await this.relayOrdered(part, maxAttempts)
      published += r.published
      failed += r.failed
    }

    return { published, failed }
  }

  private async relayIndependent(records: OutboxRecord[], maxAttempts: number): Promise<{ published: number; failed: number }> {
    let published = 0
    let failed = 0

    if (records.length > 1 && this.publisher.emitBatch) {
      const results = await this.callEmitBatch(records)
      for (let i = 0; i < records.length; i++) {
        const res = results ? (results[i] ?? { ok: false, error: "emitBatch returned no result for this item" }) : { ok: true }
        if (res.ok) {
          if ((await this.store.markPublished(records[i].id)).owned) published++
        } else {
          const mark = await this.store.markFailed(records[i].id, res.error ?? "emitBatch reported failure", maxAttempts)
          if (mark.owned && mark.status === "failed") failed++
        }
      }
      return { published, failed }
    }

    for (const rec of records) {
      try {
        await this.publisher.emit(rec.serviceName, rec.method, rec.params)
        if ((await this.store.markPublished(rec.id)).owned) published++
      } catch (err: any) {
        const mark = await this.store.markFailed(rec.id, err?.message ?? String(err), maxAttempts)
        if (mark.owned && mark.status === "failed") failed++
      }
    }
    return { published, failed }
  }

  private async relayOrdered(records: OutboxRecord[], maxAttempts: number): Promise<{ published: number; failed: number }> {
    let published = 0
    let failed = 0
    for (const rec of records) {
      try {
        await this.publisher.emit(rec.serviceName, rec.method, rec.params)
        if ((await this.store.markPublished(rec.id)).owned) published++
      } catch (err: any) {
        const mark = await this.store.markFailed(rec.id, err?.message ?? String(err), maxAttempts)
        if (mark.owned && mark.status === "failed") failed++
        // Halt the partition so later records can't overtake this one on retry.
        break
      }
    }
    return { published, failed }
  }

  private async callEmitBatch(records: OutboxRecord[]): Promise<OutboxBatchResult[] | null> {
    const items: OutboxEmitItem[] = records.map((r) => ({
      serviceName: r.serviceName,
      method: r.method,
      params: r.params,
      partitionKey: r.partitionKey
    }))
    try {
      const out = await this.publisher.emitBatch!(items)
      return Array.isArray(out) ? out : null
    } catch (err: any) {
      const error = err?.message ?? String(err)
      return records.map(() => ({ ok: false, error }))
    }
  }

  start(): void {
    this.stopped = false
    const intervalMs = this.opts.intervalMs ?? 1000
    // Self-scheduling loop so a flush that outlasts the interval can't overlap the next.
    const loop = async () => {
      if (this.stopped) return
      const startedAt = performance.now()
      try {
        await this.flushOnce()
      } catch {
        // swallow: a failed flush must not stop the loop
      }
      if (this.stopped) return
      const elapsed = performance.now() - startedAt
      const delay = Math.max(0, intervalMs - elapsed)
      this.timer = setTimeout(loop, delay)
      if (typeof this.timer.unref === "function") this.timer.unref()
    }
    this.timer = setTimeout(loop, intervalMs)
    if (typeof this.timer.unref === "function") this.timer.unref()
  }

  stop(): void {
    this.stopped = true
    if (this.timer) clearTimeout(this.timer)
    this.timer = undefined
  }
}

/** Split a pending batch into ordered partitions (keyed) and independent records. */
function partitionRecords(records: OutboxRecord[]): { ordered: OutboxRecord[][]; independent: OutboxRecord[] } {
  const byKey = new Map<string, OutboxRecord[]>()
  const independent: OutboxRecord[] = []
  for (const r of records) {
    if (r.partitionKey === undefined || r.partitionKey === null) {
      independent.push(r)
    } else {
      let arr = byKey.get(r.partitionKey)
      if (!arr) { arr = []; byKey.set(r.partitionKey, arr) }
      arr.push(r)
    }
  }
  return { ordered: [...byKey.values()], independent }
}

/** Run `fn` inside a single transaction so the outbox row and business state commit or roll back together. `client` may expose `query(sql)`, `exec(sql)`, or `beginTx()`. */
export async function withOutboxTransaction<T>(
  client: unknown,
  fn: (tx: unknown) => Promise<T> | T
): Promise<T> {
  const c = client as {
    query?: (sql: string) => Promise<unknown>
    exec?: (sql: string) => unknown
    beginTx?: () => { commit(): void | Promise<void>; rollback(): void | Promise<void> }
  }

  if (typeof c.query === "function") {
    await c.query("BEGIN")
    try {
      const result = await fn(client)
      await c.query("COMMIT")
      return result
    } catch (err) {
      try { await c.query("ROLLBACK") } catch {}
      throw err
    }
  }

  if (typeof c.exec === "function") {
    c.exec("BEGIN")
    try {
      const result = await fn(client)
      c.exec("COMMIT")
      return result
    } catch (err) {
      try { c.exec("ROLLBACK") } catch {}
      throw err
    }
  }

  if (typeof c.beginTx === "function") {
    const tx = c.beginTx()
    try {
      const result = await fn(tx)
      await tx.commit()
      return result
    } catch (err) {
      await tx.rollback()
      throw err
    }
  }

  throw new Error(
    "withOutboxTransaction: `client` must expose query(sql) (SQL connection), exec(sql) (node:sqlite), or beginTx() (InMemoryOutboxStore)"
  )
}
