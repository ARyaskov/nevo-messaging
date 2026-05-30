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
  /**
   * Optional ordering key. Records that share a partitionKey are relayed
   * strictly in `createdAt` order, and a partition halts at its first failure,
   * so a later event can never overtake an earlier one of the same aggregate on
   * retry. Records without a partitionKey are independent and relayed together.
   */
  partitionKey?: string
}

/** Outcome of finalizing a claimed record via `markPublished` / `markFailed`. */
export interface OutboxMarkResult {
  /**
   * True if this worker still owned the record and the update applied. False
   * means the claim was stolen (the claim TTL expired and another worker
   * re-claimed) or the row was already finalized — the caller MUST NOT treat
   * the record as handled.
   */
  owned: boolean
  /** The record's status after the update. Only meaningful when `owned`. */
  status: OutboxRecord["status"]
  /** The attempt count after the update. Only meaningful when `owned`. */
  attempts: number
}

export interface OutboxStore {
  /**
   * Persist a pending record.
   *
   * Pass `tx` — a caller-owned transaction/connection handle — to write the
   * outbox row in the SAME transaction as the business state change. That is
   * the entire point of the pattern; see {@link withOutboxTransaction}.
   *
   * Calling `save` (or {@link Outbox.enqueue}) WITHOUT `tx`, on the store's own
   * connection, is UNSAFE: a crash between the business COMMIT and this write
   * loses the event, and writing in the reverse order publishes a phantom event
   * for a business change that never committed.
   */
  save(record: OutboxRecord, tx?: unknown): Promise<void>
  /** Mark a claimed record published. Returns `owned: false` if the claim was stolen. */
  markPublished(id: string): Promise<OutboxMarkResult>
  /**
   * Record a failed attempt. Increments `attempts` and parks the record as
   * `failed` once `attempts >= maxAttempts`, otherwise leaves it `pending` for
   * another try. The resulting status is read back from the store rather than
   * recomputed by the caller. Returns `owned: false` if the claim was stolen.
   */
  markFailed(id: string, error: string, maxAttempts: number): Promise<OutboxMarkResult>
  listPending(limit: number): Promise<OutboxRecord[]>
}

/**
 * Staging buffer used by {@link withOutboxTransaction} for the in-memory store.
 * Records are held until `commit()`; `rollback()` discards them, so an outbox
 * row never survives a rolled-back business transaction.
 */
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
  /**
   * Optional batch fast path. Return a per-item {@link OutboxBatchResult} array
   * (aligned to `items`) to report PARTIAL success — items the broker accepted
   * are marked published and never re-sent. Returning `void` keeps the legacy
   * all-or-nothing contract: resolving means every item was accepted, throwing
   * means none were.
   */
  emitBatch?(items: OutboxEmitItem[]): Promise<OutboxBatchResult[] | void>
}

export class Outbox {
  private timer?: NodeJS.Timeout
  constructor(
    private readonly store: OutboxStore,
    private readonly publisher: OutboxPublisher,
    private readonly opts: { batch?: number; intervalMs?: number; maxAttempts?: number } = {}
  ) {}

  /**
   * Append an event to the outbox.
   *
   * Pass `opts.tx` to enlist the write in your business transaction — the only
   * safe way to use the outbox. Without it the write lands on the store's own
   * connection, decoupled from the business commit, and a crash on either side
   * of the gap loses or fabricates an event. See {@link withOutboxTransaction}.
   */
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

    // Independent records carry no ordering constraint: relay them as one batch
    // and let each succeed or fail on its own.
    if (independent.length > 0) {
      const r = await this.relayIndependent(independent, maxAttempts)
      published += r.published
      failed += r.failed
    }

    // Each ordered partition is relayed in createdAt order and HALTS at its
    // first failure, so a later event never overtakes an earlier one on retry.
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
        // Halt the partition: relaying later records now would let them overtake
        // this one, which must be retried (and delivered) first.
        break
      }
    }
    return { published, failed }
  }

  /**
   * Invoke the publisher's batch path and normalise the result into a per-item
   * array, or `null` when the publisher uses the legacy all-or-nothing contract
   * (resolved void = every item accepted; threw = none accepted).
   */
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
    const intervalMs = this.opts.intervalMs ?? 1000
    this.timer = setInterval(() => { void this.flushOnce() }, intervalMs)
    if (typeof this.timer.unref === "function") this.timer.unref()
  }

  stop(): void { if (this.timer) clearInterval(this.timer) }
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

/**
 * Run `fn` inside a single transaction so the outbox row and the business state
 * commit or roll back together — the core guarantee of the pattern:
 *
 * ```ts
 * await withOutboxTransaction(client, async (tx) => {
 *   await tx.query("INSERT INTO orders (...) VALUES (...)")  // business state
 *   await store.save(outboxRecord, tx)                       // outbox row, same tx
 * })
 * // both committed — or, if anything throws, both rolled back
 * ```
 *
 * `client` may be:
 *  - a SQL connection exposing `query(sql)` (e.g. a pooled Postgres client),
 *  - a `node:sqlite` `DatabaseSync` exposing `exec(sql)`, or
 *  - an {@link InMemoryOutboxStore} (via its `beginTx()`), for tests.
 *
 * The same handle is passed to `fn` as `tx`; forward it to
 * {@link OutboxStore.save}. NEVER enqueue outside such a transaction.
 */
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
