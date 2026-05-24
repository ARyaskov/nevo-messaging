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
}

export interface OutboxStore {
  save(record: OutboxRecord): Promise<void>
  markPublished(id: string): Promise<void>
  markFailed(id: string, error: string): Promise<void>
  listPending(limit: number): Promise<OutboxRecord[]>
}

export class InMemoryOutboxStore implements OutboxStore {
  private readonly records = new Map<string, OutboxRecord>()
  async save(record: OutboxRecord): Promise<void> { this.records.set(record.id, record) }
  async markPublished(id: string): Promise<void> {
    const r = this.records.get(id); if (r) { r.status = "published"; r.publishedAt = Date.now() }
  }
  async markFailed(id: string, error: string): Promise<void> {
    const r = this.records.get(id); if (r) { r.status = "failed"; r.lastError = error; r.attempts++ }
  }
  async listPending(limit: number): Promise<OutboxRecord[]> {
    return this.records.values().filter((r) => r.status === "pending").take(limit).toArray()
  }
}

export interface OutboxPublisher {
  emit(serviceName: string, method: string, params: unknown): Promise<void>
  emitBatch?(items: Array<{ serviceName: string; method: string; params: unknown }>): Promise<void>
}

export class Outbox {
  private timer?: NodeJS.Timeout
  constructor(
    private readonly store: OutboxStore,
    private readonly publisher: OutboxPublisher,
    private readonly opts: { batch?: number; intervalMs?: number; maxAttempts?: number } = {}
  ) {}

  async enqueue(serviceName: string, method: string, params: unknown): Promise<string> {
    const id = uuidv7()
    await this.store.save({
      id,
      serviceName,
      method,
      params,
      createdAt: Date.now(),
      attempts: 0,
      status: "pending"
    })
    return id
  }

  async flushOnce(): Promise<{ published: number; failed: number }> {
    const batch = this.opts.batch ?? 50
    const maxAttempts = this.opts.maxAttempts ?? 5
    const pending = await this.store.listPending(batch)
    let published = 0
    let failed = 0

    if (pending.length > 1 && this.publisher.emitBatch) {
      try {
        await this.publisher.emitBatch(pending.map((r) => ({ serviceName: r.serviceName, method: r.method, params: r.params })))
        for (const rec of pending) await this.store.markPublished(rec.id)
        return { published: pending.length, failed: 0 }
      } catch (err: any) {
        for (const rec of pending) {
          if (rec.attempts + 1 >= maxAttempts) failed++
          await this.store.markFailed(rec.id, err?.message ?? String(err))
        }
        return { published: 0, failed }
      }
    }

    for (const rec of pending) {
      try {
        await this.publisher.emit(rec.serviceName, rec.method, rec.params)
        await this.store.markPublished(rec.id)
        published++
      } catch (err: any) {
        if (rec.attempts + 1 >= maxAttempts) failed++
        await this.store.markFailed(rec.id, err?.message ?? String(err))
      }
    }
    return { published, failed }
  }

  start(): void {
    const intervalMs = this.opts.intervalMs ?? 1000
    this.timer = setInterval(() => { void this.flushOnce() }, intervalMs)
    if (typeof this.timer.unref === "function") this.timer.unref()
  }

  stop(): void { if (this.timer) clearInterval(this.timer) }
}
