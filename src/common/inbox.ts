import { LruIdempotencyCache } from "./idempotency"

export interface InboxStore {
  hasSeen(uuid: string): Promise<boolean>
  markSeen(uuid: string, result?: unknown): Promise<void>
  getResult(uuid: string): Promise<unknown | undefined>
}

export class InMemoryInboxStore implements InboxStore {
  private readonly cache: LruIdempotencyCache<unknown>
  constructor(opts?: { maxEntries?: number; ttlMs?: number }) {
    this.cache = new LruIdempotencyCache<unknown>({ enabled: true, maxEntries: opts?.maxEntries ?? 50_000, ttlMs: opts?.ttlMs ?? 10 * 60_000 })
  }
  async hasSeen(uuid: string): Promise<boolean> { return this.cache.has(uuid) }
  async markSeen(uuid: string, result?: unknown): Promise<void> { this.cache.set(uuid, result) }
  async getResult(uuid: string): Promise<unknown | undefined> { return this.cache.get(uuid) }
}

export interface InboxOptions {
  enabled?: boolean
  store?: InboxStore
}

export class Inbox {
  private readonly store: InboxStore
  private readonly enabled: boolean

  constructor(opts?: InboxOptions) {
    this.enabled = opts?.enabled !== false
    this.store = opts?.store ?? new InMemoryInboxStore()
  }

  isEnabled(): boolean { return this.enabled }

  async dedupe<T>(uuid: string, handler: () => Promise<T>, opts?: { tx?: (commit: () => Promise<void>) => Promise<void> }): Promise<T> {
    if (!this.enabled) return handler()
    if (await this.store.hasSeen(uuid)) {
      return (await this.store.getResult(uuid)) as T
    }
    const result = await handler()
    if (opts?.tx) {
      await opts.tx(async () => { await this.store.markSeen(uuid, result) })
    } else {
      await this.store.markSeen(uuid, result)
    }
    return result
  }
}
