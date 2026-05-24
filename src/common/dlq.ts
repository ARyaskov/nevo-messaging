import type { MessageMeta } from "./types"
import { redactObject } from "./redact"

export const DEFAULT_DLQ_SUFFIX = ".dlq"

export interface DlqEntry {
  id?: string
  topic: string
  reason: string
  error?: { code?: number; message?: string; stack?: string }
  meta?: MessageMeta
  rawPayload?: unknown
  ts: number
  attempts?: number
}

export type DlqSink = (entry: DlqEntry) => Promise<void> | void

export interface DlqRouterOptions {
  enabled?: boolean
  sinks?: DlqSink[]
  redactPaths?: string[]
  replay?: DlqReplayOptions
  store?: DlqStore
}

export interface DlqQuery {
  topic?: string
  method?: string
  reason?: string
  code?: number
  since?: number
  until?: number
  limit?: number
}

export interface DlqStats {
  total: number
  byReason: Record<string, number>
  byCode: Record<string, number>
  byMethod: Record<string, number>
  oldestTs?: number
  newestTs?: number
}

export interface DlqStore {
  push(entry: DlqEntry): Promise<void>
  list(limit?: number): Promise<DlqEntry[]>
  query?(q: DlqQuery): Promise<DlqEntry[]>
  stats?(): Promise<DlqStats>
  remove(id: string): Promise<void>
  clear?(): Promise<void>
}

export class InMemoryDlqStore implements DlqStore {
  private readonly entries: DlqEntry[] = []
  private readonly max: number
  constructor(opts?: { max?: number }) { this.max = opts?.max ?? 1000 }
  async push(entry: DlqEntry): Promise<void> {
    this.entries.push(entry)
    while (this.entries.length > this.max) this.entries.shift()
  }
  async list(limit = 100): Promise<DlqEntry[]> { return this.entries.slice(-limit) }
  async query(q: DlqQuery): Promise<DlqEntry[]> {
    const out: DlqEntry[] = []
    const limit = q.limit ?? 100
    for (let i = this.entries.length - 1; i >= 0 && out.length < limit; i--) {
      const e = this.entries[i]
      if (q.topic && e.topic !== q.topic) continue
      if (q.method && (e.meta as any)?.method !== q.method) continue
      if (q.reason && e.reason !== q.reason) continue
      if (q.code !== undefined && e.error?.code !== q.code) continue
      if (q.since !== undefined && e.ts < q.since) continue
      if (q.until !== undefined && e.ts > q.until) continue
      out.push(e)
    }
    return out
  }
  async stats(): Promise<DlqStats> {
    const byReason: Record<string, number> = {}
    const byCode: Record<string, number> = {}
    const byMethod: Record<string, number> = {}
    let oldestTs: number | undefined
    let newestTs: number | undefined
    for (const e of this.entries) {
      byReason[e.reason] = (byReason[e.reason] ?? 0) + 1
      const code = e.error?.code !== undefined ? String(e.error.code) : "unknown"
      byCode[code] = (byCode[code] ?? 0) + 1
      const m = (e.meta as any)?.method ?? "unknown"
      byMethod[m] = (byMethod[m] ?? 0) + 1
      if (oldestTs === undefined || e.ts < oldestTs) oldestTs = e.ts
      if (newestTs === undefined || e.ts > newestTs) newestTs = e.ts
    }
    return { total: this.entries.length, byReason, byCode, byMethod, oldestTs, newestTs }
  }
  async remove(id: string): Promise<void> {
    const idx = this.entries.findIndex((e) => e.id === id)
    if (idx >= 0) this.entries.splice(idx, 1)
  }
  async clear(): Promise<void> { this.entries.length = 0 }
}

export interface DlqReplayOptions {
  intervalMs?: number
  maxAttempts?: number
  policy?: (entry: DlqEntry) => boolean | Promise<boolean>
  handler?: (entry: DlqEntry) => Promise<boolean>
}

export class DlqRouter {
  private readonly sinks: DlqSink[] = []
  private readonly enabled: boolean
  private readonly redactPaths: string[] | undefined
  private readonly store: DlqStore | null
  private readonly replayOpts?: DlqReplayOptions
  private replayTimer?: NodeJS.Timeout

  constructor(opts?: DlqRouterOptions) {
    this.enabled = opts?.enabled !== false
    if (opts?.sinks) this.sinks.push(...opts.sinks)
    this.redactPaths = opts?.redactPaths
    this.store = opts?.store ?? null
    this.replayOpts = opts?.replay
    if (this.store) {
      this.sinks.push((e) => this.store!.push(e))
    }
  }

  isEnabled(): boolean { return this.enabled }

  addSink(sink: DlqSink): void {
    this.sinks.push(sink)
  }

  private redact(entry: DlqEntry): DlqEntry {
    if (!this.redactPaths || !entry.rawPayload) return entry
    return { ...entry, rawPayload: redactObject(entry.rawPayload, this.redactPaths) }
  }

  async route(entry: DlqEntry): Promise<void> {
    if (!this.enabled) return
    const safe = this.redact(entry)
    for (const sink of this.sinks) {
      try { await sink(safe) } catch (err) {
        console.error("[NevoMessaging][DLQ] sink failed", err)
      }
    }
  }

  startReplay(): void {
    if (!this.store || !this.replayOpts?.handler) return
    const intervalMs = this.replayOpts.intervalMs ?? 30_000
    this.replayTimer = setInterval(() => { void this.replayOnce() }, intervalMs)
    if (typeof this.replayTimer.unref === "function") this.replayTimer.unref()
  }

  stopReplay(): void {
    if (this.replayTimer) clearInterval(this.replayTimer)
    this.replayTimer = undefined
  }

  getStore(): DlqStore | null { return this.store }

  async query(q: DlqQuery = {}): Promise<DlqEntry[]> {
    if (!this.store) return []
    if (this.store.query) return this.store.query(q)
    const list = await this.store.list(q.limit ?? 100)
    return list.filter((e) => {
      if (q.topic && e.topic !== q.topic) return false
      if (q.reason && e.reason !== q.reason) return false
      if (q.code !== undefined && e.error?.code !== q.code) return false
      if (q.since !== undefined && e.ts < q.since) return false
      if (q.until !== undefined && e.ts > q.until) return false
      return true
    })
  }

  async stats(): Promise<DlqStats | null> {
    if (!this.store) return null
    if (this.store.stats) return this.store.stats()
    return null
  }

  async replayOnce(): Promise<{ replayed: number; skipped: number; failed: number }> {
    if (!this.store || !this.replayOpts?.handler) return { replayed: 0, skipped: 0, failed: 0 }
    const policy = this.replayOpts.policy ?? (() => true)
    const handler = this.replayOpts.handler
    const list = await this.store.list(100)
    let replayed = 0, skipped = 0, failed = 0
    for (const e of list) {
      const allow = await policy(e)
      if (!allow) { skipped++; continue }
      try {
        const ok = await handler(e)
        if (ok && e.id) await this.store.remove(e.id)
        if (ok) replayed++; else failed++
      } catch { failed++ }
    }
    return { replayed, skipped, failed }
  }
}
