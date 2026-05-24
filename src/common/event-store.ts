import { uuidv7 } from "./uuid"

export interface DomainEvent {
  id: string
  type: string
  aggregateId?: string
  payload: unknown
  meta?: Record<string, unknown>
  sequence: number
  ts: number
}

export interface EventStoreReadRange {
  from?: number
  to?: number
  type?: string
  aggregateId?: string
  limit?: number
}

export interface EventStore {
  append(event: Omit<DomainEvent, "id" | "sequence" | "ts">): Promise<DomainEvent>
  read(range?: EventStoreReadRange): Promise<DomainEvent[]>
  subscribe?(from: number, handler: (event: DomainEvent) => Promise<void> | void): Promise<{ unsubscribe(): Promise<void> }>
}

export class InMemoryEventStore implements EventStore {
  private readonly events: DomainEvent[] = []
  private nextSeq = 1
  private readonly subscribers = new Set<{ from: number; handler: (e: DomainEvent) => Promise<void> | void }>()

  async append(input: Omit<DomainEvent, "id" | "sequence" | "ts">): Promise<DomainEvent> {
    const event: DomainEvent = {
      id: uuidv7(),
      type: input.type,
      aggregateId: input.aggregateId,
      payload: input.payload,
      meta: input.meta,
      sequence: this.nextSeq++,
      ts: Date.now()
    }
    this.events.push(event)
    for (const s of this.subscribers) {
      if (event.sequence >= s.from) {
        try { await s.handler(event) } catch {}
      }
    }
    return event
  }

  async read(range: EventStoreReadRange = {}): Promise<DomainEvent[]> {
    const from = range.from ?? 0
    const to = range.to ?? Number.POSITIVE_INFINITY
    let out = this.events.filter((e) => e.sequence >= from && e.sequence <= to)
    if (range.type) out = out.filter((e) => e.type === range.type)
    if (range.aggregateId) out = out.filter((e) => e.aggregateId === range.aggregateId)
    if (range.limit) out = out.slice(0, range.limit)
    return out
  }

  async subscribe(from: number, handler: (e: DomainEvent) => Promise<void> | void): Promise<{ unsubscribe(): Promise<void> }> {
    const entry = { from, handler }
    this.subscribers.add(entry)
    for (const e of this.events) {
      if (e.sequence >= from) {
        try { await handler(e) } catch {}
      }
    }
    return { unsubscribe: async () => { this.subscribers.delete(entry) } }
  }

  size(): number { return this.events.length }
}
