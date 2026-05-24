import { randomUUID } from "node:crypto"

type Listener = (event: DevToolsEvent) => void

export type DevToolsEventType = "request" | "response" | "error" | "circuit" | "discovery" | "rate-limit" | "custom"

export interface DevToolsEvent {
  ts: number
  type: DevToolsEventType
  service?: string
  method?: string
  uuid?: string
  /**
   * Correlation id shared across every envelope in one logical fan-out (the
   * initiating call plus everything it calls, transitively). Surfaced in the
   * DevTools /traces view to reconstruct chains. Populated by the framework
   * via `chain-context.ts`; see `MessageMeta.nevoChainId`.
   */
  chainId?: string
  /**
   * Envelope uuid of the immediately-causing message. Lets the dashboard render
   * a tree instead of a flat list. Mirrors `MessageMeta.nevoParentUuid`.
   */
  parentUuid?: string
  durationMs?: number
  status?: "ok" | "error"
  error?: { code?: number; message?: string }
  origin?: string
  extra?: Record<string, unknown>
}

export type DevToolsDropStrategy = "drop-oldest" | "drop-newest" | "back-pressure"

export interface DevToolsRingOptions {
  maxEvents?: number
  originId?: string
  batchFlushMs?: number
  dropStrategy?: DevToolsDropStrategy
  onBackpressure?: (depth: number) => void
}

export class DevToolsBus {
  private readonly listeners: Set<Listener> = new Set()
  private readonly buffer: (DevToolsEvent | undefined)[]
  private writeIdx = 0
  private count = 0
  private readonly capacity: number
  public readonly originId: string

  private readonly batched: boolean
  private readonly pendingEmissions: DevToolsEvent[] = []
  private flushScheduled = false
  private flushTimer?: NodeJS.Immediate
  private readonly dropStrategy: DevToolsDropStrategy
  private readonly onBackpressure?: (depth: number) => void

  constructor(opts?: DevToolsRingOptions) {
    this.capacity = opts?.maxEvents ?? 5000
    this.buffer = new Array<DevToolsEvent | undefined>(this.capacity)
    this.originId = opts?.originId ?? randomUUID()
    this.batched = (opts?.batchFlushMs ?? 0) > 0
    this.dropStrategy = opts?.dropStrategy ?? "drop-oldest"
    this.onBackpressure = opts?.onBackpressure
  }

  private emitToListeners(event: DevToolsEvent): void {
    if (this.listeners.size === 0) return
    for (const cb of this.listeners) {
      try { cb(event) } catch {}
    }
  }

  private storeInRing(event: DevToolsEvent): boolean {
    if (this.count >= this.capacity) {
      if (this.dropStrategy === "drop-newest") return false
      if (this.dropStrategy === "back-pressure") {
        this.onBackpressure?.(this.count)
        return false
      }
    }
    this.buffer[this.writeIdx] = event
    this.writeIdx = (this.writeIdx + 1) % this.capacity
    if (this.count < this.capacity) this.count++
    return true
  }

  private scheduleFlush(): void {
    if (this.flushScheduled) return
    this.flushScheduled = true
    this.flushTimer = setImmediate(() => this.flushPending())
  }

  private flushPending(): void {
    this.flushScheduled = false
    this.flushTimer = undefined
    if (this.pendingEmissions.length === 0) return
    const batch = this.pendingEmissions.splice(0, this.pendingEmissions.length)
    for (const event of batch) this.emitToListeners(event)
  }

  publish(event: DevToolsEvent): void {
    const stamped: DevToolsEvent = event.origin ? event : { ...event, origin: this.originId }
    this.storeInRing(stamped)
    if (this.listeners.size === 0) return
    if (this.batched) {
      this.pendingEmissions.push(stamped)
      this.scheduleFlush()
    } else {
      this.emitToListeners(stamped)
    }
  }

  ingestRemote(event: DevToolsEvent): void {
    if (event.origin && event.origin === this.originId) return
    this.storeInRing(event)
    if (this.listeners.size === 0) return
    if (this.batched) {
      this.pendingEmissions.push(event)
      this.scheduleFlush()
    } else {
      this.emitToListeners(event)
    }
  }

  recent(limit = 200): DevToolsEvent[] {
    const n = Math.min(limit, this.count)
    if (n === 0) return []
    const out: DevToolsEvent[] = new Array(n)
    let idx = (this.writeIdx - n + this.capacity) % this.capacity
    for (let i = 0; i < n; i++) {
      out[i] = this.buffer[idx]!
      idx = (idx + 1) % this.capacity
    }
    return out
  }

  size(): number { return this.count }
  capacityHint(): number { return this.capacity }

  on(handler: Listener): () => void {
    this.listeners.add(handler)
    return () => { this.listeners.delete(handler) }
  }

  onLocal(handler: Listener): () => void {
    const origin = this.originId
    const wrapped: Listener = (event) => { if (event.origin === origin) handler(event) }
    this.listeners.add(wrapped)
    return () => { this.listeners.delete(wrapped) }
  }

  private readonly weakRegistry = new FinalizationRegistry<Listener>((wrapped) => {
    this.listeners.delete(wrapped)
  })

  onWeak(holder: object, handler: Listener): () => void {
    const wrapped: Listener = (event) => {
      try { handler(event) } catch {}
    }
    this.listeners.add(wrapped)
    this.weakRegistry.register(holder, wrapped, wrapped)
    return () => {
      this.listeners.delete(wrapped)
      this.weakRegistry.unregister(wrapped)
    }
  }

  drain(): void {
    if (this.flushTimer) {
      clearImmediate(this.flushTimer)
      this.flushTimer = undefined
    }
    this.flushPending()
  }
}

let globalBus: DevToolsBus | null = null

export function getDevToolsBus(): DevToolsBus {
  if (!globalBus) globalBus = new DevToolsBus()
  return globalBus
}

export function setDevToolsBus(bus: DevToolsBus): void {
  globalBus = bus
}

export interface DevToolsAdapter {
  attach(): Promise<() => Promise<void>>
}

export function publishClientEvent(
  bus: DevToolsBus | null | undefined,
  payload: {
    service: string
    method: string
    uuid?: string
    chainId?: string
    parentUuid?: string
    durationMs: number
    status: "ok" | "error"
    error?: { code?: number; message?: string }
    transport?: string
    origin?: string
  }
): void {
  if (!bus) return
  try {
    bus.publish({
      ts: Date.now(),
      type: payload.status === "ok" ? "request" : "error",
      service: payload.service,
      method: payload.method,
      uuid: payload.uuid,
      chainId: payload.chainId,
      parentUuid: payload.parentUuid,
      durationMs: payload.durationMs,
      status: payload.status,
      error: payload.error,
      extra: { role: "client", transport: payload.transport, origin: payload.origin }
    })
  } catch {}
}
