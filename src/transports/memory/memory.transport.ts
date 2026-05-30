import { uuidv7 } from "../../common/uuid"
import { MessagingError } from "../../common/errors"
import { ErrorCode } from "../../common/error-code"
import { matchesFilter } from "../../common/subscription-filters"
import type {
  MessageMeta,
  Subscription,
  SubscriptionContext,
  SubscriptionOptions
} from "../../common/types"

/**
 * In-memory transport for unit and integration tests.
 * Same `query`/`emit`/`publish`/`subscribe`/`broadcast` surface as the network
 * transports, but with zero IO — no brokers, no Docker.
 */

export type MemoryHandler = (params: any, ctx: { meta: MessageMeta }) => Promise<unknown> | unknown

export type MemorySubscribeHandler<T = unknown> = (
  data: T,
  ctx: SubscriptionContext
) => Promise<void> | void

interface RecordedCall {
  ts: number
  kind: "query" | "emit" | "publish" | "subscribe" | "broadcast"
  serviceName: string
  method: string
  uuid: string
  params: unknown
  meta?: MessageMeta
}

interface RegisteredSubscriber<T = unknown> {
  serviceName: string
  method: string
  handler: MemorySubscribeHandler<T>
  options?: SubscriptionOptions
  active: boolean
}

/** Test injection points consulted by {@link MemoryTransport} before each call. */
export class MemoryHarness {
  readonly calls: RecordedCall[] = []
  private failures = new Map<string, Error>()
  private delays = new Map<string, number>()
  private timeOffset = 0

  /** Inject a single failure for the next call matching `service:method`. */
  failNext(serviceName: string, method: string, err: Error): void {
    this.failures.set(this.k(serviceName, method), err)
  }

  /** Add latency to every call matching `service:method` until cleared. */
  delayBy(serviceName: string, method: string, ms: number): void {
    this.delays.set(this.k(serviceName, method), ms)
  }

  /** Logical clock offset; useful for replay-window / TTL-driven tests. */
  advanceTime(ms: number): void { this.timeOffset += ms }
  now(): number { return Date.now() + this.timeOffset }

  /** Drop all recorded calls and pending injections. */
  reset(): void {
    this.calls.length = 0
    this.failures.clear()
    this.delays.clear()
    this.timeOffset = 0
  }

  /** Take the next failure for `service:method`, or `undefined`. */
  consumeFailure(serviceName: string, method: string): Error | undefined {
    const key = this.k(serviceName, method)
    const err = this.failures.get(key)
    if (err) this.failures.delete(key)
    return err
  }

  getDelay(serviceName: string, method: string): number {
    return this.delays.get(this.k(serviceName, method)) ?? 0
  }

  private k(s: string, m: string): string { return `${s}:${m}` }
}

/** In-process bus. Usually created via {@link createMemoryTransport}. */
export class MemoryTransport {
  private readonly handlers = new Map<string, Map<string, MemoryHandler>>()
  private readonly subscribers = new Set<RegisteredSubscriber>()
  private readonly broadcastListeners = new Set<RegisteredSubscriber>()
  readonly harness: MemoryHarness

  constructor(harness?: MemoryHarness) {
    this.harness = harness ?? new MemoryHarness()
  }

  /** Register a request/response handler for `serviceName.method`. */
  registerHandler(serviceName: string, method: string, handler: MemoryHandler): void {
    let bag = this.handlers.get(serviceName)
    if (!bag) {
      bag = new Map()
      this.handlers.set(serviceName, bag)
    }
    bag.set(method, handler)
  }

  /** Remove a handler. Useful for `afterEach`. */
  unregisterHandler(serviceName: string, method: string): void {
    this.handlers.get(serviceName)?.delete(method)
  }

  /** Wipe all state. Convenience for `beforeEach`. */
  reset(): void {
    this.handlers.clear()
    this.subscribers.clear()
    this.broadcastListeners.clear()
    this.harness.reset()
  }

  async query<T = unknown>(
    callerService: string,
    serviceName: string,
    method: string,
    params: unknown,
    meta?: MessageMeta
  ): Promise<T> {
    const uuid = uuidv7()
    const envMeta: MessageMeta = { ...meta, service: callerService, ts: Date.now() }
    this.harness.calls.push({ ts: Date.now(), kind: "query", serviceName, method, uuid, params, meta: envMeta })

    await this.maybeDelay(serviceName, method)
    const injectedError = this.harness.consumeFailure(serviceName, method)
    if (injectedError) throw injectedError

    const handler = this.handlers.get(serviceName)?.get(method)
    if (!handler) {
      throw new MessagingError(ErrorCode.METHOD_NOT_FOUND, {
        message: `MemoryTransport: no handler registered for ${serviceName}.${method}`
      })
    }
    return handler(params, { meta: envMeta }) as T
  }

  async emit(
    callerService: string,
    serviceName: string,
    method: string,
    params: unknown,
    meta?: MessageMeta
  ): Promise<void> {
    const uuid = uuidv7()
    const envMeta: MessageMeta = { ...meta, service: callerService, ts: Date.now() }
    this.harness.calls.push({ ts: Date.now(), kind: "emit", serviceName, method, uuid, params, meta: envMeta })
    await this.maybeDelay(serviceName, method)
    const injectedError = this.harness.consumeFailure(serviceName, method)
    if (injectedError) throw injectedError
    const handler = this.handlers.get(serviceName)?.get(method)
    if (handler) {
      // Fire and forget — schedule on the macrotask queue so emit() returns
      // before the handler starts. `queueMicrotask` would run the handler
      // inside the caller's current await boundary, defeating the point.
      setImmediate(() => {
        try { Promise.resolve(handler(params, { meta: envMeta })).catch(() => undefined) } catch {}
      })
    }
  }

  async publish(
    callerService: string,
    serviceName: string,
    method: string,
    params: unknown,
    meta?: MessageMeta
  ): Promise<void> {
    const uuid = uuidv7()
    const envMeta: MessageMeta = { ...meta, service: callerService, ts: Date.now() }
    this.harness.calls.push({ ts: Date.now(), kind: "publish", serviceName, method, uuid, params, meta: envMeta })
    await this.fanout(serviceName, method, params, envMeta, uuid)
  }

  async broadcast(callerService: string, method: string, params: unknown, meta?: MessageMeta): Promise<void> {
    const uuid = uuidv7()
    const envMeta: MessageMeta = { ...meta, service: callerService, ts: Date.now() }
    this.harness.calls.push({ ts: Date.now(), kind: "broadcast", serviceName: "*", method, uuid, params, meta: envMeta })
    for (const sub of this.broadcastListeners) {
      if (!sub.active) continue
      await this.deliver(sub, params, envMeta, uuid)
    }
    // Also fan out to method-specific subscribers so broadcasts can be
    // observed identically to publishes during testing.
    for (const sub of this.subscribers) {
      if (!sub.active) continue
      if (sub.method !== method) continue
      await this.deliver(sub, params, envMeta, uuid)
    }
  }

  subscribe<T = unknown>(
    serviceName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: MemorySubscribeHandler<T>
  ): Subscription {
    const entry: RegisteredSubscriber<T> = {
      serviceName,
      method,
      handler,
      options,
      active: true
    }
    this.subscribers.add(entry as RegisteredSubscriber)
    return {
      unsubscribe: async () => {
        entry.active = false
        this.subscribers.delete(entry as RegisteredSubscriber)
      }
    }
  }

  subscribeBroadcast<T = unknown>(handler: MemorySubscribeHandler<T>): Subscription {
    const entry: RegisteredSubscriber<T> = {
      serviceName: "*",
      method: "*",
      handler,
      active: true
    }
    this.broadcastListeners.add(entry as RegisteredSubscriber)
    return {
      unsubscribe: async () => {
        entry.active = false
        this.broadcastListeners.delete(entry as RegisteredSubscriber)
      }
    }
  }

  private async maybeDelay(serviceName: string, method: string): Promise<void> {
    const ms = this.harness.getDelay(serviceName, method)
    if (ms > 0) await new Promise((r) => setTimeout(r, ms))
  }

  private async fanout(
    serviceName: string,
    method: string,
    params: unknown,
    meta: MessageMeta,
    uuid: string
  ): Promise<void> {
    for (const sub of this.subscribers) {
      if (!sub.active) continue
      if (sub.serviceName !== serviceName) continue
      if (sub.method !== method && !this.wildcardMatch(sub.method, method)) continue
      if (sub.options?.filter && !matchesFilter(sub.options.filter, { headers: meta.headers, meta } as any)) continue
      await this.deliver(sub, params, meta, uuid)
    }
  }

  private wildcardMatch(pattern: string, method: string): boolean {
    if (!pattern.includes("*") && !pattern.includes(">")) return false
    const re = new RegExp(
      "^" +
        pattern
          .replaceAll(".", "\\.")
          .replaceAll("*", "[^.]+")
          .replaceAll(">", ".+") +
        "$"
    )
    return re.test(method)
  }

  private async deliver(
    sub: RegisteredSubscriber,
    params: unknown,
    meta: MessageMeta,
    uuid: string
  ): Promise<void> {
    const ctx: SubscriptionContext = {
      meta: { ...meta, type: "sub" } as MessageMeta,
      async ack() {},
      async nack() {}
    }
    try {
      await sub.handler(params, ctx)
    } catch {
      // Subscriber errors don't break the producer in real brokers either.
    }
  }
}

export interface MemoryClientOptions {
  serviceName?: string
  tenantId?: string
}

/** Drop-in replacement for `NatsClientBase`/`KafkaClientBase`/etc. in unit tests. */
export abstract class MemoryClientBase {
  protected readonly transport: MemoryTransport
  protected readonly serviceName: string
  protected readonly tenantId?: string

  protected constructor(transport: MemoryTransport, options?: MemoryClientOptions) {
    this.transport = transport
    this.serviceName = options?.serviceName ?? "memory-test"
    this.tenantId = options?.tenantId
  }

  protected query<T = unknown>(
    serviceName: string,
    method: string,
    params: unknown,
    opts?: { meta?: MessageMeta; tenantId?: string }
  ): Promise<T> {
    return this.transport.query<T>(this.serviceName, serviceName, method, params, {
      ...opts?.meta,
      tenantId: opts?.tenantId ?? this.tenantId
    })
  }

  protected emit(
    serviceName: string,
    method: string,
    params: unknown,
    opts?: { meta?: MessageMeta; tenantId?: string }
  ): Promise<void> {
    return this.transport.emit(this.serviceName, serviceName, method, params, {
      ...opts?.meta,
      tenantId: opts?.tenantId ?? this.tenantId
    })
  }

  protected publish(
    serviceName: string,
    method: string,
    params: unknown,
    opts?: { meta?: MessageMeta }
  ): Promise<void> {
    return this.transport.publish(this.serviceName, serviceName, method, params, opts?.meta)
  }

  protected broadcast(method: string, params: unknown, opts?: { meta?: MessageMeta }): Promise<void> {
    return this.transport.broadcast(this.serviceName, method, params, opts?.meta)
  }

  protected subscribe<T = unknown>(
    serviceName: string,
    method: string,
    options: SubscriptionOptions | undefined,
    handler: MemorySubscribeHandler<T>
  ): Subscription {
    return this.transport.subscribe<T>(serviceName, method, options, handler)
  }
}

export interface CreateMemoryTransportOptions {
  /** Initial map of `(serviceName, method) → handler`, registered eagerly. */
  handlers?: Record<string, Record<string, MemoryHandler>>
}

export function createMemoryTransport(opts: CreateMemoryTransportOptions = {}): MemoryTransport {
  const t = new MemoryTransport()
  if (opts.handlers) {
    for (const [service, methods] of Object.entries(opts.handlers)) {
      for (const [method, handler] of Object.entries(methods)) {
        t.registerHandler(service, method, handler)
      }
    }
  }
  return t
}
