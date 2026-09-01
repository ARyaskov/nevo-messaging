import "reflect-metadata"
import { uuidv7 } from "./uuid"
import { getDefaultLogger, type NevoLogger } from "./logger"
import { nextCronTick, isValidCron, type CronOptions } from "./cron"
import { mapLimit } from "./concurrency"
import { defineMethodMetadata, readMethodMetadataMap } from "./method-decorators"

export interface ScheduledTask {
  id: string
  name: string
  payload: unknown
  runAt: number
  cron?: string
  /** IANA timezone the cron is evaluated in. Undefined = server local time. */
  timezone?: string
  attempts: number
  maxAttempts: number
  status: "pending" | "running" | "completed" | "failed" | "cancelled"
  lastError?: string
  claimedAt?: number
  claimedBy?: string
  completedAt?: number
  createdAt: number
}

export interface ScheduledTaskStore {
  enqueue(task: ScheduledTask): Promise<void>
  claimDue(workerId: string, now: number, limit: number, claimTtlMs: number): Promise<ScheduledTask[]>
  markCompleted(id: string, workerId: string): Promise<void>
  markFailed(id: string, error: string, workerId: string): Promise<void>
  reschedule(id: string, nextRunAt: number, workerId: string, error?: string): Promise<void>
  cancel(id: string): Promise<void>
  list(filter?: { status?: ScheduledTask["status"]; limit?: number }): Promise<ScheduledTask[]>
  /** Optional heartbeat: refresh the lease of tasks this worker is still executing. */
  extendLease?(ids: string[], workerId: string): Promise<void>
}

/** Binary min-heap over `(runAt, id)`, so a backlog of far-future tasks costs nothing to skip. */
class DueHeap {
  private readonly items: { runAt: number; id: string }[] = []

  get size(): number {
    return this.items.length
  }

  push(runAt: number, id: string): void {
    this.items.push({ runAt, id })
    let i = this.items.length - 1
    while (i > 0) {
      const parent = (i - 1) >> 1
      if (this.items[parent].runAt <= this.items[i].runAt) break
      ;[this.items[parent], this.items[i]] = [this.items[i], this.items[parent]]
      i = parent
    }
  }

  peekRunAt(): number | undefined {
    return this.items[0]?.runAt
  }

  pop(): { runAt: number; id: string } | undefined {
    const top = this.items[0]
    if (top === undefined) return undefined
    const last = this.items.pop()!
    if (this.items.length > 0) {
      this.items[0] = last
      let i = 0
      for (;;) {
        const left = 2 * i + 1
        const right = left + 1
        let smallest = i
        if (left < this.items.length && this.items[left].runAt < this.items[smallest].runAt) smallest = left
        if (right < this.items.length && this.items[right].runAt < this.items[smallest].runAt) smallest = right
        if (smallest === i) break
        ;[this.items[smallest], this.items[i]] = [this.items[i], this.items[smallest]]
        i = smallest
      }
    }
    return top
  }

  clear(): void {
    this.items.length = 0
  }
}

export class InMemoryScheduledTaskStore implements ScheduledTaskStore {
  private readonly map = new Map<string, ScheduledTask>()
  // Entries can be stale; `claimDue` validates each against `map` and drops it.
  private readonly due = new DueHeap()
  private readonly terminal: string[] = []
  private readonly terminalRetentionMs: number
  private readonly maxTerminalTasks: number

  constructor(opts: { terminalRetentionMs?: number; maxTerminalTasks?: number } = {}) {
    this.terminalRetentionMs = Math.max(0, opts.terminalRetentionMs ?? 60 * 60_000)
    this.maxTerminalTasks = Math.max(0, opts.maxTerminalTasks ?? 10_000)
  }

  private markClaimable(task: ScheduledTask): void {
    this.due.push(task.runAt, task.id)
  }

  private markTerminal(task: ScheduledTask): void {
    this.terminal.push(task.id)
  }

  async enqueue(task: ScheduledTask): Promise<void> {
    this.pruneTerminal()
    const existing = this.map.get(task.id)
    // Cron IDs are deterministic. Re-registering the same definition is a
    // no-op, while a changed expression/timezone replaces the old schedule.
    if (existing) {
      if (task.cron && (existing.cron !== task.cron || existing.timezone !== task.timezone || existing.name !== task.name)) {
        const replaced = { ...task, createdAt: existing.createdAt }
        this.map.set(task.id, replaced)
        this.markClaimable(replaced)
      }
      return
    }
    const stored = { ...task }
    this.map.set(task.id, stored)
    if (stored.status === "pending") this.markClaimable(stored)
  }

  async claimDue(workerId: string, now: number, limit: number, claimTtlMs: number): Promise<ScheduledTask[]> {
    this.pruneTerminal(now)
    const claimed: ScheduledTask[] = []
    const requeue: ScheduledTask[] = []

    while (claimed.length < limit) {
      const nextDue = this.due.peekRunAt()
      if (nextDue === undefined || nextDue > now) break
      const entry = this.due.pop()!
      const task = this.map.get(entry.id)
      if (!task || task.runAt !== entry.runAt) continue

      const leaseExpired = task.status === "running" && task.claimedAt !== undefined && now - task.claimedAt >= claimTtlMs
      if (task.status !== "pending" && !leaseExpired) {
        // A live lease may still expire; a terminal task never becomes claimable.
        if (task.status === "running") requeue.push(task)
        continue
      }

      task.claimedAt = now
      task.claimedBy = workerId
      task.status = "running"
      claimed.push({ ...task })
      requeue.push(task)
    }

    for (const task of requeue) this.markClaimable(task)
    return claimed
  }

  async markCompleted(id: string, workerId: string): Promise<void> {
    const t = this.map.get(id)
    if (!this.owns(t, workerId)) return
    t.status = "completed"
    t.completedAt = Date.now()
    this.markTerminal(t)
  }

  async markFailed(id: string, error: string, workerId: string): Promise<void> {
    const t = this.map.get(id)
    if (!this.owns(t, workerId)) return
    t.attempts++
    t.lastError = error
    t.claimedAt = undefined
    t.claimedBy = undefined
    t.status = t.attempts >= t.maxAttempts ? "failed" : "pending"
    if (t.status === "failed") {
      t.completedAt = Date.now()
      this.markTerminal(t)
    } else {
      this.markClaimable(t)
    }
  }

  async reschedule(id: string, nextRunAt: number, workerId: string, error?: string): Promise<void> {
    const t = this.map.get(id)
    if (!this.owns(t, workerId)) return
    t.runAt = nextRunAt
    t.status = "pending"
    t.attempts = 0
    if (error !== undefined) t.lastError = error
    t.claimedAt = undefined
    t.claimedBy = undefined
    t.completedAt = undefined
    this.markClaimable(t)
  }

  // Fence: only the worker that still holds the (running) claim may finalize.
  private owns(t: ScheduledTask | undefined, workerId: string): t is ScheduledTask {
    return t !== undefined && t.status === "running" && t.claimedBy === workerId
  }

  async extendLease(ids: string[], workerId: string): Promise<void> {
    const now = Date.now()
    for (const id of ids) {
      const t = this.map.get(id)
      if (this.owns(t, workerId)) t.claimedAt = now
    }
  }

  async cancel(id: string): Promise<void> {
    const t = this.map.get(id)
    if (!t) return
    t.status = "cancelled"
    t.completedAt = Date.now()
    this.markTerminal(t)
  }

  async list(filter?: { status?: ScheduledTask["status"]; limit?: number }): Promise<ScheduledTask[]> {
    this.pruneTerminal()
    let out = Array.from(this.map.values())
    if (filter?.status) out = out.filter((t) => t.status === filter.status)
    if (filter?.limit) out = out.slice(0, filter.limit)
    return out.map((t) => ({ ...t }))
  }

  /** Walks the terminal list, not the whole map, so cost tracks what actually expired. */
  private pruneTerminal(now = Date.now()): void {
    let live = 0
    for (let i = 0; i < this.terminal.length; i++) {
      const id = this.terminal[i]
      const task = this.map.get(id)
      if (!task || (task.status !== "completed" && task.status !== "failed" && task.status !== "cancelled")) continue
      const terminalAt = task.completedAt ?? task.createdAt
      const expired = now - terminalAt > this.terminalRetentionMs
      if (expired) {
        this.map.delete(id)
        continue
      }
      this.terminal[live++] = id
    }
    this.terminal.length = live

    const overflow = this.terminal.length - this.maxTerminalTasks
    if (overflow <= 0) return
    for (let i = 0; i < overflow; i++) this.map.delete(this.terminal[i])
    this.terminal.splice(0, overflow)
  }

  size(): number {
    return this.map.size
  }

  dueQueueSize(): number {
    return this.due.size
  }
}

export type ScheduledHandler = (payload: any) => Promise<void> | void

export interface SchedulerOptions {
  store?: ScheduledTaskStore
  pollIntervalMs?: number
  batchSize?: number
  claimTtlMs?: number
  maxAttempts?: number
  workerId?: string
  logger?: NevoLogger
  /** Max handlers running at once per flush. Default min(batchSize, 8). */
  concurrency?: number
}

export class Scheduler {
  private readonly store: ScheduledTaskStore
  private readonly handlers = new Map<string, ScheduledHandler>()
  private readonly pollIntervalMs: number
  private readonly batchSize: number
  private readonly claimTtlMs: number
  private readonly maxAttempts: number
  private readonly workerId: string
  private readonly logger: NevoLogger
  private readonly concurrency: number
  private timer?: NodeJS.Timeout
  private stopped = false

  constructor(opts: SchedulerOptions = {}) {
    this.store = opts.store ?? new InMemoryScheduledTaskStore()
    this.pollIntervalMs = Math.max(50, opts.pollIntervalMs ?? 1000)
    this.batchSize = Math.max(1, opts.batchSize ?? 20)
    this.claimTtlMs = Math.max(1000, opts.claimTtlMs ?? 60_000)
    this.maxAttempts = Math.max(1, opts.maxAttempts ?? 5)
    this.workerId = opts.workerId ?? `worker-${uuidv7().slice(0, 12)}`
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "scheduler" })
    this.concurrency = Math.max(1, opts.concurrency ?? Math.min(this.batchSize, 8))
  }

  registerHandler(name: string, handler: ScheduledHandler): void {
    this.handlers.set(name, handler)
  }

  unregisterHandler(name: string): void {
    this.handlers.delete(name)
  }

  hasHandler(name: string): boolean {
    return this.handlers.has(name)
  }

  /** Run `name` at the given epoch ms. */
  async enqueueAt(name: string, payload: unknown, runAt: number | Date, opts: { id?: string; maxAttempts?: number } = {}): Promise<string> {
    return this.enqueue(name, payload, typeof runAt === "number" ? runAt : runAt.getTime(), opts)
  }

  /** Run `name` after `ms` milliseconds. */
  async enqueueIn(name: string, payload: unknown, ms: number, opts: { id?: string; maxAttempts?: number } = {}): Promise<string> {
    return this.enqueue(name, payload, Date.now() + Math.max(0, ms), opts)
  }

  /** Schedule `name` to recur on a cron expression (deduped per cluster by a name-derived id). */
  async enqueueCron(name: string, payload: unknown, cron: string, opts: CronOptions & { id?: string; maxAttempts?: number } = {}): Promise<string> {
    if (!isValidCron(cron)) throw new Error(`Scheduler: invalid cron "${cron}"`)
    const timezone = opts.utc ? "UTC" : opts.timezone
    const next = nextCronTick(cron, Date.now(), opts)
    return this.enqueue(name, payload, next, {
      cron,
      timezone,
      id: opts.id ?? `cron:${name}`,
      maxAttempts: opts.maxAttempts
    })
  }

  /** Cancel a pending task. No-op if it already ran. */
  async cancel(id: string): Promise<void> {
    await this.store.cancel(id)
  }

  /** Inspect tasks. */
  async list(filter?: { status?: ScheduledTask["status"]; limit?: number }): Promise<ScheduledTask[]> {
    return this.store.list(filter)
  }

  start(): void {
    if (this.timer) return
    this.stopped = false
    const tick = async () => {
      if (this.stopped) return
      try {
        await this.flushOnce()
      } catch (err) {
        this.logger.warn({ event: "scheduler.tick.failed", err: (err as Error)?.message })
      }
      if (!this.stopped) {
        this.timer = setTimeout(tick, this.pollIntervalMs)
        if (typeof this.timer.unref === "function") this.timer.unref()
      }
    }
    this.timer = setTimeout(tick, 0)
    if (typeof this.timer.unref === "function") this.timer.unref()
  }

  stop(): void {
    this.stopped = true
    if (this.timer) clearTimeout(this.timer)
    this.timer = undefined
  }

  /** Process every due task once; useful in tests. */
  async flushOnce(): Promise<{ executed: number; failed: number; rescheduled: number }> {
    const now = Date.now()
    const claimed = await this.store.claimDue(this.workerId, now, this.batchSize, this.claimTtlMs)
    const counters = { executed: 0, failed: 0, rescheduled: 0 }
    if (claimed.length === 0) return counters

    // Heartbeat so long handlers keep their lease instead of being reclaimed mid-run.
    const inflightIds = new Set(claimed.map((t) => t.id))
    let heartbeat: NodeJS.Timeout | undefined
    if (typeof this.store.extendLease === "function") {
      heartbeat = setInterval(
        () => {
          void this.store.extendLease!([...inflightIds], this.workerId).catch((err: unknown) => {
            this.logger.warn({ event: "scheduler.lease.extend_failed", err: (err as Error)?.message })
          })
        },
        Math.max(1000, Math.floor(this.claimTtlMs / 2))
      )
      if (typeof heartbeat.unref === "function") heartbeat.unref()
    }

    try {
      await mapLimit(claimed, this.concurrency, async (task) => {
        try {
          await this.runClaimedTask(task, counters)
        } finally {
          inflightIds.delete(task.id)
        }
      })
    } finally {
      if (heartbeat) clearInterval(heartbeat)
    }
    return counters
  }

  private async runClaimedTask(task: ScheduledTask, counters: { executed: number; failed: number; rescheduled: number }): Promise<void> {
    const handler = this.handlers.get(task.name)
    if (!handler) {
      await this.store.markFailed(task.id, `No handler registered for "${task.name}"`, this.workerId)
      counters.failed++
      return
    }
    try {
      await handler(task.payload)
      if (task.cron) {
        const cronOpts = task.timezone ? { timezone: task.timezone } : undefined
        // Next tick from the SCHEDULED runAt so a slow handler doesn't drift the cadence.
        let next = nextCronTick(task.cron, task.runAt, cronOpts)
        // Missed-run policy: SKIP — jump to the next tick after now.
        const now = Date.now()
        if (next > 0 && next <= now) next = nextCronTick(task.cron, now, cronOpts)
        if (next > 0) {
          await this.store.reschedule(task.id, next, this.workerId)
          counters.rescheduled++
        } else {
          await this.store.markCompleted(task.id, this.workerId)
          counters.executed++
        }
      } else {
        await this.store.markCompleted(task.id, this.workerId)
        counters.executed++
      }
    } catch (err) {
      const error = (err as Error)?.message ?? String(err)
      if (task.cron && task.attempts + 1 >= task.maxAttempts) {
        const cronOpts = task.timezone ? { timezone: task.timezone } : undefined
        const next = nextCronTick(task.cron, Date.now(), cronOpts)
        if (next > 0) {
          await this.store.reschedule(task.id, next, this.workerId, error)
          counters.failed++
          counters.rescheduled++
          return
        }
      }
      await this.store.markFailed(task.id, error, this.workerId)
      counters.failed++
    }
  }

  private async enqueue(
    name: string,
    payload: unknown,
    runAt: number,
    opts: { cron?: string; timezone?: string; id?: string; maxAttempts?: number } = {}
  ): Promise<string> {
    const id = opts.id ?? uuidv7()
    const task: ScheduledTask = {
      id,
      name,
      payload,
      runAt,
      cron: opts.cron,
      timezone: opts.timezone,
      attempts: 0,
      maxAttempts: Math.max(1, opts.maxAttempts ?? this.maxAttempts),
      status: "pending",
      createdAt: Date.now()
    }
    await this.store.enqueue(task)
    return id
  }
}

// ---------------------------------------------------------------------------
// @Scheduled decorator + discovery
// ---------------------------------------------------------------------------

export const NEVO_METHOD_SCHEDULED = "nevo:method:scheduled"

export interface ScheduledDecoratorOptions {
  /** Logical name registered with the scheduler. Defaults to `Class#method`. */
  name?: string
  /** Cron expression for repeatable runs. */
  cron?: string
  /** One-shot run at this epoch (ms) or Date. */
  at?: number | Date
  /** One-shot run after this many ms from registration. */
  in?: number
  /** Override scheduler-wide maxAttempts. */
  maxAttempts?: number
  /** IANA timezone for cron evaluation, e.g. "America/New_York". Default: server local time. */
  timezone?: string
  /** Evaluate cron in UTC. Shorthand for `timezone: "UTC"`. */
  utc?: boolean
}

interface ScheduledMeta extends ScheduledDecoratorOptions {
  propertyKey: string
}

export function Scheduled(options: ScheduledDecoratorOptions = {}): MethodDecorator {
  return (target, propertyKey) => {
    const ctor = (target as any)?.constructor ?? target
    defineMethodMetadata(NEVO_METHOD_SCHEDULED, ctor, propertyKey, { ...options, propertyKey: propertyKey as string })
  }
}

export function getScheduledMethods(target: any): ScheduledMeta[] {
  const map = readMethodMetadataMap<ScheduledMeta>(NEVO_METHOD_SCHEDULED, target)
  return map ? [...map.values()] : []
}

/** Find `@Scheduled` methods on `instances`, register them, and enqueue an initial run. */
export async function discoverAndRegisterScheduled(scheduler: Scheduler, instances: object[]): Promise<Array<{ name: string; taskId?: string }>> {
  const out: Array<{ name: string; taskId?: string }> = []
  for (const instance of instances) {
    const className = instance.constructor?.name ?? "Unknown"
    for (const meta of getScheduledMethods(instance)) {
      const handlerName = meta.name ?? `${className}#${meta.propertyKey}`
      const method = (instance as any)[meta.propertyKey]
      if (typeof method !== "function") continue
      scheduler.registerHandler(handlerName, (payload) => method.call(instance, payload))
      let taskId: string | undefined
      if (meta.cron) {
        taskId = await scheduler.enqueueCron(handlerName, undefined, meta.cron, {
          timezone: meta.timezone,
          utc: meta.utc,
          maxAttempts: meta.maxAttempts
        })
      } else if (meta.at !== undefined) {
        taskId = await scheduler.enqueueAt(handlerName, undefined, meta.at, {
          maxAttempts: meta.maxAttempts
        })
      } else if (meta.in !== undefined) {
        taskId = await scheduler.enqueueIn(handlerName, undefined, meta.in, {
          maxAttempts: meta.maxAttempts
        })
      }
      out.push({ name: handlerName, taskId })
    }
  }
  return out
}
