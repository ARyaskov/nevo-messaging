import "reflect-metadata"
import { uuidv7 } from "./uuid"
import { getDefaultLogger, type NevoLogger } from "./logger"
import { nextCronTick, isValidCron, type CronOptions } from "./cron"

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
  // The finalizers take the claiming `workerId` and only mutate a row that is
  // still `running` and still owned by that worker. The lease reaper in
  // `claimDue` lets a second worker re-claim a task whose holder stalled past
  // its lease without crashing; fencing here stops the stalled original from
  // double-bumping `attempts` (premature maxAttempts exhaustion) or
  // double-rescheduling a row the reaper now owns. Mirrors the outbox fence in
  // `PgOutboxStore.markPublished`/`markFailed`.
  markCompleted(id: string, workerId: string): Promise<void>
  markFailed(id: string, error: string, workerId: string): Promise<void>
  reschedule(id: string, nextRunAt: number, workerId: string): Promise<void>
  cancel(id: string): Promise<void>
  list(filter?: { status?: ScheduledTask["status"]; limit?: number }): Promise<ScheduledTask[]>
}

export class InMemoryScheduledTaskStore implements ScheduledTaskStore {
  private readonly map = new Map<string, ScheduledTask>()

  async enqueue(task: ScheduledTask): Promise<void> {
    // Dedup by id, mirroring the PG store's `ON CONFLICT (id) DO NOTHING`: a
    // deterministic cron id means N replicas enqueue the same row but only the
    // first wins, so the job fires once per cluster, not once per replica.
    if (this.map.has(task.id)) return
    this.map.set(task.id, { ...task })
  }

  async claimDue(workerId: string, now: number, limit: number, claimTtlMs: number): Promise<ScheduledTask[]> {
    const claimed: ScheduledTask[] = []
    for (const task of this.map.values()) {
      if (claimed.length >= limit) break
      if (task.runAt > now) continue
      // Claimable when pending, or when a prior claim's lease has expired — the
      // worker that held it likely crashed between claim and completion. Without
      // this reaper a task stuck in "running" is stranded forever (and a stuck
      // cron silently stops recurring).
      const leaseExpired =
        task.status === "running" &&
        task.claimedAt !== undefined &&
        now - task.claimedAt >= claimTtlMs
      if (task.status !== "pending" && !leaseExpired) continue
      task.claimedAt = now
      task.claimedBy = workerId
      task.status = "running"
      claimed.push({ ...task })
    }
    return claimed
  }

  async markCompleted(id: string, workerId: string): Promise<void> {
    const t = this.map.get(id)
    if (!this.owns(t, workerId)) return
    t.status = "completed"
    t.completedAt = Date.now()
  }

  async markFailed(id: string, error: string, workerId: string): Promise<void> {
    const t = this.map.get(id)
    if (!this.owns(t, workerId)) return
    t.attempts++
    t.lastError = error
    t.claimedAt = undefined
    t.claimedBy = undefined
    t.status = t.attempts >= t.maxAttempts ? "failed" : "pending"
  }

  async reschedule(id: string, nextRunAt: number, workerId: string): Promise<void> {
    const t = this.map.get(id)
    if (!this.owns(t, workerId)) return
    t.runAt = nextRunAt
    t.status = "pending"
    t.attempts = 0
    t.claimedAt = undefined
    t.claimedBy = undefined
  }

  // Fence: only the worker that still holds the (running) claim may finalize.
  // A worker reaped after its lease expired (claim stolen, status flipped, or
  // already finalized) sees this return false and its finalizer is a no-op.
  private owns(t: ScheduledTask | undefined, workerId: string): t is ScheduledTask {
    return t !== undefined && t.status === "running" && t.claimedBy === workerId
  }

  async cancel(id: string): Promise<void> {
    const t = this.map.get(id)
    if (!t) return
    t.status = "cancelled"
  }

  async list(filter?: { status?: ScheduledTask["status"]; limit?: number }): Promise<ScheduledTask[]> {
    let out = Array.from(this.map.values())
    if (filter?.status) out = out.filter((t) => t.status === filter.status)
    if (filter?.limit) out = out.slice(0, filter.limit)
    return out.map((t) => ({ ...t }))
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
  }

  registerHandler(name: string, handler: ScheduledHandler): void {
    this.handlers.set(name, handler)
  }

  unregisterHandler(name: string): void {
    this.handlers.delete(name)
  }

  hasHandler(name: string): boolean { return this.handlers.has(name) }

  /** Run `name` at the given epoch ms. */
  async enqueueAt(name: string, payload: unknown, runAt: number | Date): Promise<string> {
    return this.enqueue(name, payload, typeof runAt === "number" ? runAt : runAt.getTime())
  }

  /** Run `name` after `ms` milliseconds. */
  async enqueueIn(name: string, payload: unknown, ms: number): Promise<string> {
    return this.enqueue(name, payload, Date.now() + Math.max(0, ms))
  }

  /**
   * Schedule `name` to recur on a cron expression.
   *
   * The task id is DERIVED from the logical name (`cron:<name>`), so every
   * replica enqueues the same row and the store dedups it — the job fires once
   * per cluster per tick, not once per replica. Re-enqueuing an existing cron
   * is a no-op (first writer wins); use distinct names for distinct schedules.
   *
   * Cron is evaluated in server local time by default; pass `{ utc: true }` or
   * `{ timezone: "Area/City" }` to override. The zone is persisted so each
   * reschedule uses the same rule.
   */
  async enqueueCron(name: string, payload: unknown, cron: string, opts: CronOptions = {}): Promise<string> {
    if (!isValidCron(cron)) throw new Error(`Scheduler: invalid cron "${cron}"`)
    const timezone = opts.utc ? "UTC" : opts.timezone
    const next = nextCronTick(cron, Date.now(), opts)
    return this.enqueue(name, payload, next, { cron, timezone, id: `cron:${name}` })
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
      try { await this.flushOnce() } catch (err) {
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
    let executed = 0
    let failed = 0
    let rescheduled = 0
    for (const task of claimed) {
      const handler = this.handlers.get(task.name)
      if (!handler) {
        await this.store.markFailed(task.id, `No handler registered for "${task.name}"`, this.workerId)
        failed++
        continue
      }
      try {
        await handler(task.payload)
        if (task.cron) {
          const cronOpts = task.timezone ? { timezone: task.timezone } : undefined
          // Compute the next tick from the task's SCHEDULED runAt (not the wall
          // clock at completion) so a slow handler doesn't drift the cadence.
          let next = nextCronTick(task.cron, task.runAt, cronOpts)
          // Missed-run policy: SKIP. If we've fallen behind (worker was down, or
          // the handler ran past one or more ticks), jump to the next tick after
          // now instead of replaying every missed occurrence.
          const now = Date.now()
          if (next > 0 && next <= now) next = nextCronTick(task.cron, now, cronOpts)
          if (next > 0) {
            await this.store.reschedule(task.id, next, this.workerId)
            rescheduled++
          } else {
            await this.store.markCompleted(task.id, this.workerId)
            executed++
          }
        } else {
          await this.store.markCompleted(task.id, this.workerId)
          executed++
        }
      } catch (err) {
        await this.store.markFailed(task.id, (err as Error)?.message ?? String(err), this.workerId)
        failed++
      }
    }
    return { executed, failed, rescheduled }
  }

  private async enqueue(
    name: string,
    payload: unknown,
    runAt: number,
    opts: { cron?: string; timezone?: string; id?: string } = {}
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
      maxAttempts: this.maxAttempts,
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
    const list =
      (Reflect.getMetadata(NEVO_METHOD_SCHEDULED, ctor) as ScheduledMeta[] | undefined) ?? []
    list.push({ ...options, propertyKey: propertyKey as string })
    Reflect.defineMetadata(NEVO_METHOD_SCHEDULED, list, ctor)
  }
}

export function getScheduledMethods(target: any): ScheduledMeta[] {
  const ctor = target?.constructor ?? target
  return (Reflect.getMetadata(NEVO_METHOD_SCHEDULED, ctor) as ScheduledMeta[] | undefined) ?? []
}

/**
 * Walk through `instances`, find any `@Scheduled` methods, register them with
 * `scheduler` and enqueue an initial run.
 *
 * Cron tasks use a DETERMINISTIC id derived from the logical name, so re-running
 * discovery — or running it across N replicas — enqueues each cron exactly once
 * (the store dedups via `ON CONFLICT (id) DO NOTHING`). One-shot `at`/`in` tasks
 * are NOT deduped and enqueue a fresh run on every call.
 */
export async function discoverAndRegisterScheduled(
  scheduler: Scheduler,
  instances: object[]
): Promise<Array<{ name: string; taskId?: string }>> {
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
          utc: meta.utc
        })
      } else if (meta.at !== undefined) {
        taskId = await scheduler.enqueueAt(handlerName, undefined, meta.at)
      } else if (meta.in !== undefined) {
        taskId = await scheduler.enqueueIn(handlerName, undefined, meta.in)
      }
      out.push({ name: handlerName, taskId })
    }
  }
  return out
}
