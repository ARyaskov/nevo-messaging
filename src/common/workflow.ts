import "reflect-metadata"
import { uuidv7 } from "./uuid"
import { getDefaultLogger, type NevoLogger } from "./logger"
import { InMemoryEventStore, type EventStore, type DomainEvent } from "./event-store"
import type { Scheduler } from "./scheduler"

// Workflow engine — Temporal-style durable execution on top of EventStore.
export const WORKFLOW_SUSPEND = Symbol.for("nevo.workflow.suspend")

class WorkflowSuspended extends Error {
  readonly token = WORKFLOW_SUSPEND
  constructor(public readonly reason: string) {
    super(`workflow suspended: ${reason}`)
  }
}

export function isWorkflowSuspended(err: unknown): boolean {
  return err instanceof WorkflowSuspended || (err as { token?: symbol })?.token === WORKFLOW_SUSPEND
}

/** Thrown from `ctx.waitForSignal` when its `timeoutMs` elapses before a signal arrives. */
export class WorkflowSignalTimeout extends Error {
  constructor(public readonly signalName: string) {
    super(`workflow signal "${signalName}" timed out`)
  }
}

export type WorkflowStatus = "running" | "completed" | "failed" | "cancelled" | "suspended"

export interface WorkflowState {
  workflowId: string
  name: string
  status: WorkflowStatus
  input: unknown
  result?: unknown
  error?: string
  startedAt: number
  completedAt?: number
}

export interface WorkflowContext<C = unknown> {
  readonly workflowId: string
  readonly input: C
  step<T>(name: string, fn: () => Promise<T> | T): Promise<T>
  sleep(ms: number): Promise<void>
  waitForSignal<T = unknown>(name: string, opts?: { timeoutMs?: number }): Promise<T>
  now(): number
}

export type WorkflowFn<C = unknown, R = unknown> = (ctx: WorkflowContext<C>) => Promise<R> | R

interface ConsumedSignal {
  name: string
  index: number
}

interface WorkflowReplayState {
  steps: Map<string, unknown>
  sleepDone: Set<string>
  signals: Map<string, unknown[]>
  consumedByOrdinal: Map<number, ConsumedSignal>
  claimed: Set<string>
  signalTimeouts: Set<number>
  nows: Map<number, number>
  input: unknown
  status: WorkflowStatus
}

export interface WorkflowEngineOptions {
  store?: EventStore
  scheduler?: Scheduler
  logger?: NevoLogger
}

interface WorkflowRunResult {
  status: WorkflowStatus
  result?: unknown
  error?: string
}

const WAKEUP_TASK_NAME = "nevo.workflow.wake"

type WakeupPayload =
  | { workflowId: string }
  | { workflowId: string; kind: "sleep"; ordinal: number }
  | { workflowId: string; kind: "signal-timeout"; ordinal: number; name: string }

export class WorkflowEngine {
  private readonly store: EventStore
  private readonly scheduler?: Scheduler
  private readonly logger: NevoLogger
  private readonly workflows = new Map<string, WorkflowFn<any, any>>()

  constructor(opts: WorkflowEngineOptions = {}) {
    this.store = opts.store ?? new InMemoryEventStore()
    this.scheduler = opts.scheduler
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "workflow" })

    if (this.scheduler) {
      this.scheduler.registerHandler(WAKEUP_TASK_NAME, async (payload: WakeupPayload) => {
        await this.onWakeup(payload).catch((err) => {
          this.logger.warn(
            { event: "workflow.wake.failed", workflowId: payload.workflowId, err: (err as Error)?.message }
          )
        })
      })
    }
  }

  register<C = unknown, R = unknown>(name: string, fn: WorkflowFn<C, R>): void {
    this.workflows.set(name, fn as WorkflowFn<any, any>)
  }

  /** Start a new workflow run. Returns the workflowId immediately. */
  async start<C>(name: string, input: C, opts?: { workflowId?: string }): Promise<{ workflowId: string; result?: unknown; status: WorkflowStatus }> {
    if (!this.workflows.has(name)) {
      throw new Error(`WorkflowEngine: workflow "${name}" is not registered`)
    }
    const workflowId = opts?.workflowId ?? uuidv7()
    await this.store.append({
      type: "workflow.started",
      aggregateId: workflowId,
      payload: { name, input }
    })
    const run = await this.execute(workflowId)
    return { workflowId, status: run.status, result: run.result }
  }

  /** Re-enter a workflow that previously suspended. Used by wake-ups and signals. */
  async resume(workflowId: string): Promise<WorkflowRunResult> {
    return this.execute(workflowId)
  }

  /** Send a signal to a suspended workflow; awakens it if waiting on that signal. */
  async signal(workflowId: string, name: string, payload: unknown): Promise<void> {
    await this.store.append({
      type: "workflow.signal.received",
      aggregateId: workflowId,
      payload: { name, value: payload }
    })
    await this.resume(workflowId)
  }

  /** Cancel a workflow. The next call to a suspended ctx.* will throw. */
  async cancel(workflowId: string): Promise<void> {
    await this.store.append({
      type: "workflow.cancelled",
      aggregateId: workflowId,
      payload: {}
    })
  }

  /** Get the current state of a workflow by reading its history. */
  async getState(workflowId: string): Promise<WorkflowState | null> {
    const events = await this.store.read({ aggregateId: workflowId })
    if (events.length === 0) return null
    const start = events.find((e) => e.type === "workflow.started")
    if (!start) return null
    const started = start.payload as { name: string; input: unknown }
    const last = events[events.length - 1]
    const completed = events.find((e) => e.type === "workflow.completed")
    const failed = events.find((e) => e.type === "workflow.failed")
    const cancelled = events.find((e) => e.type === "workflow.cancelled")
    let status: WorkflowStatus = "running"
    if (completed) status = "completed"
    else if (failed) status = "failed"
    else if (cancelled) status = "cancelled"
    else if (events.some((e) => e.type === "workflow.suspended")) status = "suspended"
    return {
      workflowId,
      name: started.name,
      status,
      input: started.input,
      result: completed ? (completed.payload as { result: unknown }).result : undefined,
      error: failed ? (failed.payload as { error: string }).error : undefined,
      startedAt: start.ts,
      completedAt: completed?.ts ?? failed?.ts ?? cancelled?.ts ?? (status === "suspended" ? undefined : last.ts)
    }
  }

  private async onWakeup(payload: WakeupPayload): Promise<void> {
    const kind = (payload as { kind?: string }).kind
    if (kind === "sleep") {
      const p = payload as { workflowId: string; ordinal: number }
      const events = await this.store.read({ aggregateId: p.workflowId })
      const already = events.some(
        (e) => e.type === "workflow.sleep.completed" && (e.payload as { ordinal: number }).ordinal === p.ordinal
      )
      if (!already && !this.isTerminal(events)) {
        await this.store.append({
          type: "workflow.sleep.completed",
          aggregateId: p.workflowId,
          payload: { ordinal: p.ordinal }
        })
      }
    } else if (kind === "signal-timeout") {
      const p = payload as { workflowId: string; ordinal: number; name: string }
      const events = await this.store.read({ aggregateId: p.workflowId })
      const settled = events.some(
        (e) =>
          (e.type === "workflow.signal.consumed" &&
            (e.payload as { ordinal: number }).ordinal === p.ordinal) ||
          (e.type === "workflow.signal.timeout" &&
            (e.payload as { ordinal: number }).ordinal === p.ordinal)
      )
      if (!settled && !this.isTerminal(events)) {
        await this.store.append({
          type: "workflow.signal.timeout",
          aggregateId: p.workflowId,
          payload: { ordinal: p.ordinal, name: p.name }
        })
      }
    }
    await this.resume(payload.workflowId)
  }

  private isTerminal(events: DomainEvent[]): boolean {
    return events.some(
      (e) =>
        e.type === "workflow.completed" ||
        e.type === "workflow.failed" ||
        e.type === "workflow.cancelled"
    )
  }

  private async execute(workflowId: string): Promise<WorkflowRunResult> {
    const events = await this.store.read({ aggregateId: workflowId })
    const replay = this.buildReplayState(events)
    if (!replay) {
      throw new Error(`WorkflowEngine: no workflow.started event for ${workflowId}`)
    }
    if (replay.status === "completed" || replay.status === "failed" || replay.status === "cancelled") {
      const completion = events.find((e) => e.type.startsWith("workflow.") && (
        e.type === "workflow.completed" || e.type === "workflow.failed" || e.type === "workflow.cancelled"
      ))
      const payload = completion?.payload as { result?: unknown; error?: string } | undefined
      return { status: replay.status, result: payload?.result, error: payload?.error }
    }

    const startEvent = events.find((e) => e.type === "workflow.started")
    const startPayload = startEvent!.payload as { name: string; input: unknown }
    const fn = this.workflows.get(startPayload.name)
    if (!fn) {
      const error = `WorkflowEngine: workflow "${startPayload.name}" is not registered`
      await this.store.append({ type: "workflow.failed", aggregateId: workflowId, payload: { error } })
      return { status: "failed", error }
    }

    let sleepOrdinal = 0
    let waitOrdinal = 0
    let nowOrdinal = 0
    const claimed = new Set<string>(replay.claimed)
    const ctx: WorkflowContext = {
      workflowId,
      input: startPayload.input,
      now: (): number => {
        nowOrdinal++
        const key = nowOrdinal
        const recorded = replay.nows.get(key)
        if (recorded !== undefined) return recorded
        const value = Date.now()
        replay.nows.set(key, value)
        void this.store.append({
          type: "workflow.now.recorded",
          aggregateId: workflowId,
          payload: { ordinal: key, value }
        })
        return value
      },
      step: async <T>(name: string, body: () => Promise<T> | T): Promise<T> => {
        if (replay.steps.has(name)) {
          return replay.steps.get(name) as T
        }
        const result = await body()
        await this.store.append({
          type: "workflow.step.completed",
          aggregateId: workflowId,
          payload: { name, result }
        })
        replay.steps.set(name, result)
        return result
      },
      sleep: async (ms: number): Promise<void> => {
        sleepOrdinal++
        const key = `sleep#${sleepOrdinal}`
        if (replay.sleepDone.has(key)) return
        if (!this.scheduler) {
          throw new Error("WorkflowEngine: ctx.sleep requires a Scheduler — pass one to the engine constructor")
        }
        await this.store.append({
          type: "workflow.sleep.started",
          aggregateId: workflowId,
          payload: { ordinal: sleepOrdinal, ms, wakeAt: Date.now() + ms }
        })
        const wake: WakeupPayload = { workflowId, kind: "sleep", ordinal: sleepOrdinal }
        await this.scheduler.enqueueIn(WAKEUP_TASK_NAME, wake, ms)
        throw new WorkflowSuspended(`sleep#${sleepOrdinal}`)
      },
      waitForSignal: async <T>(name: string, opts?: { timeoutMs?: number }): Promise<T> => {
        waitOrdinal++
        const ordinal = waitOrdinal
        const prior = replay.consumedByOrdinal.get(ordinal)
        if (prior) {
          const queue = replay.signals.get(prior.name) ?? []
          return queue[prior.index] as T
        }
        if (replay.signalTimeouts.has(ordinal)) {
          throw new WorkflowSignalTimeout(name)
        }
        const queue = replay.signals.get(name) ?? []
        let index = -1
        for (let i = 0; i < queue.length; i++) {
          if (!claimed.has(`${name}#${i}`)) { index = i; break }
        }
        if (index >= 0) {
          claimed.add(`${name}#${index}`)
          await this.store.append({
            type: "workflow.signal.consumed",
            aggregateId: workflowId,
            payload: { name, index, ordinal }
          })
          return queue[index] as T
        }
        if (opts?.timeoutMs && this.scheduler) {
          const wake: WakeupPayload = { workflowId, kind: "signal-timeout", ordinal, name }
          await this.scheduler.enqueueIn(WAKEUP_TASK_NAME, wake, opts.timeoutMs)
        }
        throw new WorkflowSuspended(`signal:${name}`)
      }
    }

    try {
      const result = await fn(ctx)
      await this.store.append({
        type: "workflow.completed",
        aggregateId: workflowId,
        payload: { result }
      })
      return { status: "completed", result }
    } catch (err) {
      if (isWorkflowSuspended(err)) {
        await this.store.append({
          type: "workflow.suspended",
          aggregateId: workflowId,
          payload: { reason: (err as WorkflowSuspended).reason }
        })
        return { status: "suspended" }
      }
      const error = (err as Error)?.message ?? String(err)
      await this.store.append({
        type: "workflow.failed",
        aggregateId: workflowId,
        payload: { error }
      })
      return { status: "failed", error }
    }
  }

  private buildReplayState(events: DomainEvent[]): WorkflowReplayState | null {
    const start = events.find((e) => e.type === "workflow.started")
    if (!start) return null
    const state: WorkflowReplayState = {
      steps: new Map(),
      sleepDone: new Set(),
      signals: new Map(),
      consumedByOrdinal: new Map(),
      claimed: new Set(),
      signalTimeouts: new Set(),
      nows: new Map(),
      input: (start.payload as { input: unknown }).input,
      status: "running"
    }
    for (const e of events) {
      switch (e.type) {
        case "workflow.step.completed": {
          const p = e.payload as { name: string; result: unknown }
          state.steps.set(p.name, p.result)
          break
        }
        case "workflow.sleep.completed": {
          const p = e.payload as { ordinal: number }
          state.sleepDone.add(`sleep#${p.ordinal}`)
          break
        }
        case "workflow.signal.received": {
          const p = e.payload as { name: string; value: unknown }
          const list = state.signals.get(p.name) ?? []
          list.push(p.value)
          state.signals.set(p.name, list)
          break
        }
        case "workflow.signal.consumed": {
          const p = e.payload as { name: string; index: number; ordinal: number }
          state.consumedByOrdinal.set(p.ordinal, { name: p.name, index: p.index })
          state.claimed.add(`${p.name}#${p.index}`)
          break
        }
        case "workflow.signal.timeout": {
          const p = e.payload as { ordinal: number }
          state.signalTimeouts.add(p.ordinal)
          break
        }
        case "workflow.now.recorded": {
          const p = e.payload as { ordinal: number; value: number }
          state.nows.set(p.ordinal, p.value)
          break
        }
        case "workflow.completed": state.status = "completed"; break
        case "workflow.failed":    state.status = "failed"; break
        case "workflow.cancelled": state.status = "cancelled"; break
        case "workflow.suspended": state.status = "suspended"; break
      }
    }
    return state
  }
}

// ---------------------------------------------------------------------------
// @Workflow decorator + discovery
// ---------------------------------------------------------------------------

export const NEVO_METHOD_WORKFLOW = "nevo:method:workflow"

export interface WorkflowDecoratorOptions {
  /** Logical name. Defaults to `Class#method`. */
  name?: string
}

interface WorkflowMeta extends WorkflowDecoratorOptions {
  propertyKey: string
}

export function Workflow(options: WorkflowDecoratorOptions = {}): MethodDecorator {
  return (target, propertyKey) => {
    const ctor = (target as any)?.constructor ?? target
    const list =
      (Reflect.getMetadata(NEVO_METHOD_WORKFLOW, ctor) as WorkflowMeta[] | undefined) ?? []
    list.push({ ...options, propertyKey: propertyKey as string })
    Reflect.defineMetadata(NEVO_METHOD_WORKFLOW, list, ctor)
  }
}

export function getWorkflowMethods(target: any): WorkflowMeta[] {
  const ctor = target?.constructor ?? target
  return (Reflect.getMetadata(NEVO_METHOD_WORKFLOW, ctor) as WorkflowMeta[] | undefined) ?? []
}

/** Walk through `instances`, find any `@Workflow` methods, register with the engine. */
export function discoverAndRegisterWorkflows(
  engine: WorkflowEngine,
  instances: object[]
): Array<{ name: string }> {
  const out: Array<{ name: string }> = []
  for (const instance of instances) {
    const className = instance.constructor?.name ?? "Unknown"
    for (const meta of getWorkflowMethods(instance)) {
      const workflowName = meta.name ?? `${className}#${meta.propertyKey}`
      const method = (instance as any)[meta.propertyKey]
      if (typeof method !== "function") continue
      engine.register(workflowName, (ctx) => method.call(instance, ctx))
      out.push({ name: workflowName })
    }
  }
  return out
}
