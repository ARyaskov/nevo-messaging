import { setTimeout as sleep } from "node:timers/promises"
import { uuidv7 } from "./uuid"
import type { DlqSink } from "./dlq"
import type { NevoLogger } from "./logger"
import { NEVO_METRIC_NAMES, type MetricsRegistry } from "./metrics"

/** Default saga type used when a saga is created without an explicit one. */
export const DEFAULT_SAGA_TYPE = "default"

export interface SagaStepBackoff {
  baseMs?: number
  maxMs?: number
  jitter?: boolean
}

export interface SagaStep<C = any> {
  name: string
  /**
   * Forward action. Receives an {@link AbortSignal} that fires when the step
   * exceeds `timeoutMs`. IMPORTANT: a timeout only rejects the orchestrator's
   * wait — it cannot stop work already in flight — so the step is retried while
   * the original attempt may still be running. Steps MUST therefore be
   * idempotent, and SHOULD honour `signal` (abort the underlying I/O, or at
   * least track and ignore the late result) so a non-idempotent side effect
   * (e.g. chargeUser) cannot execute twice.
   */
  execute: (ctx: C, signal: AbortSignal) => Promise<unknown> | unknown
  /**
   * Undo action, run in reverse order on failure. `error` is the failure that
   * triggered compensation (may be undefined on a resumed saga). `signal` fires
   * on `compensateTimeoutMs`; the same idempotency rules as {@link execute}
   * apply. If a compensation exhausts its retries the saga is marked
   * `compensation_failed` (never `compensated`) and routed to the DLQ.
   */
  compensate?: (ctx: C, error: unknown, signal: AbortSignal) => Promise<void> | void
  retries?: number
  timeoutMs?: number
  backoff?: SagaStepBackoff
  compensateRetries?: number
  compensateTimeoutMs?: number
  compensateBackoff?: SagaStepBackoff
}

export interface SagaResult {
  status: "success" | "failed"
  error?: unknown
  executed: string[]
  compensated: string[]
  /** Steps whose compensation threw after exhausting retries (needs intervention). */
  compensationFailed?: string[]
  sagaId: string
}

export interface SagaSnapshot<C = any> {
  sagaId: string
  /** Saga type — keys the step registry so a recovered saga finds its definitions. */
  type?: string
  steps: string[]
  executed: string[]
  ctx: C
  status: "pending" | "success" | "failed" | "compensating" | "compensated" | "compensation_failed"
  error?: { message: string }
  updatedAt: number
}

export interface SagaStore {
  save(snapshot: SagaSnapshot): Promise<void>
  load(sagaId: string): Promise<SagaSnapshot | null>
  listPending(): Promise<SagaSnapshot[]>
  delete(sagaId: string): Promise<void>
}

export class InMemorySagaStore implements SagaStore {
  private readonly data = new Map<string, SagaSnapshot>()
  async save(s: SagaSnapshot): Promise<void> { this.data.set(s.sagaId, structuredClone(s)) }
  async load(id: string): Promise<SagaSnapshot | null> { return this.data.get(id) ? structuredClone(this.data.get(id)!) : null }
  async listPending(): Promise<SagaSnapshot[]> {
    return this.data
      .values()
      .filter((s) => s.status === "pending" || s.status === "compensating")
      .toArray()
      .map((s) => structuredClone(s))
  }
  async delete(id: string): Promise<void> { this.data.delete(id) }
}

// A never-aborting signal handed to steps with no timeout configured, so
// `execute`/`compensate` always receive an AbortSignal and can be written
// against a single, uniform contract.
const NEVER_ABORTED: AbortSignal = new AbortController().signal

async function withTimeout<T>(fn: (signal: AbortSignal) => Promise<T>, timeoutMs?: number): Promise<T> {
  if (!timeoutMs || timeoutMs <= 0) return await fn(NEVER_ABORTED)
  // AbortSignal.timeout fires after `timeoutMs`. We reject the outer promise so
  // the orchestrator stops waiting AND hand the signal to `fn`, so a cooperative
  // step can cancel its in-flight work instead of running to completion behind a
  // retry (which would otherwise double a non-idempotent side effect).
  const signal = AbortSignal.timeout(timeoutMs)
  const { promise, resolve, reject } = Promise.withResolvers<T>()
  const onAbort = () => reject(new Error(`Saga step timeout after ${timeoutMs}ms`))
  signal.addEventListener("abort", onAbort, { once: true })
  fn(signal).then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort))
  return promise
}

function computeBackoff(attempt: number, backoff?: SagaStepBackoff): number {
  if (!backoff) return 100 * attempt
  const base = backoff.baseMs ?? 100
  const max = backoff.maxMs ?? 2000
  const exp = Math.min(max, base * Math.pow(2, attempt - 1))
  if (!backoff.jitter) return exp
  return Math.floor(Math.random() * exp)
}

async function runWithRetry<T>(
  attempts: number,
  timeoutMs: number | undefined,
  backoff: SagaStepBackoff | undefined,
  fn: (signal: AbortSignal) => Promise<T>
): Promise<T> {
  let lastErr: unknown
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      return await withTimeout(fn, timeoutMs)
    } catch (err) {
      lastErr = err
      if (attempt < attempts) await sleep(computeBackoff(attempt, backoff))
    }
  }
  throw lastErr
}

/** Options for {@link Saga.resume} / passed through by {@link SagaRecovery}. */
export interface SagaResumeOptions {
  type?: string
  dlq?: DlqSink
  metrics?: MetricsRegistry
  logger?: NevoLogger
}

export class Saga<C = any> {
  private readonly steps: SagaStep<C>[] = []
  private readonly type: string
  private store: SagaStore | null = null
  private sagaId: string | null = null
  private dlq: DlqSink | null = null
  private metrics: MetricsRegistry | null = null
  private logger: NevoLogger | null = null

  constructor(type: string = DEFAULT_SAGA_TYPE) {
    this.type = type
  }

  withStore(store: SagaStore, sagaId?: string): this {
    this.store = store
    this.sagaId = sagaId ?? uuidv7()
    return this
  }

  /** Route compensation failures to a DLQ sink so they are alertable / actionable. */
  withDlq(sink: DlqSink): this {
    this.dlq = sink
    return this
  }

  /** Emit saga metrics (currently: compensation-failure counter). */
  withMetrics(metrics: MetricsRegistry): this {
    this.metrics = metrics
    return this
  }

  withLogger(logger: NevoLogger): this {
    this.logger = logger
    return this
  }

  step(step: SagaStep<C>): this {
    this.steps.push(step)
    return this
  }

  private async persist(ctx: C, executed: string[], status: SagaSnapshot["status"], error?: unknown): Promise<void> {
    if (!this.store || !this.sagaId) return
    await this.store.save({
      sagaId: this.sagaId,
      type: this.type,
      steps: this.steps.map((s) => s.name),
      executed,
      ctx,
      status,
      error: error ? { message: error instanceof Error ? error.message : String(error) } : undefined,
      updatedAt: Date.now()
    })
  }

  async run(ctx: C): Promise<SagaResult> {
    if (!this.sagaId) this.sagaId = uuidv7()
    await this.persist(ctx, [], "pending")
    return await this.forward(ctx, [])
  }

  // Execute steps not yet in `alreadyExecuted`, in order. Shared by run() and
  // resume() so the crash-recovery path gets the same compensation semantics.
  private async forward(ctx: C, alreadyExecuted: string[]): Promise<SagaResult> {
    const sagaId = this.sagaId as string
    const executed = [...alreadyExecuted]
    for (const step of this.steps) {
      if (executed.includes(step.name)) continue
      const retries = (step.retries ?? 0) + 1
      try {
        await runWithRetry(retries, step.timeoutMs, step.backoff, (signal) =>
          Promise.resolve(step.execute(ctx, signal))
        )
        executed.push(step.name)
        await this.persist(ctx, executed, "pending")
      } catch (lastErr) {
        return await this.fail(ctx, executed, lastErr)
      }
    }
    await this.persist(ctx, executed, "success")
    if (this.store) await this.store.delete(sagaId)
    return { status: "success", executed, compensated: [], sagaId }
  }

  // Compensate `executed` in reverse, then settle the saga's terminal status.
  private async fail(ctx: C, executed: string[], err: unknown): Promise<SagaResult> {
    const sagaId = this.sagaId as string
    await this.persist(ctx, executed, "compensating", err)
    const { compensated, failed } = await this.compensate(ctx, executed, err)
    if (failed.length > 0) {
      // A compensation threw after exhausting retries — the saga is NOT clean.
      // Persist a DISTINCT terminal status so it is excluded from listPending()
      // (no blind auto-retry) and stays visible for manual intervention. The
      // failure has already been routed to the DLQ / metric inside compensate().
      await this.persist(ctx, executed, "compensation_failed", err)
      return { status: "failed", error: err, executed, compensated, compensationFailed: failed, sagaId }
    }
    await this.persist(ctx, executed, "compensated", err)
    return { status: "failed", error: err, executed, compensated, sagaId }
  }

  private async compensate(
    ctx: C,
    executed: string[],
    lastErr: unknown
  ): Promise<{ compensated: string[]; failed: string[] }> {
    const compensated: string[] = []
    const failed: string[] = []
    let snapshot: C
    try { snapshot = structuredClone(ctx) } catch { snapshot = ctx }
    for (let i = executed.length - 1; i >= 0; i--) {
      const name = executed[i]
      const original = this.steps.find((s) => s.name === name)
      if (!original?.compensate) continue
      const cAttempts = (original.compensateRetries ?? 0) + 1
      try {
        await runWithRetry(cAttempts, original.compensateTimeoutMs, original.compensateBackoff, (signal) =>
          Promise.resolve(original.compensate!(snapshot, lastErr, signal))
        )
        compensated.push(name)
      } catch (compErr) {
        // Do NOT swallow: a compensation that exhausted its retries means a side
        // effect (e.g. a reserved wallet) may not have been released. Record it,
        // alert via DLQ + metric, and let fail() mark the saga compensation_failed.
        failed.push(name)
        await this.reportCompensationFailure(name, snapshot, lastErr, compErr, cAttempts)
      }
    }
    return { compensated, failed }
  }

  private async reportCompensationFailure(
    step: string,
    ctx: C,
    cause: unknown,
    compErr: unknown,
    attempts: number
  ): Promise<void> {
    const message = compErr instanceof Error ? compErr.message : String(compErr)
    const stack = compErr instanceof Error ? compErr.stack : undefined
    this.metrics?.incCounter(NEVO_METRIC_NAMES.sagaCompensationFailures, { type: this.type, step })
    this.logger?.error(
      { event: "saga.compensation_failed", sagaId: this.sagaId, type: this.type, step, attempts, err: message },
      "Saga compensation failed after exhausting retries; manual intervention required"
    )
    if (!this.dlq) return
    try {
      await this.dlq({
        topic: `saga.${this.type}`,
        reason: "saga_compensation_failed",
        error: { message, stack },
        rawPayload: {
          sagaId: this.sagaId,
          type: this.type,
          step,
          ctx,
          cause: cause instanceof Error ? cause.message : cause
        },
        ts: Date.now(),
        attempts
      })
    } catch (sinkErr) {
      this.logger?.error(
        {
          event: "saga.dlq_failed",
          sagaId: this.sagaId,
          step,
          err: sinkErr instanceof Error ? sinkErr.message : String(sinkErr)
        },
        "Saga DLQ sink threw while recording a compensation failure"
      )
    }
  }

  static async resume<C>(
    store: SagaStore,
    sagaId: string,
    steps: SagaStep<C>[],
    opts: SagaResumeOptions = {}
  ): Promise<SagaResult> {
    const snapshot = await store.load(sagaId)
    if (!snapshot) throw new Error(`Saga ${sagaId} not found`)
    const saga = new Saga<C>(opts.type ?? snapshot.type ?? DEFAULT_SAGA_TYPE)
    saga.steps.push(...steps)
    saga.store = store
    saga.sagaId = sagaId
    if (opts.dlq) saga.dlq = opts.dlq
    if (opts.metrics) saga.metrics = opts.metrics
    if (opts.logger) saga.logger = opts.logger

    const ctx = snapshot.ctx as C
    // Crashed mid-compensation: don't re-run forward steps, just finish undoing.
    if (snapshot.status === "compensating") {
      return await saga.fail(ctx, snapshot.executed, snapshot.error)
    }
    return await saga.forward(ctx, snapshot.executed)
  }
}

/**
 * Registry of step definitions keyed by `(saga type, step name)`. Step
 * functions can't be serialized into a snapshot, so a process that recovers a
 * crashed saga needs them re-registered by name to rebuild the ordered step
 * list. Register every saga type your service runs at startup, then hand the
 * registry to {@link SagaRecovery}.
 */
export class SagaStepRegistry<C = any> {
  private readonly steps = new Map<string, SagaStep<C>>()

  private key(type: string, name: string): string {
    return `${type} ${name}`
  }

  register(type: string, step: SagaStep<C>): this {
    this.steps.set(this.key(type, step.name), step)
    return this
  }

  registerAll(type: string, steps: SagaStep<C>[]): this {
    for (const step of steps) this.register(type, step)
    return this
  }

  get(type: string, name: string): SagaStep<C> | undefined {
    return this.steps.get(this.key(type, name))
  }

  has(type: string, name: string): boolean {
    return this.steps.has(this.key(type, name))
  }

  /**
   * Resolve a snapshot's ordered step names into their registered definitions.
   * Returns null if ANY name is unknown — a saga can't be safely resumed without
   * every step's compensate handler, so the recovery worker skips it.
   */
  resolve(type: string, names: string[]): SagaStep<C>[] | null {
    const out: SagaStep<C>[] = []
    for (const name of names) {
      const step = this.get(type, name)
      if (!step) return null
      out.push(step)
    }
    return out
  }
}

export interface SagaRecoveryOptions {
  /** How often to poll the store for stuck sagas. Default 30s. */
  intervalMs?: number
  dlq?: DlqSink
  metrics?: MetricsRegistry
  logger?: NevoLogger
  /** Called when resuming a specific saga throws (e.g. transient store error). */
  onError?: (err: unknown, snapshot: SagaSnapshot) => void
}

export interface SagaRecoveryResult {
  recovered: number
  skipped: number
  failed: number
}

/**
 * Background worker that drives crash recovery: it periodically calls
 * `store.listPending()` and resumes every `pending` / `compensating` saga via
 * {@link Saga.resume}, looking step definitions up in a {@link SagaStepRegistry}.
 *
 * Without this, a process that dies mid-saga leaves its snapshot stuck forever.
 *
 * NOTE: `listPending()` does not lock rows, so running this in more than one
 * process can resume the same saga concurrently. That is safe ONLY because saga
 * steps are required to be idempotent (see {@link SagaStep.execute}); the late /
 * duplicate attempt must be a no-op.
 */
export class SagaRecovery<C = any> {
  private readonly store: SagaStore
  private readonly registry: SagaStepRegistry<C>
  private readonly intervalMs: number
  private readonly dlq?: DlqSink
  private readonly metrics?: MetricsRegistry
  private readonly logger?: NevoLogger
  private readonly onError?: SagaRecoveryOptions["onError"]
  private timer?: NodeJS.Timeout
  private stopped = false
  private running = false

  constructor(store: SagaStore, registry: SagaStepRegistry<C>, opts: SagaRecoveryOptions = {}) {
    this.store = store
    this.registry = registry
    this.intervalMs = opts.intervalMs ?? 30_000
    this.dlq = opts.dlq
    this.metrics = opts.metrics
    this.logger = opts.logger
    this.onError = opts.onError
  }

  start(): void {
    this.stopped = false
    this.timer = setInterval(() => { void this.recoverOnce() }, this.intervalMs)
    // Don't keep the process alive just for the recovery poll.
    if (typeof this.timer.unref === "function") this.timer.unref()
  }

  stop(): void {
    this.stopped = true
    if (this.timer) clearInterval(this.timer)
    this.timer = undefined
  }

  async recoverOnce(): Promise<SagaRecoveryResult> {
    // Skip if stopped or a previous tick is still draining (ticks don't overlap).
    if (this.stopped || this.running) return { recovered: 0, skipped: 0, failed: 0 }
    this.running = true
    const result: SagaRecoveryResult = { recovered: 0, skipped: 0, failed: 0 }
    try {
      const pending = await this.store.listPending()
      for (const snapshot of pending) {
        const type = snapshot.type ?? DEFAULT_SAGA_TYPE
        const steps = this.registry.resolve(type, snapshot.steps)
        if (!steps) {
          result.skipped++
          this.logger?.warn(
            { event: "saga.recovery_skipped", sagaId: snapshot.sagaId, type, steps: snapshot.steps },
            "Saga recovery skipped: step definitions are not registered for this saga type"
          )
          continue
        }
        try {
          const r = await Saga.resume<C>(this.store, snapshot.sagaId, steps, {
            type,
            dlq: this.dlq,
            metrics: this.metrics,
            logger: this.logger
          })
          result.recovered++
          this.logger?.info(
            {
              event: "saga.recovered",
              sagaId: snapshot.sagaId,
              type,
              status: r.status,
              compensationFailed: r.compensationFailed
            },
            "Saga resumed by recovery worker"
          )
        } catch (err) {
          result.failed++
          this.onError?.(err, snapshot)
          this.logger?.error(
            {
              event: "saga.recovery_failed",
              sagaId: snapshot.sagaId,
              type,
              err: err instanceof Error ? err.message : String(err)
            },
            "Saga recovery failed to resume a pending saga"
          )
        }
      }
    } catch (err) {
      this.logger?.error(
        { event: "saga.recovery_error", err: err instanceof Error ? err.message : String(err) },
        "Saga recovery tick failed to list pending sagas"
      )
    } finally {
      this.running = false
    }
    return result
  }
}

export function createSaga<C = any>(type?: string): Saga<C> {
  return new Saga<C>(type)
}
