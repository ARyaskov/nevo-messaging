import { setTimeout as sleep } from "node:timers/promises"
import { uuidv7 } from "./uuid"
import { MessagingError } from "./errors"
import { ErrorCode } from "./error-code"
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
  /** Forward action; `signal` fires on `timeoutMs`. A timeout doesn't stop in-flight work, so steps MUST be idempotent and SHOULD honour `signal`. */
  execute: (ctx: C, signal: AbortSignal) => Promise<unknown> | unknown
  /** Undo action, run in reverse order on failure; `signal` fires on `compensateTimeoutMs`. Exhausting retries marks the saga `compensation_failed` and routes it to the DLQ. */
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
  /** Steps whose compensation already completed, so a resume never re-runs them. */
  compensated?: string[]
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
  /** Optional recovery lease: single winner per saga until `leaseMs` expires. */
  claim?(sagaId: string, workerId: string, leaseMs: number): Promise<boolean>
}

export class InMemorySagaStore implements SagaStore {
  private readonly data = new Map<string, SagaSnapshot>()
  private readonly claims = new Map<string, { by: string; at: number }>()
  async save(s: SagaSnapshot): Promise<void> {
    this.data.set(s.sagaId, structuredClone(s))
  }
  async load(id: string): Promise<SagaSnapshot | null> {
    return this.data.get(id) ? structuredClone(this.data.get(id)!) : null
  }
  async listPending(): Promise<SagaSnapshot[]> {
    return this.data
      .values()
      .filter((s) => s.status === "pending" || s.status === "compensating")
      .toArray()
      .map((s) => structuredClone(s))
  }
  async delete(id: string): Promise<void> {
    this.data.delete(id)
    this.claims.delete(id)
  }
  async claim(sagaId: string, workerId: string, leaseMs: number): Promise<boolean> {
    const now = Date.now()
    const existing = this.claims.get(sagaId)
    if (existing && existing.by !== workerId && now - existing.at < leaseMs) return false
    this.claims.set(sagaId, { by: workerId, at: now })
    return true
  }
}

// Never-aborting signal handed to steps with no timeout, so they always receive an AbortSignal.
const NEVER_ABORTED: AbortSignal = new AbortController().signal

async function withTimeout<T>(fn: (signal: AbortSignal) => Promise<T>, timeoutMs?: number): Promise<T> {
  if (!timeoutMs || timeoutMs <= 0) return await fn(NEVER_ABORTED)
  const signal = AbortSignal.timeout(timeoutMs)
  const { promise, resolve, reject } = Promise.withResolvers<T>()
  const onAbort = () => reject(new Error(`Saga step timeout after ${timeoutMs}ms`))
  signal.addEventListener("abort", onAbort, { once: true })
  fn(signal)
    .then(resolve, reject)
    .finally(() => signal.removeEventListener("abort", onAbort))
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
  /** Lease identity to renew during the resume (defaults to a fresh one). */
  leaseOwner?: string
  leaseMs?: number
}

export class Saga<C = any> {
  private readonly steps: SagaStep<C>[] = []
  private readonly type: string
  private store: SagaStore | null = null
  private sagaId: string | null = null
  private dlq: DlqSink | null = null
  private metrics: MetricsRegistry | null = null
  private logger: NevoLogger | null = null
  private leaseOwner = `saga-${uuidv7().slice(0, 12)}`
  private leaseMs = 60_000

  constructor(type: string = DEFAULT_SAGA_TYPE) {
    this.type = type
  }

  /** Recovery-lease duration; keep it aligned with SagaRecovery's staleAfterMs. */
  withLease(leaseMs: number): this {
    this.leaseMs = Math.max(1_000, leaseMs)
    return this
  }

  // Renews the store lease while the saga runs so a recovery worker never
  // resumes a saga that is still alive, even during steps longer than the lease.
  private startLeaseHeartbeat(): () => void {
    const store = this.store
    const sagaId = this.sagaId
    if (!store || typeof store.claim !== "function" || !sagaId) return () => {}
    const renew = () => void store.claim!(sagaId, this.leaseOwner, this.leaseMs).catch(() => {})
    renew()
    const timer = setInterval(renew, Math.max(1_000, Math.floor(this.leaseMs / 3)))
    if (typeof timer.unref === "function") timer.unref()
    return () => clearInterval(timer)
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

  private async persist(ctx: C, executed: string[], status: SagaSnapshot["status"], error?: unknown, compensated: string[] = []): Promise<void> {
    if (!this.store || !this.sagaId) return
    await this.store.save({
      sagaId: this.sagaId,
      type: this.type,
      steps: this.steps.map((s) => s.name),
      executed,
      compensated,
      ctx,
      status,
      error: error ? { message: error instanceof Error ? error.message : String(error) } : undefined,
      updatedAt: Date.now()
    })
  }

  async run(ctx: C): Promise<SagaResult> {
    if (!this.sagaId) this.sagaId = uuidv7()
    const stopHeartbeat = this.startLeaseHeartbeat()
    try {
      await this.persist(ctx, [], "pending")
      return await this.forward(ctx, [])
    } finally {
      stopHeartbeat()
    }
  }

  // Execute steps not yet in `alreadyExecuted`, in order. Shared by run() and resume().
  private async forward(ctx: C, alreadyExecuted: string[]): Promise<SagaResult> {
    const sagaId = this.sagaId as string
    const executed = [...alreadyExecuted]
    for (const step of this.steps) {
      if (executed.includes(step.name)) continue
      const retries = (step.retries ?? 0) + 1
      try {
        await runWithRetry(retries, step.timeoutMs, step.backoff, (signal) => Promise.resolve(step.execute(ctx, signal)))
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
  private async fail(ctx: C, executed: string[], err: unknown, alreadyCompensated: string[] = []): Promise<SagaResult> {
    const sagaId = this.sagaId as string
    await this.persist(ctx, executed, "compensating", err, alreadyCompensated)
    const { compensated, failed } = await this.compensate(ctx, executed, err, alreadyCompensated)
    if (failed.length > 0) {
      // Distinct terminal status excludes it from listPending() and keeps it visible for manual intervention.
      await this.persist(ctx, executed, "compensation_failed", err, compensated)
      return { status: "failed", error: err, executed, compensated, compensationFailed: failed, sagaId }
    }
    await this.persist(ctx, executed, "compensated", err, compensated)
    return { status: "failed", error: err, executed, compensated, sagaId }
  }

  private async compensate(
    ctx: C,
    executed: string[],
    lastErr: unknown,
    alreadyCompensated: string[] = []
  ): Promise<{ compensated: string[]; failed: string[] }> {
    const compensated: string[] = [...alreadyCompensated]
    const failed: string[] = []
    let snapshot: C
    try {
      snapshot = structuredClone(ctx)
    } catch {
      snapshot = ctx
    }
    for (let i = executed.length - 1; i >= 0; i--) {
      const name = executed[i]
      if (compensated.includes(name)) continue
      const original = this.steps.find((s) => s.name === name)
      if (!original?.compensate) continue
      const cAttempts = (original.compensateRetries ?? 0) + 1
      try {
        await runWithRetry(cAttempts, original.compensateTimeoutMs, original.compensateBackoff, (signal) =>
          Promise.resolve(original.compensate!(snapshot, lastErr, signal))
        )
        compensated.push(name)
        // Persist progress so a crash mid-compensation never re-runs finished undos.
        try {
          await this.persist(ctx, executed, "compensating", lastErr, compensated)
        } catch {}
      } catch (compErr) {
        failed.push(name)
        await this.reportCompensationFailure(name, snapshot, lastErr, compErr, cAttempts)
      }
    }
    return { compensated, failed }
  }

  private async reportCompensationFailure(step: string, ctx: C, cause: unknown, compErr: unknown, attempts: number): Promise<void> {
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

  static async resume<C>(store: SagaStore, sagaId: string, steps: SagaStep<C>[], opts: SagaResumeOptions = {}): Promise<SagaResult> {
    const snapshot = await store.load(sagaId)
    if (!snapshot) throw new Error(`Saga ${sagaId} not found`)
    const saga = new Saga<C>(opts.type ?? snapshot.type ?? DEFAULT_SAGA_TYPE)
    saga.steps.push(...steps)
    saga.store = store
    saga.sagaId = sagaId
    if (opts.dlq) saga.dlq = opts.dlq
    if (opts.metrics) saga.metrics = opts.metrics
    if (opts.logger) saga.logger = opts.logger
    if (opts.leaseOwner) saga.leaseOwner = opts.leaseOwner
    if (opts.leaseMs) saga.leaseMs = Math.max(1_000, opts.leaseMs)

    const ctx = snapshot.ctx as C
    const stopHeartbeat = saga.startLeaseHeartbeat()
    try {
      // Crashed mid-compensation: don't re-run forward steps, just finish undoing.
      if (snapshot.status === "compensating") {
        return await saga.fail(ctx, snapshot.executed, snapshot.error, snapshot.compensated ?? [])
      }
      if (snapshot.status !== "pending") {
        throw new MessagingError(ErrorCode.BAD_REQUEST, {
          message: `Saga ${sagaId} is terminal (status "${snapshot.status}") and cannot be resumed`,
          sagaId,
          status: snapshot.status
        })
      }
      return await saga.forward(ctx, snapshot.executed)
    } finally {
      stopHeartbeat()
    }
  }
}

/** Registry of step definitions keyed by `(saga type, step name)`, used by {@link SagaRecovery} to rebuild a crashed saga's step list. */
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

  /** Resolve ordered step names into registered definitions; returns null if any name is unknown. */
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
  /** Only resume sagas whose `updatedAt` is at least this old, so live sagas aren't re-run in parallel. Default 2× `intervalMs`. */
  staleAfterMs?: number
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

/** Background worker that polls `store.listPending()` and resumes stuck sagas via {@link Saga.resume}. Running it in multiple processes is safe only because steps are idempotent. */
export class SagaRecovery<C = any> {
  private readonly store: SagaStore
  private readonly registry: SagaStepRegistry<C>
  private readonly intervalMs: number
  private readonly staleAfterMs: number
  private readonly dlq?: DlqSink
  private readonly metrics?: MetricsRegistry
  private readonly logger?: NevoLogger
  private readonly onError?: SagaRecoveryOptions["onError"]
  private readonly workerId = `saga-recovery-${uuidv7().slice(0, 12)}`
  private timer?: NodeJS.Timeout
  private stopped = false
  private running = false

  constructor(store: SagaStore, registry: SagaStepRegistry<C>, opts: SagaRecoveryOptions = {}) {
    this.store = store
    this.registry = registry
    this.intervalMs = opts.intervalMs ?? 30_000
    this.staleAfterMs = opts.staleAfterMs ?? this.intervalMs * 2
    this.dlq = opts.dlq
    this.metrics = opts.metrics
    this.logger = opts.logger
    this.onError = opts.onError
  }

  start(): void {
    this.stopped = false
    // Self-scheduling loop so a pass that outlasts the interval can't overlap the next.
    const loop = async () => {
      if (this.stopped) return
      const startedAt = performance.now()
      try {
        await this.recoverOnce()
      } catch {
        // swallow: a failed pass must not stop the loop
      }
      if (this.stopped) return
      const elapsed = performance.now() - startedAt
      const delay = Math.max(0, this.intervalMs - elapsed)
      this.timer = setTimeout(loop, delay)
      if (typeof this.timer.unref === "function") this.timer.unref()
    }
    this.timer = setTimeout(loop, this.intervalMs)
    if (typeof this.timer.unref === "function") this.timer.unref()
  }

  stop(): void {
    this.stopped = true
    if (this.timer) clearTimeout(this.timer)
    this.timer = undefined
  }

  async recoverOnce(): Promise<SagaRecoveryResult> {
    if (this.stopped || this.running) return { recovered: 0, skipped: 0, failed: 0 }
    this.running = true
    const result: SagaRecoveryResult = { recovered: 0, skipped: 0, failed: 0 }
    try {
      const pending = await this.store.listPending()
      const staleBefore = Date.now() - this.staleAfterMs
      for (const snapshot of pending) {
        if (snapshot.updatedAt > staleBefore) continue
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
        // Lease so concurrent recovery workers never resume the same saga.
        if (typeof this.store.claim === "function") {
          const owned = await this.store.claim(snapshot.sagaId, this.workerId, this.staleAfterMs)
          if (!owned) {
            result.skipped++
            continue
          }
        }
        try {
          const r = await Saga.resume<C>(this.store, snapshot.sagaId, steps, {
            type,
            dlq: this.dlq,
            metrics: this.metrics,
            logger: this.logger,
            leaseOwner: this.workerId,
            leaseMs: this.staleAfterMs
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
