import { setTimeout as sleep } from "node:timers/promises"
import { uuidv7 } from "./uuid"

export interface SagaStepBackoff {
  baseMs?: number
  maxMs?: number
  jitter?: boolean
}

export interface SagaStep<C = any> {
  name: string
  execute: (ctx: C) => Promise<unknown> | unknown
  compensate?: (ctx: C, error?: unknown) => Promise<void> | void
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
  sagaId: string
}

export interface SagaSnapshot<C = any> {
  sagaId: string
  steps: string[]
  executed: string[]
  ctx: C
  status: "pending" | "success" | "failed" | "compensating" | "compensated"
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

async function withTimeout<T>(fn: () => Promise<T>, timeoutMs?: number): Promise<T> {
  if (!timeoutMs || timeoutMs <= 0) return await fn()
  const { promise, resolve, reject } = Promise.withResolvers<T>()
  const timer = setTimeout(() => reject(new Error(`Saga step timeout after ${timeoutMs}ms`)), timeoutMs)
  fn().then(resolve, reject).finally(() => clearTimeout(timer))
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
  fn: () => Promise<T>
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

export class Saga<C = any> {
  private readonly steps: SagaStep<C>[] = []
  private store: SagaStore | null = null
  private sagaId: string | null = null

  withStore(store: SagaStore, sagaId?: string): this {
    this.store = store
    this.sagaId = sagaId ?? uuidv7()
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
      steps: this.steps.map((s) => s.name),
      executed,
      ctx,
      status,
      error: error ? { message: error instanceof Error ? error.message : String(error) } : undefined,
      updatedAt: Date.now()
    })
  }

  async run(ctx: C): Promise<SagaResult> {
    const sagaId = this.sagaId ?? uuidv7()
    if (!this.sagaId) this.sagaId = sagaId
    const executed: string[] = []
    await this.persist(ctx, executed, "pending")

    for (const step of this.steps) {
      const retries = (step.retries ?? 0) + 1
      try {
        await runWithRetry(retries, step.timeoutMs, step.backoff, () => Promise.resolve(step.execute(ctx)))
        executed.push(step.name)
        await this.persist(ctx, executed, "pending")
      } catch (lastErr) {
        await this.persist(ctx, executed, "compensating", lastErr)
        const compensated = await this.compensate(ctx, executed, lastErr)
        await this.persist(ctx, executed, "compensated", lastErr)
        return { status: "failed", error: lastErr, executed, compensated, sagaId }
      }
    }

    await this.persist(ctx, executed, "success")
    if (this.store) await this.store.delete(sagaId)
    return { status: "success", executed, compensated: [], sagaId }
  }

  private async compensate(ctx: C, executed: string[], lastErr: unknown): Promise<string[]> {
    const compensated: string[] = []
    let snapshot: C
    try { snapshot = structuredClone(ctx) } catch { snapshot = ctx }
    for (let i = executed.length - 1; i >= 0; i--) {
      const name = executed[i]
      const original = this.steps.find((s) => s.name === name)
      if (!original?.compensate) continue
      const cAttempts = (original.compensateRetries ?? 0) + 1
      try {
        await runWithRetry(cAttempts, original.compensateTimeoutMs, original.compensateBackoff, () =>
          Promise.resolve(original.compensate!(snapshot, lastErr))
        )
        compensated.push(name)
      } catch {
        // compensate failed; continue with rest
      }
    }
    return compensated
  }

  static async resume<C>(store: SagaStore, sagaId: string, steps: SagaStep<C>[]): Promise<SagaResult> {
    const snapshot = await store.load(sagaId)
    if (!snapshot) throw new Error(`Saga ${sagaId} not found`)
    const saga = new Saga<C>()
    saga.steps.push(...steps)
    saga.store = store
    saga.sagaId = sagaId

    const remaining = steps.filter((s) => !snapshot.executed.includes(s.name))
    const ctx = snapshot.ctx as C
    if (snapshot.status === "compensating") {
      const compensated = await saga.compensate(ctx, snapshot.executed, snapshot.error)
      await saga.persist(ctx, snapshot.executed, "compensated", snapshot.error)
      return { status: "failed", error: snapshot.error, executed: snapshot.executed, compensated, sagaId }
    }
    const executed = [...snapshot.executed]
    for (const step of remaining) {
      const retries = (step.retries ?? 0) + 1
      try {
        await runWithRetry(retries, step.timeoutMs, step.backoff, () => Promise.resolve(step.execute(ctx)))
        executed.push(step.name)
        await saga.persist(ctx, executed, "pending")
      } catch (err) {
        await saga.persist(ctx, executed, "compensating", err)
        const compensated = await saga.compensate(ctx, executed, err)
        await saga.persist(ctx, executed, "compensated", err)
        return { status: "failed", error: err, executed, compensated, sagaId }
      }
    }
    await saga.persist(ctx, executed, "success")
    await store.delete(sagaId)
    return { status: "success", executed, compensated: [], sagaId }
  }
}

export function createSaga<C = any>(): Saga<C> {
  return new Saga<C>()
}
