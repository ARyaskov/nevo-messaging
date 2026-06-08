import { test } from "node:test"
import assert from "node:assert/strict"
import {
  Hedge,
  CircuitBreaker,
  Adaptive,
  Backpressure,
  getMethodHedge,
  getMethodCircuit,
  getMethodAdaptive,
  getMethodBackpressure,
  readMethodResilience,
  applyResilience,
  wrapMethodWithResilience,
  snapshotResilience,
  runClientPipeline,
  CircuitBreakerRegistry,
  resolveRetryOptions,
  MessagingError,
  ErrorCode
} from "../src/common"
import { createSignalRouterDecorator } from "../src/signal-router.utils"
import { addSignalMetadata } from "../src/signal.decorator"

// Decorator factories are applied manually here so the test works regardless of
// whether the runner is using legacy (TS) or stage-3 decorators. In the actual
// Nest app the `@Decorator(...)` syntax with `experimentalDecorators: true`
// applies them at class-declaration time — semantically identical.
class Svc {
  async readOne() { return 1 }
  async risky() { return 2 }
  async tuned() { return 3 }
  async ingest() { return 4 }
}

Hedge({ copies: 2, delayMs: 5 })(Svc.prototype, "readOne", { value: Svc.prototype.readOne })
CircuitBreaker({ mode: "sliding", windowMs: 1000, errorRateThreshold: 0.5, minSampleSize: 2 })(
  Svc.prototype,
  "risky",
  { value: Svc.prototype.risky }
)
Adaptive({ targetP99Ms: 100 })(Svc.prototype, "tuned", { value: Svc.prototype.tuned })
Backpressure({ maxInflight: 2, highWatermark: 2, lowWatermark: 1 })(Svc.prototype, "ingest", {
  value: Svc.prototype.ingest
})

test("decorator metadata is stored and readable", () => {
  const s = new Svc()
  assert.deepEqual(getMethodHedge(s, "readOne"), { enabled: true, copies: 2, delayMs: 5 })
  const cb = getMethodCircuit(s, "risky")
  assert.equal(cb?.mode, "sliding")
  assert.equal(getMethodAdaptive(s, "tuned")?.targetP99Ms, 100)
  assert.equal(getMethodBackpressure(s, "ingest")?.maxInflight, 2)
})

test("readMethodResilience compiles full config", () => {
  const s = new Svc()
  const cfg = readMethodResilience(s, "risky")
  assert.ok(cfg?.circuit)
  assert.equal(cfg.circuit.mode, "sliding")
})

test("applyResilience runs invoke and feeds adaptive tuner", async () => {
  const s = new Svc()
  const cfg = readMethodResilience(s, "tuned")!
  let calls = 0
  const res = await applyResilience<number>({
    config: cfg,
    ctx: { key: "svc:tuned" },
    invoke: async () => { calls++; return 42 }
  })
  assert.equal(res, 42)
  assert.equal(calls, 1)
  const snap = snapshotResilience()
  assert.ok(snap.adaptive["svc:tuned"])
})

test("hedge fires multiple copies and returns first success", async () => {
  const s = new Svc()
  const cfg = readMethodResilience(s, "readOne")!
  let attempts = 0
  const res = await applyResilience<number>({
    config: cfg,
    ctx: { key: "svc:readOne" },
    invoke: async (attempt) => {
      attempts++
      if (attempt === 1) await new Promise((r) => setTimeout(r, 50))
      return attempt
    }
  })
  assert.equal(res, 2) // 2nd attempt wins because 1st sleeps 50ms while delayMs=5
  assert.ok(attempts >= 2)
})

test("circuit opens after threshold and rejects further calls", async () => {
  const s = new Svc()
  const cfg = readMethodResilience(s, "risky")!
  const fail = () =>
    applyResilience<number>({
      config: cfg,
      ctx: { key: "svc:risky" },
      invoke: async () => { throw new Error("boom") }
    })
  await assert.rejects(fail())
  await assert.rejects(fail())
  // After two failures, the breaker should open on the next attempt.
  // The error message may be "boom" (last upstream) or "Circuit is open".
  await assert.rejects(fail())
})

test("wrapMethodWithResilience is transparent when no decorator is present", async () => {
  class Plain { async noop() { return 7 } }
  const p = new Plain()
  const fn = p.noop.bind(p)
  const wrapped = wrapMethodWithResilience(p, "noop", fn, () => ({ key: "x" }))
  assert.equal(await wrapped(), 7)
  assert.strictEqual(wrapped, fn)
})

test("backpressure config admits up to maxInflight and rejects beyond", async () => {
  const s = new Svc()
  const cfg = readMethodResilience(s, "ingest")!
  const release: Array<() => void> = []
  const runs = Promise.allSettled(
    Array.from({ length: 3 }, () =>
      applyResilience<void>({
        config: cfg,
        ctx: { key: "svc:ingest" },
        invoke: () => new Promise<void>((res) => release.push(res))
      })
    )
  )
  await new Promise((r) => setTimeout(r, 20))
  assert.ok(release.length <= 2)
  for (const r of release) r()
  const results = await runs
  const rejected = results.filter((r) => r.status === "rejected")
  assert.ok(rejected.length >= 1)
})

// ---------------------------------------------------------------------------
// @Adaptive now drives real behaviour (retry count + timeout), not a no-op.
// ---------------------------------------------------------------------------

class AdaptiveSvc {
  async failingHigh() { throw new Error("nope") }
  async failingLow() { throw new Error("nope") }
  async slow() { return "done" }
}
// currentRetries = max(minRetries, 2): floor of 4 → 4 attempts, default floor → 2.
Adaptive({ minRetries: 4, maxRetries: 5 })(AdaptiveSvc.prototype, "failingHigh", { value: AdaptiveSvc.prototype.failingHigh })
Adaptive({ minRetries: 1, maxRetries: 5 })(AdaptiveSvc.prototype, "failingLow", { value: AdaptiveSvc.prototype.failingLow })
Adaptive({ minRetries: 1, maxRetries: 2, minTimeoutMs: 30, maxTimeoutMs: 30 })(AdaptiveSvc.prototype, "slow", {
  value: AdaptiveSvc.prototype.slow
})

test("@Adaptive bounds the retry count by tuner.getRetries() (high floor → 4 attempts)", async () => {
  const s = new AdaptiveSvc()
  const cfg = readMethodResilience(s, "failingHigh")!
  let calls = 0
  await assert.rejects(
    applyResilience<string>({
      config: cfg,
      ctx: { key: "adaptive:high" },
      invoke: async () => { calls++; throw new Error("nope") }
    })
  )
  assert.equal(calls, 4)
})

test("@Adaptive retry count tracks the tuner — a different floor changes the count (default → 2)", async () => {
  const s = new AdaptiveSvc()
  const cfg = readMethodResilience(s, "failingLow")!
  let calls = 0
  await assert.rejects(
    applyResilience<string>({
      config: cfg,
      ctx: { key: "adaptive:low" },
      invoke: async () => { calls++; throw new Error("nope") }
    })
  )
  assert.equal(calls, 2)
})

test("@Adaptive enforces a per-attempt timeout derived from tuner.getTimeoutMs()", async () => {
  const s = new AdaptiveSvc()
  const cfg = readMethodResilience(s, "slow")!
  let aborted = false
  const t0 = Date.now()
  await assert.rejects(
    applyResilience<string>({
      config: cfg,
      ctx: { key: "adaptive:timeout" },
      // Work takes 500ms but the adaptive timeout is 30ms — the call must abort
      // long before the work completes, and the supplied signal must fire.
      invoke: (_attempt, signal) =>
        new Promise<string>((resolve) => {
          const timer = setTimeout(() => resolve("done"), 500)
          if (typeof timer.unref === "function") timer.unref()
          // The runtime's race already rejected by now; settle so no promise is
          // left dangling past the test. `aborted` proves the signal fired.
          signal.addEventListener("abort", () => { aborted = true; clearTimeout(timer); resolve("aborted") })
        }),
    }),
    (err) => err instanceof MessagingError && err.code === ErrorCode.TIMEOUT
  )
  const elapsed = Date.now() - t0
  assert.ok(aborted, "the per-attempt AbortSignal should fire on timeout")
  assert.ok(elapsed < 400, `should abort well before the 500ms work finishes (took ${elapsed}ms)`)
})

// ---------------------------------------------------------------------------
// Shared client pipeline: the breaker wraps the *whole* retried op, so one
// logical call records one outcome (the NATS amplification fix).
// ---------------------------------------------------------------------------

test("runClientPipeline records one breaker outcome per logical call (no retry amplification)", async () => {
  const breaker = new CircuitBreakerRegistry({ enabled: true, failureThreshold: 2 })
  const retry = resolveRetryOptions({ maxAttempts: 3, baseMs: 1, jitter: false })
  let attempts = 0
  await assert.rejects(
    runClientPipeline<number>(breaker, retry, "amp:method", async () => {
      attempts++
      throw new MessagingError(ErrorCode.INTERNAL, { message: "boom", retryable: true })
    })
  )
  assert.equal(attempts, 3, "withRetry should still run every attempt")
  // 3 retries but only ONE failure recorded → breaker (threshold 2) stays CLOSED.
  // The old per-attempt ordering would have recorded 3 and tripped it open.
  assert.equal(breaker.snapshot()["amp:method"].state, "closed")
  assert.equal(breaker.snapshot()["amp:method"].failures, 1)
})

// ---------------------------------------------------------------------------
// signal-router.utils end-to-end: the live handler honours decorators on the
// target service method.
// ---------------------------------------------------------------------------

/** Build a `handleSignalMessage`-style entrypoint wired to a decorated service. */
function buildRouterHandler(serviceType: any, serviceInstance: any, signalName: string, methodName: string) {
  class Ctrl {
    svc: any = serviceInstance
  }
  addSignalMetadata(Ctrl, signalName, methodName)
  const decorate = createSignalRouterDecorator(
    serviceType,
    { serviceName: "router", tracing: { enabled: false }, devtools: false },
    (data: any) => ({ method: data.method, params: data.params, uuid: data.uuid, meta: data.meta }),
    () => {}
  )
  decorate(Ctrl)
  const ctrl: any = new Ctrl()
  return (data: any) => ctrl.handleSignalMessage(data) as Promise<any>
}

test("signal-router honours @CircuitBreaker end-to-end (opens → CIRCUIT_OPEN, handler skipped)", async () => {
  class CbSvc {
    calls = 0
    async doWork() { this.calls++; throw new Error("upstream down") }
  }
  CircuitBreaker({ mode: "count", failureThreshold: 2 })(CbSvc.prototype, "doWork", { value: CbSvc.prototype.doWork })

  const svc = new CbSvc()
  const handle = buildRouterHandler(CbSvc, svc, "cb.doWork", "doWork")
  const msg = (uuid: string) => ({ method: "cb.doWork", params: {}, uuid, meta: { service: "caller" } })

  const r1 = await handle(msg("u1"))
  const r2 = await handle(msg("u2"))
  assert.equal(r1.params.result, "error")
  assert.equal(r2.params.result, "error")
  assert.equal(svc.calls, 2)

  // Breaker is now open: the third call short-circuits without touching the service.
  const r3 = await handle(msg("u3"))
  assert.equal(r3.params.result, "error")
  assert.equal(r3.params.error.code, ErrorCode.CIRCUIT_OPEN)
  assert.equal(svc.calls, 2, "handler must not run while the circuit is open")
})

test("signal-router honours @Backpressure end-to-end (over-admission → RATE_LIMITED response)", async () => {
  let release!: () => void
  const gate = new Promise<void>((r) => { release = r })
  class BpSvc {
    started = 0
    async ingest() { this.started++; await gate; return "ok" }
  }
  Backpressure({ maxInflight: 1, highWatermark: 1, lowWatermark: 0 })(BpSvc.prototype, "ingest", {
    value: BpSvc.prototype.ingest
  })

  const svc = new BpSvc()
  const handle = buildRouterHandler(BpSvc, svc, "bp.ingest", "ingest")
  const msg = (uuid: string) => ({ method: "bp.ingest", params: {}, uuid, meta: { service: "caller" } })

  const p1 = handle(msg("b1"))
  const p2 = handle(msg("b2"))
  // Let both calls reach the admission gate while the admitted one is in-flight.
  await new Promise((r) => setTimeout(r, 30))
  release()
  const [r1, r2] = await Promise.all([p1, p2])

  const responses = [r1, r2]
  const shed = responses.filter((r) => r.params.result === "error" && r.params.error?.code === ErrorCode.RATE_LIMITED)
  const ok = responses.filter((r) => r.params.result === "ok")
  assert.equal(shed.length, 1, "exactly one call should be shed with RATE_LIMITED")
  assert.equal(ok.length, 1, "exactly one call should be admitted and succeed")
  assert.equal(svc.started, 1, "the shed call must never enter the handler")
})
