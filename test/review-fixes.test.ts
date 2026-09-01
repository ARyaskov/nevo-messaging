import { test } from "node:test"
import assert from "node:assert/strict"
import { isAccessAllowed } from "../src/common/access-control"
import type { AccessControlConfig, MessageMeta, MessageResponse } from "../src/common/types"
import { methodSubject } from "../src/transports/nats/nevo-nats.client"
import {
  normalizeWireValue,
  serializeBigInt,
  deserializeBigInt,
  stringifyWithBigInt,
  parseWithBigInt,
  STRING_ESCAPE,
  BIGINT_SENTINEL
} from "../src/common/bigint.utils"
import { JsonCodec } from "../src/common/codec"
import { computeDelay, shouldRetry, resolveRetryOptions, retryAfterHintMs } from "../src/common/retry"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"
import { CircuitBreakerRegistry } from "../src/common/circuit-breaker"
import { SlidingCircuitBreakerRegistry } from "../src/common/sliding-circuit-breaker"
import { InMemorySagaStore, Saga } from "../src/common/saga"
import { BaseMessageController } from "../src/common/base.controller"
import { InMemoryMetrics, setDefaultMetrics } from "../src/common/metrics"
import { createSignalRouterDecorator, bindSignalRouterForTesting } from "../src/signal-router.utils"
import { addSignalMetadata } from "../src/signal.decorator"
import { Cacheable } from "../src/common/method-decorators"

// ---------------------------------------------------------------------------
// ACL: default-deny once rules exist
// ---------------------------------------------------------------------------

test("ACL with rules denies unmatched methods by default", () => {
  const cfg: AccessControlConfig = {
    rules: [{ topic: "user-events", method: "user.getById", allow: ["frontend"] }]
  }
  assert.equal(isAccessAllowed(cfg, "user-events", "user.getById", "frontend"), true)
  assert.equal(isAccessAllowed(cfg, "user-events", "user.delete", "frontend"), false)
  assert.equal(isAccessAllowed(cfg, "other-events", "anything", "frontend"), false)
})

test("ACL explicit allowAllByDefault:true keeps unmatched methods open", () => {
  const cfg: AccessControlConfig = {
    rules: [{ topic: "user-events", method: "user.getById", allow: ["frontend"] }],
    allowAllByDefault: true
  }
  assert.equal(isAccessAllowed(cfg, "user-events", "user.delete", "frontend"), true)
})

test("ACL without rules stays open", () => {
  const cfg: AccessControlConfig = { rules: [] }
  assert.equal(isAccessAllowed(cfg, "user-events", "user.getById", "frontend"), true)
})

test("ACL deny-default still lets builtin nevo.* methods through", () => {
  const cfg: AccessControlConfig = {
    rules: [{ topic: "user-events", method: "user.getById", allow: ["frontend"] }]
  }
  assert.equal(isAccessAllowed(cfg, "user-events", "nevo.contract", "frontend"), true)
  assert.equal(isAccessAllowed(cfg, "user-events", "nevo.health", undefined), true)
})

test("ACL explicit deny rule overrides the builtin exemption", () => {
  const cfg: AccessControlConfig = {
    rules: [{ topic: "user-events", method: "nevo.contract", deny: ["*"] }]
  }
  assert.equal(isAccessAllowed(cfg, "user-events", "nevo.contract", "frontend"), false)
})

// ---------------------------------------------------------------------------
// NATS: method-scoped pub/sub subjects
// ---------------------------------------------------------------------------

test("methodSubject derives a method-scoped subject", () => {
  assert.equal(methodSubject("user-events.sub", "user.updated"), "user-events.sub.user.updated")
})

test("methodSubject strips the version so all versions share a subject", () => {
  assert.equal(methodSubject("user-events.sub", "user.updated@v2"), "user-events.sub.user.updated")
})

test("methodSubject falls back to a wildcard for subscribe-all", () => {
  assert.equal(methodSubject("user-events.sub", ""), "user-events.sub.>")
  assert.equal(methodSubject("user-events.sub", undefined), "user-events.sub.>")
})

test("methodSubject sanitises characters NATS subjects reject on publish", () => {
  assert.equal(methodSubject("s", "a b*c>d"), "s.a_b_c_d")
})

test("methodSubject keeps NATS wildcards in subscribe mode", () => {
  assert.equal(methodSubject("user-events.sub", "user.*", "subscribe"), "user-events.sub.user.*")
  assert.equal(methodSubject("user-events.sub", "user.>", "subscribe"), "user-events.sub.user.>")
  // Wildcards are still stripped from publish subjects.
  assert.equal(methodSubject("user-events.sub", "user.*", "publish"), "user-events.sub.user._")
})

// ---------------------------------------------------------------------------
// Wire model: copy-on-write + sentinel escaping
// ---------------------------------------------------------------------------

test("normalizeWireValue returns the SAME reference for plain payloads", () => {
  const payload = { a: 1, b: "x", c: [1, 2, { d: null }], e: true }
  assert.equal(normalizeWireValue(payload), payload)
})

test("normalizeWireValue copies only along changed paths", () => {
  const untouched = { deep: [1, 2, 3] }
  const payload = { untouched, changed: { v: 10n } }
  const out = normalizeWireValue(payload) as any
  assert.notEqual(out, payload)
  assert.equal(out.untouched, untouched)
  assert.equal(out.changed.v, `${BIGINT_SENTINEL}10`)
})

test("user strings that look like the bigint sentinel round-trip unchanged (JSON)", () => {
  const tricky = { s: `${BIGINT_SENTINEL}123`, esc: `${STRING_ESCAPE}already`, n: 42n }
  const codec = new JsonCodec()
  const decoded = codec.decode<typeof tricky>(codec.encode(tricky))
  assert.equal(decoded.s, `${BIGINT_SENTINEL}123`)
  assert.equal(decoded.esc, `${STRING_ESCAPE}already`)
  assert.equal(decoded.n, 42n)
})

test("user strings that look like the bigint sentinel round-trip unchanged (serialize/deserialize)", () => {
  const tricky = { s: `${BIGINT_SENTINEL}999`, n: 7n }
  const roundtripped = deserializeBigInt(serializeBigInt(tricky))
  assert.equal(roundtripped.s, `${BIGINT_SENTINEL}999`)
  assert.equal(roundtripped.n, 7n)
})

test("stringify/parseWithBigInt escape and unescape sentinel-shaped strings", () => {
  const tricky = { s: `${BIGINT_SENTINEL}5`, n: 5n }
  const parsed = parseWithBigInt(stringifyWithBigInt(tricky))
  assert.equal(parsed.s, `${BIGINT_SENTINEL}5`)
  assert.equal(parsed.n, 5n)
})

// ---------------------------------------------------------------------------
// Retry: retryAfterMs hint + syscall codes
// ---------------------------------------------------------------------------

test("computeDelay honours a server retryAfterMs hint", () => {
  const opts = resolveRetryOptions({ baseMs: 100, maxMs: 2000 })
  const err = new MessagingError(ErrorCode.RATE_LIMITED, { message: "slow down", retryAfterMs: 5000, retryable: true })
  assert.equal(computeDelay(1, opts, err), 5000)
})

test("retryAfterMs hint is capped at 30s and ignores junk", () => {
  const err = new MessagingError(ErrorCode.RATE_LIMITED, { retryAfterMs: 120_000, retryable: true })
  assert.equal(retryAfterHintMs(err), 30_000)
  assert.equal(retryAfterHintMs(new MessagingError(ErrorCode.RATE_LIMITED, { retryAfterMs: -5 })), null)
  assert.equal(retryAfterHintMs(new Error("nope")), null)
})

test("shouldRetry recognises syscall codes on plain errors", () => {
  const opts = resolveRetryOptions({})
  const err = Object.assign(new Error("socket hang up"), { code: "ECONNRESET" })
  assert.equal(shouldRetry(err, opts), true)
  const noRetry = Object.assign(new Error("nope"), { code: "EACCES" })
  assert.equal(shouldRetry(noRetry, opts), false)
})

// ---------------------------------------------------------------------------
// Circuit breaker: a hung half-open probe no longer wedges the breaker
// ---------------------------------------------------------------------------

test("count breaker: stuck half-open probe is taken over after resetTimeoutMs", async () => {
  const reg = new CircuitBreakerRegistry({ enabled: true, failureThreshold: 1, resetTimeoutMs: 20 })
  const key = "svc:m"
  reg.onFailure(key, new Error("boom")) // opens
  assert.throws(() => reg.before(key)) // still open
  await new Promise((r) => setTimeout(r, 25))
  reg.before(key) // half-open, probe #1 starts and never settles
  assert.throws(() => reg.before(key)) // probe in flight → rejected
  await new Promise((r) => setTimeout(r, 25))
  reg.before(key) // probe timed out → this caller takes over
  reg.onSuccess(key)
  reg.before(key) // closed again
})

test("sliding breaker: stuck half-open probe is taken over after resetTimeoutMs", async () => {
  const reg = new SlidingCircuitBreakerRegistry({ enabled: true, minSampleSize: 1, errorRateThreshold: 0.5, resetTimeoutMs: 20 })
  const key = "svc:m"
  reg.onFailure(key, new Error("boom"))
  assert.throws(() => reg.before(key))
  await new Promise((r) => setTimeout(r, 25))
  reg.before(key)
  assert.throws(() => reg.before(key))
  await new Promise((r) => setTimeout(r, 25))
  reg.before(key)
  reg.onSuccess(key)
  reg.before(key)
})

// ---------------------------------------------------------------------------
// Saga: live lease heartbeat blocks a recovery claim while the saga runs
// ---------------------------------------------------------------------------

test("a running saga holds its lease so another worker cannot claim it", async () => {
  const store = new InMemorySagaStore()
  let midStepClaim: boolean | null = null
  const saga = new Saga<{ v: number }>()
    .withStore(store, "saga-lease-test")
    .withLease(5_000)
    .step({
      name: "slow",
      execute: async () => {
        await new Promise((r) => setTimeout(r, 30))
        midStepClaim = await store.claim("saga-lease-test", "recovery-worker", 5_000)
      }
    })
  const result = await saga.run({ v: 1 })
  assert.equal(result.status, "success")
  assert.equal(midStepClaim, false)
})

// ---------------------------------------------------------------------------
// Unified dispatch pipeline (via BaseMessageController)
// ---------------------------------------------------------------------------

class TestController extends BaseMessageController {
  constructor(handlers: Record<string, any>, instances: any[]) {
    super("testsvc", instances, handlers)
  }
  protected extractMessageData(data: any): { method: string; uuid: string; params: any; meta?: MessageMeta } {
    if (data?.__malformed) throw new Error("cannot parse")
    return { method: data.method, uuid: data.uuid, params: data.params, meta: data.meta }
  }
  async handleMessage(data: any): Promise<MessageResponse> {
    return this.processMessage(data)
  }
}

test("pipeline counts an unknown method as an error in metrics", async () => {
  const metrics = new InMemoryMetrics()
  setDefaultMetrics(metrics)
  try {
    const ctl = new TestController({ known: { serviceMethod: "known" } }, [{ known: async () => "ok" }])
    const resp = await ctl.handleMessage({ method: "definitely.missing", uuid: "u1", params: {} })
    assert.equal(resp.params.result, "error")
    assert.equal(resp.params.error?.code, ErrorCode.METHOD_NOT_FOUND)
    const exposed = metrics.expose()
    assert.match(exposed, /nevo_messaging_request_errors_total\{[^}]*status="error"[^}]*\} 1/)
  } finally {
    setDefaultMetrics(new InMemoryMetrics())
  }
})

test("pipeline turns a malformed envelope into BAD_REQUEST instead of throwing", async () => {
  const ctl = new TestController({}, [])
  const resp = await ctl.handleMessage({ __malformed: true, uuid: "u2" })
  assert.equal(resp.params.result, "error")
  assert.equal(resp.params.error?.code, ErrorCode.BAD_REQUEST)
})

test("pipeline dispatches a registered method end to end", async () => {
  const ctl = new TestController({ "user.get": { serviceMethod: "getUser" } }, [{ getUser: async (p: any) => ({ id: p.id, name: "x" }) }])
  const resp = await ctl.handleMessage({ method: "user.get", uuid: "u3", params: { id: 5 } })
  assert.deepEqual(resp.params.result, { id: 5, name: "x" })
})

test("pipeline serves the builtin contract method", async () => {
  const ctl = new TestController({ "user.get": { serviceMethod: "getUser" } }, [{ getUser: async () => ({}) }])
  const resp = await ctl.handleMessage({ method: "nevo.contract", uuid: "u4", params: {} })
  assert.notEqual(resp.params.result, "error")
  assert.equal((resp.params.result as any).serviceName, "testsvc")
})

// ---------------------------------------------------------------------------
// Router pipeline: hooks and @Cacheable interaction
// ---------------------------------------------------------------------------

function buildRouter(serviceInstance: any, signalName: string, methodName: string, options?: Record<string, unknown>) {
  class Ctrl {
    svc: any = serviceInstance
  }
  addSignalMetadata(Ctrl, signalName, methodName)
  const opts: any = { serviceName: "router", tracing: { enabled: false }, devtools: false, ...options }
  const decorate = createSignalRouterDecorator(
    serviceInstance.constructor,
    opts,
    (data: any) => ({ method: data.method, params: data.params, uuid: data.uuid, meta: data.meta }),
    () => {}
  )
  decorate(Ctrl)
  bindSignalRouterForTesting(Ctrl, opts)
  const ctrl: any = new Ctrl()
  return (data: any) => ctrl.handleSignalMessage(data) as Promise<MessageResponse>
}

test("router runs before and after hooks around a handler", async () => {
  class Svc {
    async echo(p: any) {
      return { got: p.v }
    }
  }
  const seen: string[] = []
  const handle = buildRouter(new Svc(), "echo.run", "echo", {
    before: (ctx) => {
      seen.push("before")
      return ctx.params
    },
    after: (ctx) => {
      seen.push("after")
      return { ...ctx.response, params: { result: { wrapped: (ctx.response.params.result as any).got } } }
    }
  })
  const resp = await handle({ method: "echo.run", uuid: "r1", params: { v: 7 } })
  assert.deepEqual(resp.params.result, { wrapped: 7 })
  assert.deepEqual(seen, ["before", "after"])
})

test("router does NOT re-run the after hook on a @Cacheable hit", async () => {
  let afterRuns = 0
  let handlerRuns = 0
  class Svc {
    @Cacheable({ ttlMs: 60_000 })
    async compute() {
      handlerRuns++
      return { n: handlerRuns }
    }
  }
  const handle = buildRouter(new Svc(), "svc.compute", "compute", {
    after: (ctx) => {
      afterRuns++
      return ctx.response
    }
  })
  const first = await handle({ method: "svc.compute", uuid: "c1", params: {} })
  const second = await handle({ method: "svc.compute", uuid: "c2", params: {} })
  assert.deepEqual(first.params.result, { n: 1 })
  assert.deepEqual(second.params.result, { n: 1 }) // served from cache, handler ran once
  assert.equal(handlerRuns, 1)
  assert.equal(afterRuns, 1) // after hook only ran for the miss, not the hit
})

test("router error paths are counted as errors, not ok", async () => {
  const metrics = new InMemoryMetrics()
  setDefaultMetrics(metrics)
  try {
    class Svc {
      async run() {
        return 1
      }
    }
    const handle = buildRouter(new Svc(), "svc.run", "run")
    const resp = await handle({ method: "svc.nope", uuid: "e1", params: {} })
    assert.equal(resp.params.result, "error")
    assert.equal(resp.params.error?.code, ErrorCode.METHOD_NOT_FOUND)
    assert.match(metrics.expose(), /nevo_messaging_request_errors_total\{[^}]*status="error"[^}]*\} 1/)
  } finally {
    setDefaultMetrics(new InMemoryMetrics())
  }
})
