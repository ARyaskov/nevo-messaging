import { test } from "node:test"
import assert from "node:assert/strict"
import { hedge } from "../src/common/hedging"
import { SlidingCircuitBreakerRegistry } from "../src/common/sliding-circuit-breaker"
import { CircuitOpenError, MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"
import { AdaptiveTuner } from "../src/common/adaptive"
import { contractToOpenApi } from "../src/common/openapi-gen"
import { InMemoryEventStore } from "../src/common/event-store"
import { Inbox, InMemoryInboxStore } from "../src/common/inbox"
import { ContractPoller, createContractFetcherForClient } from "../src/common/contract-poller"

test("hedge returns the first successful copy", async () => {
  const t0 = Date.now()
  const result = await hedge(async (attempt, signal) => {
    if (attempt === 1) {
      await new Promise((r) => setTimeout(r, 200))
      return "slow"
    }
    return "fast"
  }, { enabled: true, copies: 2, delayMs: 30 })
  assert.equal(result, "fast")
  assert.ok(Date.now() - t0 < 150)
})

test("hedge falls back on rejections", async () => {
  await assert.rejects(async () => {
    await hedge(async () => { throw new Error("nope") }, { enabled: true, copies: 3, delayMs: 1 })
  })
})

test("sliding-window CB opens after error-rate exceeds threshold with min sample", () => {
  const cb = new SlidingCircuitBreakerRegistry({ enabled: true, windowMs: 60_000, errorRateThreshold: 0.5, minSampleSize: 4, resetTimeoutMs: 1000 })
  cb.before("svc:m"); cb.onSuccess("svc:m")
  cb.before("svc:m"); cb.onFailure("svc:m", new Error("x"))
  cb.before("svc:m"); cb.onFailure("svc:m", new Error("x"))
  cb.before("svc:m"); cb.onFailure("svc:m", new Error("x"))
  assert.throws(() => cb.before("svc:m"), CircuitOpenError)
})

test("sliding-window CB ignores validation errors", () => {
  const cb = new SlidingCircuitBreakerRegistry({ enabled: true, errorRateThreshold: 0.1, minSampleSize: 1 })
  cb.before("svc:m"); cb.onFailure("svc:m", new MessagingError(ErrorCode.VALIDATION_FAILED, { message: "x" }))
  cb.before("svc:m")
})

test("AdaptiveTuner observes and tunes retries", () => {
  const t = new AdaptiveTuner({ enabled: true, windowMs: 60_000, targetP99Ms: 100, minRetries: 1, maxRetries: 4 })
  for (let i = 0; i < 30; i++) t.observe(80, true)
  for (let i = 0; i < 10; i++) t.observe(50, false)
  const snap = t.snapshot()
  assert.ok(snap.sampleSize > 30)
  assert.ok(snap.retries >= 1 && snap.retries <= 4)
})

test("contractToOpenApi produces paths", () => {
  const spec = contractToOpenApi({
    protocol: "1",
    serviceName: "user",
    serviceVersion: "1.0",
    generatedAt: Date.now(),
    methods: [{ signalName: "user.get", version: "v1" }]
  }) as any
  assert.ok(spec.paths["/user-events/user.get"])
  assert.equal(spec.info.title, "user API")
})

test("event store appends and reads in order", async () => {
  const store = new InMemoryEventStore()
  await store.append({ type: "x", payload: 1 })
  await store.append({ type: "x", payload: 2 })
  const events = await store.read({ from: 1 })
  assert.equal(events.length, 2)
  assert.equal((events[0].payload as number), 1)
  assert.ok(events[0].sequence < events[1].sequence)
})

test("inbox dedupes by uuid", async () => {
  const inbox = new Inbox({ store: new InMemoryInboxStore() })
  let calls = 0
  const result1 = await inbox.dedupe("u1", async () => { calls++; return 42 })
  const result2 = await inbox.dedupe("u1", async () => { calls++; return 999 })
  assert.equal(result1, 42)
  assert.equal(result2, 42)
  assert.equal(calls, 1)
})

test("ContractPoller fires onChange on new contract", async () => {
  let invocations = 0
  let fetched = 0
  const fakeClient = {
    query: async () => {
      fetched++
      return { protocol: "1", serviceName: "user", serviceVersion: fetched < 2 ? "1.0" : "1.1", generatedAt: Date.now(), methods: [] }
    }
  }
  const poller = new ContractPoller(["user"], createContractFetcherForClient(fakeClient), { intervalMs: 1, onChange: () => invocations++ })
  await poller.pollOnce()
  await poller.pollOnce()
  poller.stop()
  assert.equal(invocations, 2)
})
