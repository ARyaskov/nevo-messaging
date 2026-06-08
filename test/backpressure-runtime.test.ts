import { test } from "node:test"
import assert from "node:assert/strict"
import { wrapSubscriptionHandler, installBackpressureFromDecorator } from "../src/common/backpressure-runtime"
import { Backpressure } from "../src/common/resilience-decorators"
import type { PausableSubscription, SubscriptionContext } from "../src/common"

function fakeSub(): PausableSubscription & { paused: number; resumed: number } {
  let paused = false
  const state = {
    paused: 0,
    resumed: 0,
    pause() { paused = true; state.paused++ },
    resume() { paused = false; state.resumed++ },
    isPaused() { return paused },
    async unsubscribe() {}
  }
  return state as any
}

function fakeCtx(extra: Partial<SubscriptionContext> = {}): SubscriptionContext {
  return { meta: {}, async ack() {}, async nack() {}, ...extra } as SubscriptionContext
}

test("wrapSubscriptionHandler pauses on high water, resumes on low water", async () => {
  const sub = fakeSub()
  let resolveOne!: () => void
  const block = new Promise<void>((r) => (resolveOne = r))
  const handler = wrapSubscriptionHandler<number>(
    async () => block,
    sub,
    { maxInflight: 4, highWatermark: 2, lowWatermark: 1 }
  )
  void handler(1, fakeCtx())
  void handler(2, fakeCtx())
  await new Promise((r) => setTimeout(r, 5))
  assert.ok(sub.paused >= 1, "should have paused after high watermark crossed")
  resolveOne()
  await new Promise((r) => setTimeout(r, 5))
  resolveOne()
  await new Promise((r) => setTimeout(r, 10))
})

test("installBackpressureFromDecorator wraps based on @Backpressure metadata", async () => {
  class Svc {
    async ingest(_msg: number) { await new Promise((r) => setTimeout(r, 20)) }
  }
  Backpressure({ maxInflight: 1, highWatermark: 1, lowWatermark: 0, onOverflow: "reject" } as any)(
    Svc.prototype,
    "ingest",
    { value: Svc.prototype.ingest }
  )
  const s = new Svc()
  const sub = fakeSub()
  const wrapped = installBackpressureFromDecorator<number>(s, "ingest", (m, _c) => s.ingest(m), sub)
  const first = wrapped(1, fakeCtx())
  await new Promise((r) => setTimeout(r, 5))
  await assert.rejects(() => Promise.resolve(wrapped(2, fakeCtx())), /Backpressure|RATE/)
  await first
})

test("nack-mode dispatches ctx.nack instead of throwing", async () => {
  const sub = fakeSub()
  const wrapped = wrapSubscriptionHandler<number>(
    async () => new Promise<void>(() => {}),
    sub,
    { maxInflight: 1, highWatermark: 1, lowWatermark: 0, onOverflow: "nack" }
  )
  let nacks = 0
  const ctx: SubscriptionContext = {
    meta: {},
    async ack() {},
    async nack() { nacks++ }
  }
  void wrapped(1, ctx)
  await new Promise((r) => setTimeout(r, 5))
  await wrapped(2, ctx) // resolves cleanly under nack-mode
  assert.equal(nacks, 1)
})
