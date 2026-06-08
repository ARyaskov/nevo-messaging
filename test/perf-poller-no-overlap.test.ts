import { test } from "node:test"
import assert from "node:assert/strict"
import { Outbox } from "../src/common/outbox"
import type { OutboxStore, OutboxRecord, OutboxMarkResult, OutboxPublisher } from "../src/common/outbox"
import { ContractPoller } from "../src/common/contract-poller"
import type { ContractFetcher } from "../src/common/contract-poller"
import { CONTRACT_PROTOCOL_VERSION } from "../src/common/contract"
import type { ServiceContract } from "../src/common/contract"
import { SagaRecovery, SagaStepRegistry, InMemorySagaStore } from "../src/common/saga"
import type { SagaSnapshot } from "../src/common/saga"

// A tracker that records concurrent entries into a body whose work takes longer
// than the poller's interval. If the scheduler overlapped runs, `max` rises
// above 1; a self-scheduling loop keeps it pinned at 1.
class ConcurrencyTracker {
  current = 0
  max = 0
  calls = 0
  async run(workMs: number): Promise<void> {
    this.calls++
    this.current++
    if (this.current > this.max) this.max = this.current
    try {
      await new Promise<void>((r) => setTimeout(r, workMs))
    } finally {
      this.current--
    }
  }
}

const tick = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

test("Outbox.start does not overlap flushes when the body outlasts the interval", async () => {
  const tracker = new ConcurrencyTracker()
  // A store whose listPending always returns work, and a publisher that takes
  // 40ms — well over the 5ms interval — so naive setInterval would overlap.
  const store: OutboxStore = {
    async save() {},
    async markPublished(): Promise<OutboxMarkResult> { return { owned: true, status: "published", attempts: 0 } },
    async markFailed(): Promise<OutboxMarkResult> { return { owned: true, status: "failed", attempts: 1 } },
    async listPending(): Promise<OutboxRecord[]> {
      return [{ id: "x", serviceName: "s", method: "m", params: {}, createdAt: Date.now(), attempts: 0, status: "pending" }]
    }
  }
  const publisher: OutboxPublisher = {
    async emit() { await tracker.run(40) }
  }
  const outbox = new Outbox(store, publisher, { intervalMs: 5, batch: 1 })
  outbox.start()
  await tick(300)
  outbox.stop()
  const callsAtStop = tracker.calls
  assert.equal(tracker.max, 1, "flushes must never overlap")
  assert.ok(tracker.calls >= 2, "should have run multiple flushes")
  await tick(150)
  assert.equal(tracker.calls, callsAtStop, "no further flushes after stop()")
})

test("ContractPoller.start does not overlap ticks when fetch outlasts the interval", async () => {
  const tracker = new ConcurrencyTracker()
  const contract: ServiceContract = {
    protocol: CONTRACT_PROTOCOL_VERSION,
    serviceName: "svc",
    serviceVersion: "1",
    generatedAt: Date.now(),
    methods: []
  }
  const fetcher: ContractFetcher = {
    async fetch(): Promise<ServiceContract> {
      await tracker.run(40)
      return contract
    }
  }
  const poller = new ContractPoller(["svc"], fetcher, { intervalMs: 5 })
  poller.start()
  await tick(300)
  poller.stop()
  const callsAtStop = tracker.calls
  assert.equal(tracker.max, 1, "ticks must never overlap")
  assert.ok(tracker.calls >= 2, "should have run multiple ticks")
  await tick(150)
  assert.equal(tracker.calls, callsAtStop, "no further ticks after stop()")
})

test("SagaRecovery.start does not overlap recovery passes when listPending outlasts the interval", async () => {
  const tracker = new ConcurrencyTracker()
  const base = new InMemorySagaStore()
  // Wrap the store so listPending blocks for 40ms — longer than the interval.
  const store = {
    save: base.save.bind(base),
    load: base.load.bind(base),
    delete: base.delete.bind(base),
    async listPending(): Promise<SagaSnapshot[]> {
      await tracker.run(40)
      return []
    }
  }
  const registry = new SagaStepRegistry()
  const recovery = new SagaRecovery(store, registry, { intervalMs: 5 })
  recovery.start()
  await tick(300)
  recovery.stop()
  const callsAtStop = tracker.calls
  assert.equal(tracker.max, 1, "recovery passes must never overlap")
  assert.ok(tracker.calls >= 2, "should have run multiple recovery passes")
  await tick(150)
  assert.equal(tracker.calls, callsAtStop, "no further passes after stop()")
})
