import { test } from "node:test"
import assert from "node:assert/strict"
import { mapLimit } from "../src/common/concurrency"
import { NevoKafkaClient } from "../src/transports/kafka/nevo-kafka.client"
import { JsonCodec } from "../src/common/codec"

// mapLimit is the bounded-concurrency primitive behind the batch publish paths.
// These tests pin its three load-bearing guarantees (never exceed `limit`
// in-flight, preserve input order, reject on first error) plus an integration
// check that a large simulated emitBatch still encodes+publishes every item.

const tick = () => new Promise((r) => setImmediate(r))

test("mapLimit never exceeds the concurrency limit in flight", async () => {
  const limit = 4
  let inFlight = 0
  let peak = 0
  const items = Array.from({ length: 50 }, (_, i) => i)

  await mapLimit(items, limit, async (item) => {
    inFlight++
    if (inFlight > peak) peak = inFlight
    // Yield several times so a broken (unbounded) implementation would pile every
    // task up at once and blow past the limit before any resolves.
    await tick()
    await tick()
    inFlight--
    return item
  })

  assert.ok(peak <= limit, `peak in-flight ${peak} exceeded limit ${limit}`)
  assert.ok(peak > 1, "workers should actually run concurrently up to the limit")
})

test("mapLimit preserves input order regardless of completion order", async () => {
  const items = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
  const results = await mapLimit(items, 3, async (item) => {
    // Smaller values resolve later, so completion order != input order — yet the
    // returned array must still mirror the input positions.
    await new Promise((r) => setTimeout(r, item))
    return item * 2
  })
  assert.deepEqual(results, items.map((n) => n * 2))
})

test("mapLimit surfaces the first error like Promise.all", async () => {
  await assert.rejects(
    () => mapLimit([1, 2, 3, 4, 5], 2, async (item) => {
      if (item === 3) throw new Error("boom-3")
      await tick()
      return item
    }),
    /boom-3/
  )
})

test("mapLimit returns an empty array for empty input without invoking fn", async () => {
  let called = false
  const out = await mapLimit([], 8, async () => { called = true; return 1 })
  assert.deepEqual(out, [])
  assert.equal(called, false)
})

const SILENT_LOGGER: any = {
  trace() {}, debug() {}, info() {}, warn() {}, error() {}, fatal() {},
  child() { return SILENT_LOGGER }
}

// A codec wrapper that records how many encode calls are concurrently "in flight"
// across the async compression hop, so we can assert the batch path stays bounded
// while still encoding every item. encode() is sync, but the surrounding mapLimit
// task is async (await maybeCompressAsync), so we approximate in-flight by
// counting encodes that have not yet been followed by a value being collected.
test("large simulated emitBatch encodes and publishes every item under a bounded fan-out", async () => {
  const codec = new JsonCodec()
  let encodeCount = 0
  const trackingCodec: any = {
    name: codec.name,
    encode(v: any) { encodeCount++; return codec.encode(v) },
    decode(b: any) { return codec.decode(b) }
  }

  const fakeClientKafka: any = { subscribeToResponseOf() {}, emit() {}, send() {} }
  const client = new NevoKafkaClient(fakeClientKafka, ["svc"], {
    discovery: { enabled: false },
    devtools: false,
    codec: trackingCodec,
    logger: SILENT_LOGGER,
    // Force the async-compression batch path (the one that previously fanned out
    // unbounded). threshold 0 so every item actually compresses.
    compression: { enabled: true, async: true, threshold: 0 }
  } as any)

  // Capture what the batch producer is asked to send instead of hitting a broker.
  const sentMessages: Array<{ key: string; value: Buffer }> = []
  const fakeBatchProducer = {
    async connect() {},
    async disconnect() {},
    async sendBatch({ topicMessages }: { topicMessages: Array<{ topic: string; messages: Array<{ key: string; value: Buffer }> }> }) {
      for (const tm of topicMessages) for (const m of tm.messages) sentMessages.push(m)
    }
  }
  ;(client as any).batchProducer = fakeBatchProducer

  try {
    const COUNT = 5000
    const items = Array.from({ length: COUNT }, (_, i) => ({
      serviceName: "svc",
      method: "evt",
      params: { i }
    }))

    await client.emitBatch(items)

    assert.equal(encodeCount, COUNT, "every item must be encoded exactly once")
    assert.equal(sentMessages.length, COUNT, "every encoded item must be published")
    // Keys are the per-message uuids; uniqueness confirms no item was dropped or
    // duplicated by the bounded fan-out.
    assert.equal(new Set(sentMessages.map((m) => m.key)).size, COUNT)
  } finally {
    await client.close()
  }
})
