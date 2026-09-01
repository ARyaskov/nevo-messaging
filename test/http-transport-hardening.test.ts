import { test } from "node:test"
import assert from "node:assert/strict"
import type { ExecutionContext } from "@nestjs/common"
import {
  DEFAULT_MAX_SSE_CHANNELS,
  HttpSseBroker,
  HttpTransportAuthGuard,
  HttpTransportController,
  resetHttpSseFallbackBroker
} from "../src/transports/http/http.transport.controller"
import { DiscoveryRegistry } from "../src/common/discovery"

function ctx(req: unknown = {}): ExecutionContext {
  return { switchToHttp: () => ({ getRequest: () => req }) } as unknown as ExecutionContext
}

// ---------------------------------------------------------------------------
// Auth guard fails closed
// ---------------------------------------------------------------------------

test("transport auth guard denies when neither authorize nor insecure is configured", async () => {
  const guard = new HttpTransportAuthGuard()
  await assert.rejects(() => guard.canActivate(ctx()), /unauthorized/i)
})

test("transport auth guard denies when options exist but say nothing about auth", async () => {
  const guard = new HttpTransportAuthGuard({})
  await assert.rejects(() => guard.canActivate(ctx()), /unauthorized/i)
})

test("transport auth guard allows only on an explicit insecure opt-out", async () => {
  const guard = new HttpTransportAuthGuard({ insecure: true })
  assert.equal(await guard.canActivate(ctx()), true)
})

test("transport auth guard delegates to authorize and requires a strict true", async () => {
  const seen: unknown[] = []
  const allow = new HttpTransportAuthGuard({
    authorize: (req) => {
      seen.push(req)
      return true
    }
  })
  assert.equal(await allow.canActivate(ctx({ id: 1 })), true)
  assert.deepEqual(seen, [{ id: 1 }])

  const deny = new HttpTransportAuthGuard({ authorize: async () => false })
  assert.equal(await deny.canActivate(ctx()), false)

  // A truthy non-boolean must not pass.
  const sloppy = new HttpTransportAuthGuard({ authorize: () => "yes" as unknown as boolean })
  assert.equal(await sloppy.canActivate(ctx()), false)
})

// ---------------------------------------------------------------------------
// SSE broker: channels are reference-counted and bounded
// ---------------------------------------------------------------------------

test("sse broker creates a channel only while a subscriber is attached", () => {
  const broker = new HttpSseBroker()
  try {
    assert.equal(broker.channelCount(), 0)

    const first = broker.stream("svc-events.sub").subscribe()
    assert.equal(broker.channelCount(), 1)

    const second = broker.stream("svc-events.sub").subscribe()
    assert.equal(broker.channelCount(), 1, "same channel is shared, not duplicated")

    first.unsubscribe()
    assert.equal(broker.channelCount(), 1, "still held by the second subscriber")

    second.unsubscribe()
    assert.equal(broker.channelCount(), 0, "last subscriber out removes the channel")
  } finally {
    broker.onModuleDestroy()
  }
})

test("sse broker drops a publish nobody is listening to instead of retaining a channel", () => {
  const broker = new HttpSseBroker()
  try {
    assert.equal(broker.publish("nobody-events.sub", { a: 1 }), false)
    assert.equal(broker.channelCount(), 0, "an unsolicited publish must not allocate a channel")
  } finally {
    broker.onModuleDestroy()
  }
})

test("sse broker delivers to live subscribers", () => {
  const broker = new HttpSseBroker()
  const received: string[] = []
  try {
    const sub = broker.stream("svc-events.sub").subscribe((frame) => received.push(frame.data))
    assert.equal(broker.publish("svc-events.sub", { hello: "world" }), true)
    assert.deepEqual(received, ['{"hello":"world"}'])
    sub.unsubscribe()
  } finally {
    broker.onModuleDestroy()
  }
})

test("sse broker refuses to open more channels than maxChannels", () => {
  const broker = new HttpSseBroker({ maxChannels: 2 })
  try {
    const a = broker.stream("a").subscribe()
    const b = broker.stream("b").subscribe()
    assert.equal(broker.channelCount(), 2)

    assert.throws(() => broker.stream("c"), /channel limit reached/i)

    // Freeing one makes room again — the cap is not a permanent latch.
    a.unsubscribe()
    const c = broker.stream("c").subscribe()
    assert.equal(broker.channelCount(), 2)

    b.unsubscribe()
    c.unsubscribe()
    assert.equal(broker.channelCount(), 0)
  } finally {
    broker.onModuleDestroy()
  }
})

test("a forged ?service= flood cannot grow the broker past its cap", () => {
  const broker = new HttpSseBroker({ maxChannels: 8 })
  const controller = new HttpTransportController(broker)
  try {
    let refused = 0
    for (let i = 0; i < 500; i++) {
      try {
        // No subscriber attaches, mirroring a caller that opens and abandons the stream.
        controller.streamSubscription(`forged-${i}`)
      } catch {
        refused++
      }
    }
    assert.equal(broker.channelCount(), 0, "streams that were never subscribed hold nothing")
    assert.equal(refused, 0, "unsubscribed streams must not consume capacity either")
  } finally {
    broker.onModuleDestroy()
  }
})

test("controller rejects a blank ?service= before touching the broker", () => {
  const broker = new HttpSseBroker()
  const controller = new HttpTransportController(broker)
  try {
    assert.throws(() => controller.streamSubscription(""), /non-empty string/)
    assert.throws(() => controller.streamSubscription(undefined as unknown as string), /non-empty string/)
    assert.equal(broker.channelCount(), 0)
  } finally {
    broker.onModuleDestroy()
  }
})

test("controller without an injected broker reuses one shared fallback", () => {
  resetHttpSseFallbackBroker()
  try {
    const a = new HttpTransportController()
    const b = new HttpTransportController()
    const streamA = a.streamBroadcast().subscribe()
    const received: string[] = []
    const streamB = b.streamBroadcast().subscribe((frame) => received.push(frame.data))

    // Both controllers must be talking to the same hub.
    b.publishBroadcast({ n: 1 })
    assert.deepEqual(received, ['{"n":1}'])

    streamA.unsubscribe()
    streamB.unsubscribe()
  } finally {
    resetHttpSseFallbackBroker()
  }
})

test("default channel cap is exported and sane", () => {
  assert.equal(typeof DEFAULT_MAX_SSE_CHANNELS, "number")
  assert.ok(DEFAULT_MAX_SSE_CHANNELS >= 64)
})

// ---------------------------------------------------------------------------
// DiscoveryRegistry is bounded
// ---------------------------------------------------------------------------

test("discovery registry caps retained instances", () => {
  const reg = new DiscoveryRegistry({ maxEntries: 16 })
  for (let i = 0; i < 1000; i++) {
    reg.update({ serviceName: `forged-${i}`, instanceId: `i-${i}`, transport: "nats", ts: Date.now() })
  }
  assert.ok(reg.size() <= 16, `expected <= 16 entries, got ${reg.size()}`)
  assert.equal(reg.list().length, reg.size())
})

test("discovery registry evicts stale entries before live ones", () => {
  const reg = new DiscoveryRegistry({ maxEntries: 4 })
  reg.startBackgroundPrune(50_000)
  try {
    // Three stale instances…
    for (const id of ["s1", "s2", "s3"]) {
      reg.update({ serviceName: "stale", instanceId: id, transport: "nats", ts: Date.now() })
    }
    for (const entry of reg.list()) entry.lastSeen = Date.now() - 500_000

    // …then a live one that must survive the next insert.
    reg.update({ serviceName: "live", instanceId: "l1", transport: "nats", ts: Date.now() })
    reg.update({ serviceName: "new", instanceId: "n1", transport: "nats", ts: Date.now() })

    assert.equal(reg.isAvailable("live", 60_000), true, "a live instance must not be evicted while stale ones exist")
    assert.ok(reg.size() <= 4)
  } finally {
    reg.stopBackgroundPrune()
  }
})

test("discovery registry keeps its per-service index consistent through eviction and removal", () => {
  const reg = new DiscoveryRegistry({ maxEntries: 3 })
  reg.update({ serviceName: "user", instanceId: "a", transport: "nats", ts: Date.now() })
  reg.update({ serviceName: "user", instanceId: "b", transport: "nats", ts: Date.now() })
  assert.equal(reg.listByService("user").length, 2)
  assert.deepEqual(reg.listInstanceIdsFor("user").sort(), ["a", "b"])

  assert.equal(reg.removeInstance("user", "a"), true)
  assert.equal(reg.removeInstance("user", "a"), false)
  assert.equal(reg.listByService("user").length, 1)

  // Push past the cap; whatever survives must still be reachable through the index.
  for (let i = 0; i < 20; i++) {
    reg.update({ serviceName: "other", instanceId: `o-${i}`, transport: "nats", ts: Date.now() })
  }
  const indexed = reg.listByService("other").length + reg.listByService("user").length
  assert.equal(indexed, reg.size(), "index and primary map must agree")
})

test("discovery registry refreshes recency on update so a heartbeating peer is never the eviction victim", () => {
  const reg = new DiscoveryRegistry({ maxEntries: 3 })
  reg.update({ serviceName: "keep", instanceId: "k", transport: "nats", ts: Date.now() })
  reg.update({ serviceName: "a", instanceId: "a", transport: "nats", ts: Date.now() })
  reg.update({ serviceName: "b", instanceId: "b", transport: "nats", ts: Date.now() })
  // Heartbeat moves "keep" to the MRU end.
  reg.update({ serviceName: "keep", instanceId: "k", transport: "nats", ts: Date.now() })
  reg.update({ serviceName: "c", instanceId: "c", transport: "nats", ts: Date.now() })
  assert.equal(reg.isAvailable("keep", 60_000), true)
})
