import { test } from "node:test"
import assert from "node:assert/strict"
import { CqrsBridge, type CqrsLikeBus } from "../src/common/cqrs-bridge"
import { serializeBigInt, deserializeBigInt } from "../src/common/bigint.utils"
import { resolveInboundChainId, isValidChainId } from "../src/common/chain-context"
import { GracefulShutdown } from "../src/common/graceful-shutdown"

// ---------------------------------------------------------------------------
// CQRS bridge: attach is idempotent and returns a working disposer.
// ---------------------------------------------------------------------------

function makeBridge(remoteCommands: string[], remoteEvents: string[]): CqrsBridge {
  const calls: string[] = []
  const bridge = new CqrsBridge({
    service: "svc",
    client: {
      async query(_svc, method) {
        calls.push(`q:${method}`)
        return `remote:${method}`
      },
      async emit(_svc, method) {
        calls.push(`e:${method}`)
      }
    },
    remoteCommands,
    remoteEvents,
    commandKey: (c) => (c as any).name,
    eventKey: (e) => (e as any).name
  })
  ;(bridge as any).calls = calls
  return bridge
}

test("attachToCommandBus is idempotent (no double-wrap)", async () => {
  const bridge = makeBridge([], [])
  let executions = 0
  const bus: CqrsLikeBus = {
    async execute(_cmd) {
      executions++
      return "local"
    }
  }

  const d1 = bridge.attachToCommandBus(bus)
  const wrappedOnce = bus.execute
  const d2 = bridge.attachToCommandBus(bus)
  // Second attach must be a no-op: same wrapper still installed.
  assert.equal(bus.execute, wrappedOnce)

  const out = await bus.execute!({ name: "LocalCmd" })
  assert.equal(out, "local")
  // Despite two attach calls, the local handler ran exactly once (not wrapped twice).
  assert.equal(executions, 1)

  // Disposer restores the original; a no-op disposer from the 2nd attach changes nothing.
  d2()
  assert.equal(bus.execute, wrappedOnce)
  d1()
  assert.notEqual(bus.execute, wrappedOnce)
})

test("attachToEventBus preserves sync publish (does not turn it async)", () => {
  const bridge = makeBridge([], ["RemoteEvt"])
  let published: unknown
  const bus: CqrsLikeBus = {
    publish(evt) {
      published = evt
    } // sync, returns void
  }

  const detach = bridge.attachToEventBus(bus)
  // Non-forwarded event: must stay synchronous (return value is void, not a Promise).
  const ret = bus.publish!({ name: "LocalEvt" })
  assert.equal(ret, undefined)
  assert.equal((ret as any) instanceof Promise, false)
  assert.deepEqual(published, { name: "LocalEvt" })

  // Forwarded event still goes remote (async).
  const remoteRet = bus.publish!({ name: "RemoteEvt" })
  assert.equal(remoteRet instanceof Promise, true)

  detach()
  // After detach the original sync publish is restored.
  assert.equal(typeof bus.publish, "function")
})

test("attachToEventBus is idempotent and disposer restores original", async () => {
  const bridge = makeBridge([], ["RemoteEvt"])
  const original = (_evt: unknown): void => {}
  const bus: CqrsLikeBus = { publish: original }

  const d1 = bridge.attachToEventBus(bus)
  const wrappedOnce = bus.publish
  const d2 = bridge.attachToEventBus(bus)
  assert.equal(bus.publish, wrappedOnce)

  d2() // no-op disposer from 2nd attach
  assert.equal(bus.publish, wrappedOnce)
  d1()
  assert.equal(bus.publish, original)
})

// ---------------------------------------------------------------------------
// BigInt depth / cycle guard.
// ---------------------------------------------------------------------------

test("serializeBigInt throws on deeply nested input instead of overflowing", () => {
  let deep: any = {}
  const root = deep
  for (let i = 0; i < 5000; i++) {
    deep.child = {}
    deep = deep.child
  }
  assert.throws(() => serializeBigInt(root), /depth/i)
})

test("deserializeBigInt throws on deeply nested input instead of overflowing", () => {
  let deep: any = {}
  const root = deep
  for (let i = 0; i < 5000; i++) {
    deep.child = {}
    deep = deep.child
  }
  assert.throws(() => deserializeBigInt(root), /depth/i)
})

test("serializeBigInt detects circular references", () => {
  const a: any = { name: "a" }
  a.self = a
  assert.throws(() => serializeBigInt(a), /circular/i)
})

test("serializeBigInt still handles normal shallow input (incl. bigint)", () => {
  const out = serializeBigInt({ id: 7n, list: [1n, { x: 2n }] })
  assert.equal(out.id, "@@nevo:bigint:7")
  assert.equal(out.list[1].x, "@@nevo:bigint:2")
  // Round-trips cleanly.
  assert.equal(deserializeBigInt(out).id, 7n)
})

// ---------------------------------------------------------------------------
// Chain-id validation.
// ---------------------------------------------------------------------------

test("resolveInboundChainId honors a sane bounded id", () => {
  const id = "0190b5d2-1c3f-7abc-8def-0123456789ab"
  assert.equal(resolveInboundChainId(id), id)
  assert.equal(isValidChainId(id), true)
})

test("resolveInboundChainId rejects over-long ids and mints a fresh one", () => {
  const evil = "x".repeat(5000)
  const out = resolveInboundChainId(evil)
  assert.notEqual(out, evil)
  assert.ok(out.length <= 64)
  assert.equal(isValidChainId(evil), false)
})

test("resolveInboundChainId rejects ids with control / injection chars", () => {
  for (const bad of ["has space", "line\nbreak", "semi;colon", "<script>", "tab\tchar"]) {
    assert.equal(isValidChainId(bad), false)
    assert.notEqual(resolveInboundChainId(bad), bad)
  }
})

test("resolveInboundChainId mints a fresh id for non-string / empty input", () => {
  assert.equal(typeof resolveInboundChainId(undefined), "string")
  assert.ok(resolveInboundChainId(undefined).length > 0)
  assert.ok(resolveInboundChainId("").length > 0)
  assert.notEqual(resolveInboundChainId(""), "")
})

// ---------------------------------------------------------------------------
// Graceful shutdown: tasks started during the drain window are tracked.
// ---------------------------------------------------------------------------

test("drain waits for tasks started during the drain window", async () => {
  const gs = new GracefulShutdown()
  const order: string[] = []

  let resolveSecond!: () => void
  const second = new Promise<void>((r) => {
    resolveSecond = r
  })

  // First task: completes quickly, then (during the drain window) spawns a
  // second tracked task. Before the fix, the second task was NOT tracked
  // (trackInflight returned early once shuttingDown), so drain could finish
  // before it did.
  let resolveFirst!: () => void
  const first = new Promise<void>((r) => {
    resolveFirst = r
  })
  const firstTracked = gs.trackInflight(
    first.then(() => {
      order.push("first")
    })
  )

  const shutdownP = gs.shutdown(2000).then(() => order.push("shutdown-done"))

  // We are now in the drain window (shuttingDown === true).
  assert.equal(gs.isShuttingDown(), true)

  // Spawn a task during the drain window.
  const secondTracked = gs.trackInflight(
    second.then(() => {
      order.push("second")
    })
  )

  // Complete the first task; drain must still wait for the second.
  resolveFirst()
  await firstTracked
  // Give the event loop a tick so shutdown could (wrongly) complete if buggy.
  await Promise.resolve()
  await Promise.resolve()
  assert.deepEqual(order, ["first"], "shutdown must not finish while drain-window task is inflight")

  // Now finish the second task.
  resolveSecond()
  await secondTracked
  await shutdownP
  assert.deepEqual(order, ["first", "second", "shutdown-done"])
})

test("drain resolver is not reused across drains", async () => {
  const gs = new GracefulShutdown()
  // No inflight -> drain returns immediately and leaves no stale resolver.
  await gs.drain(50)
  // A second drain with inflight work should still resolve cleanly via completion.
  let release!: () => void
  const work = gs.trackInflight(
    new Promise<void>((r) => {
      release = r
    })
  )
  const d = gs.drain(2000)
  release()
  await work
  await d // must resolve via completion, not a reused/settled resolver
  assert.ok(true)
})

test("direct drain() resolves via completion well before its timeout", async () => {
  // drain() is public and may be awaited without shutdown() flipping
  // shuttingDown. Completion of the last inflight task must resolve it via
  // completion, not force a wait until the (here, very large) timeout.
  const gs = new GracefulShutdown()
  let release!: () => void
  const work = gs.trackInflight(
    new Promise<void>((r) => {
      release = r
    })
  )
  const started = Date.now()
  const d = gs.drain(60_000)
  release()
  await work
  await d
  assert.ok(Date.now() - started < 5_000, "drain must resolve on completion, not on timeout")
})
