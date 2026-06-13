import { test } from "node:test"
import assert from "node:assert/strict"
import { createMemoryTransport, MemoryClientBase, MemoryHarness, MemoryTransport } from "../src/transports/memory"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"

test("query routes to a registered handler and returns its value", async () => {
  const t = createMemoryTransport()
  t.registerHandler("user", "user.getById", async (params: any) => {
    return { id: params.id, name: "Eddie" }
  })
  class Caller extends MemoryClientBase {
    constructor() {
      super(t, { serviceName: "frontend" })
    }
    fetch(id: bigint) {
      return this.query<{ id: bigint; name: string }>("user", "user.getById", { id })
    }
  }
  const c = new Caller()
  const u = await c.fetch(42n)
  assert.equal(u.name, "Eddie")
  assert.equal(t.harness.calls.length, 1)
  assert.equal(t.harness.calls[0].kind, "query")
})

test("query without handler throws METHOD_NOT_FOUND", async () => {
  const t = createMemoryTransport()
  class C extends MemoryClientBase {
    constructor() {
      super(t, { serviceName: "f" })
    }
    p() {
      return this.query("nope", "x", {})
    }
  }
  await assert.rejects(
    () => new C().p(),
    (err) => {
      assert.ok(err instanceof MessagingError)
      assert.equal((err as MessagingError).code, ErrorCode.METHOD_NOT_FOUND)
      return true
    }
  )
})

test("emit is fire-and-forget — handler runs asynchronously", async () => {
  const t = createMemoryTransport()
  let received: any = null
  let handlerStartedDuringEmit = false
  t.registerHandler("audit", "user.created", async (p: any) => {
    // If this runs synchronously inside `emit()`, `emitCompleted` would still be false.
    if (!emitCompleted) handlerStartedDuringEmit = true
    received = p
  })
  let emitCompleted = false
  class C extends MemoryClientBase {
    constructor() {
      super(t, { serviceName: "user" })
    }
    send() {
      return this.emit("audit", "user.created", { id: 1n })
    }
  }
  await new C().send()
  emitCompleted = true
  // Let the queued microtask + handler complete.
  await new Promise((r) => setImmediate(r))
  assert.deepEqual(received, { id: 1n })
  assert.equal(handlerStartedDuringEmit, false, "emit must not block on the handler")
})

test("publish fans out to subscribers", async () => {
  const t = createMemoryTransport()
  const received: number[] = []
  t.subscribe<{ n: number }>("metrics", "ping", undefined, async (msg) => {
    received.push(msg.n)
  })
  t.subscribe<{ n: number }>("metrics", "ping", undefined, async (msg) => {
    received.push(msg.n * 10)
  })
  class P extends MemoryClientBase {
    constructor() {
      super(t, { serviceName: "src" })
    }
    fire(n: number) {
      return this.publish("metrics", "ping", { n })
    }
  }
  await new P().fire(7)
  assert.deepEqual(
    received.sort((a, b) => a - b),
    [7, 70]
  )
})

test("broadcast reaches broadcast listeners + matching subscribers", async () => {
  const t = createMemoryTransport()
  let bcasts = 0
  let pubs = 0
  t.subscribeBroadcast(async () => {
    bcasts++
  })
  t.subscribe("anything", "system.status", undefined, async () => {
    pubs++
  })
  class C extends MemoryClientBase {
    constructor() {
      super(t, { serviceName: "x" })
    }
    go() {
      return this.broadcast("system.status", { ok: true })
    }
  }
  await new C().go()
  assert.equal(bcasts, 1)
  assert.equal(pubs, 1)
})

test("wildcard subscriptions match dotted method names", async () => {
  const t = createMemoryTransport()
  const received: string[] = []
  t.subscribe("user", "user.event.>", undefined, async (_msg, ctx) => {
    // Wildcard tracking — the dispatcher only routes matching methods, so the
    // count itself is the assertion.
    received.push("hit")
  })
  class P extends MemoryClientBase {
    constructor() {
      super(t, { serviceName: "x" })
    }
    fire(m: string) {
      return this.publish("user", m, {})
    }
  }
  await new P().fire("user.event.created")
  await new P().fire("user.event.deleted")
  await new P().fire("user.changed") // does NOT match `user.event.>`
  assert.equal(received.length, 2)
})

test("MemoryHarness.failNext injects a single error", async () => {
  const t = new MemoryTransport()
  t.registerHandler("svc", "fail", async () => "ok")
  t.harness.failNext("svc", "fail", new Error("simulated"))
  class C extends MemoryClientBase {
    constructor() {
      super(t, { serviceName: "x" })
    }
    call() {
      return this.query("svc", "fail", {})
    }
  }
  await assert.rejects(() => new C().call(), /simulated/)
  // The injection is consumed — the next call succeeds.
  const v = await new C().call()
  assert.equal(v, "ok")
})

test("MemoryHarness.delayBy applies latency", async () => {
  const t = new MemoryTransport()
  t.registerHandler("svc", "slow", async () => "ok")
  t.harness.delayBy("svc", "slow", 30)
  class C extends MemoryClientBase {
    constructor() {
      super(t, { serviceName: "x" })
    }
    call() {
      return this.query("svc", "slow", {})
    }
  }
  const t0 = Date.now()
  await new C().call()
  assert.ok(Date.now() - t0 >= 25, "delay was not applied")
})

test("harness records call kind, service, method, uuid", async () => {
  const t = new MemoryTransport()
  t.registerHandler("a", "m", async () => 1)
  class C extends MemoryClientBase {
    constructor() {
      super(t, { serviceName: "x" })
    }
    q() {
      return this.query("a", "m", { hello: "world" })
    }
  }
  await new C().q()
  const [call] = t.harness.calls
  assert.equal(call.kind, "query")
  assert.equal(call.serviceName, "a")
  assert.equal(call.method, "m")
  assert.ok(call.uuid)
  assert.deepEqual(call.params, { hello: "world" })
})

test("reset() wipes handlers and history", async () => {
  const t = createMemoryTransport({
    handlers: { svc: { m: async () => "first" } }
  })
  class C extends MemoryClientBase {
    constructor() {
      super(t, { serviceName: "x" })
    }
    q() {
      return this.query("svc", "m", {})
    }
  }
  assert.equal(await new C().q(), "first")
  assert.equal(t.harness.calls.length, 1)
  t.reset()
  assert.equal(t.harness.calls.length, 0)
  // The handler was removed too — the next call has nothing to route to.
  await assert.rejects(() => new C().q())
})
