import { test } from "node:test"
import assert from "node:assert/strict"
import { GracefulShutdown } from "../src/common/graceful-shutdown"
import { ClientRuntime } from "../src/common/client-runtime"
import { JsonCodec } from "../src/common/codec"

test("runs hooks LIFO and drains inflight", async () => {
  const gs = new GracefulShutdown()
  const order: string[] = []
  gs.register("first", () => {
    order.push("first")
  })
  gs.register("second", () => {
    order.push("second")
  })

  const work = gs.trackInflight(
    new Promise<void>((resolve) =>
      setTimeout(() => {
        order.push("work")
        resolve()
      }, 30)
    )
  )
  const shutdownP = gs.shutdown(1000)
  await Promise.all([work, shutdownP])
  assert.deepEqual(order, ["work", "second", "first"])
})

test("isShuttingDown turns true", async () => {
  const gs = new GracefulShutdown()
  assert.equal(gs.isShuttingDown(), false)
  const p = gs.shutdown(10)
  assert.equal(gs.isShuttingDown(), true)
  await p
})

test("trackInflight rejects new work once shutdown started", async () => {
  const gs = new GracefulShutdown()
  const p = gs.shutdown(10)
  assert.throws(() => gs.trackInflight(() => Promise.resolve("late")), /shutdown/)
  await p
})

test("a task spawned during shutdown is rejected before it can start", async () => {
  const gs = new GracefulShutdown()
  let hookDone = false
  gs.register("close", () => {
    hookDone = true
  })
  await gs.shutdown(10)
  assert.equal(hookDone, true)
  let started = false
  assert.throws(
    () =>
      gs.trackInflight(() => {
        started = true
        return new Promise<void>(() => {})
      }),
    /shutdown/
  )
  assert.equal(started, false, "the task must never start once shutdown began")
})

test("the client runtime does not dispatch a request that races shutdown", async () => {
  const r = new ClientRuntime({ codec: new JsonCodec(), devtools: false } as any, { transport: "test" })
  await r.close(10)
  let sent = 0
  await assert.rejects(
    () =>
      r.query({
        serviceName: "svc",
        method: "m",
        params: {},
        send: async () => {
          sent++
          return { uuid: "u", method: "m", params: { result: "late" }, meta: {} }
        }
      }),
    /shutdown/
  )
  assert.equal(sent, 0, "the pipeline must not start, so the send never dispatches")
})
