import { test } from "node:test"
import assert from "node:assert/strict"
import { GracefulShutdown } from "../src/common/graceful-shutdown"

test("runs hooks LIFO and drains inflight", async () => {
  const gs = new GracefulShutdown()
  const order: string[] = []
  gs.register("first", () => { order.push("first") })
  gs.register("second", () => { order.push("second") })

  const work = gs.trackInflight(new Promise<void>((resolve) => setTimeout(() => { order.push("work"); resolve() }, 30)))
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
