import { test } from "node:test"
import assert from "node:assert/strict"
import { ClientRuntime } from "../src/common/client-runtime"
import { JsonCodec } from "../src/common/codec"

function stillPending(p: Promise<unknown>, ms = 20): Promise<boolean> {
  return Promise.race([
    p.then(
      () => false,
      () => false
    ),
    new Promise<boolean>((r) => setTimeout(() => r(true), ms))
  ])
}

test("two overlapping close() calls share ONE drain; the in-flight response is preserved", async () => {
  const rt = new ClientRuntime({ codec: new JsonCodec(), devtools: false } as Record<string, never>, { transport: "test" })
  let release!: () => void
  const gate = new Promise<void>((r) => {
    release = r
  })

  // A live in-flight query is running when shutdown begins.
  const q = rt.query<{ ok: number }>({
    serviceName: "svc",
    method: "m",
    params: {},
    send: async () => {
      await gate
      return { uuid: "u", method: "m@1", params: { result: { ok: 1 } }, meta: {} }
    }
  })
  await new Promise((r) => setTimeout(r, 20))

  let teardowns = 0
  rt.shutdown.register("teardown", () => {
    teardowns++
  })

  const close1 = rt.close(5000)
  const close2 = rt.close(5000)

  // The old defect: the second close() returned at once, tearing down agents
  // while the first was still draining. It must instead await the same drain.
  const pendingWhileDraining = await stillPending(close2)
  assert.equal(pendingWhileDraining, true, "the second close() must still await the shared shutdown sequence")

  release()
  await Promise.all([close1, close2, q])

  assert.equal(teardowns, 1, "exactly one shutdown sequence (one drain, one hook run) for both callers")
  assert.deepEqual(await q, { ok: 1 }, "the in-flight response must survive the drain and teardown")

  // No further work may start after the (shared) shutdown completed.
  assert.throws(() => rt.shutdown.trackInflight(() => Promise.resolve("late")), /shutdown/)
})
