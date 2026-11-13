import { test } from "node:test"
import assert from "node:assert/strict"
import { typed } from "../src/typed-client"

interface SampleContract {
  "user.getById": { params: { id: string }; result: { id: string; name: string } }
  "user.touch": { params: { id: string }; result: void }
}

interface RecordedCall {
  kind: "query" | "emit"
  opts?: unknown
}

// The fake mimics the real runtime contract: a signal aborted before dispatch
// rejects instead of completing (client-runtime.ts runClientPipeline does the
// same via CANCELLED).
function makeSignalAwareClient(calls: RecordedCall[]) {
  return {
    async query<T = unknown>(_s: string, _m: string, _p: unknown, opts?: { signal?: AbortSignal }): Promise<T> {
      calls.push({ kind: "query", opts })
      if (opts?.signal?.aborted) throw new Error("Request aborted")
      return { id: "1", name: "Ada" } as T
    },
    async emit(_s: string, _m: string, _p: unknown, opts?: { signal?: AbortSignal }): Promise<void> {
      calls.push({ kind: "emit", opts })
      if (opts?.signal?.aborted) throw new Error("Emit aborted")
    }
  }
}

test("typed query accepts a literal { signal } option and forwards the same AbortSignal", async () => {
  const calls: RecordedCall[] = []
  const api = typed<SampleContract>(makeSignalAwareClient(calls))
  const controller = new AbortController()

  // The object literal below is also the compile-time check: without `signal`
  // on TypedQueryOptions, tsc fails with an excess-property error here.
  const user = await api.query("user", "user.getById", { id: "1" }, { signal: controller.signal })
  assert.deepEqual(user, { id: "1", name: "Ada" })
  assert.equal(calls.length, 1)
  assert.equal((calls[0].opts as { signal?: AbortSignal } | undefined)?.signal, controller.signal)
})

test("typed emit accepts a literal { signal } option and forwards the same AbortSignal", async () => {
  const calls: RecordedCall[] = []
  const api = typed<SampleContract>(makeSignalAwareClient(calls))
  const controller = new AbortController()

  await api.emit("user", "user.touch", { id: "1" }, { signal: controller.signal })
  assert.equal(calls.length, 1)
  assert.equal((calls[0].opts as { signal?: AbortSignal } | undefined)?.signal, controller.signal)
})

test("a pre-aborted signal forwarded through the facade reaches cancellation on query", async () => {
  const calls: RecordedCall[] = []
  const api = typed<SampleContract>(makeSignalAwareClient(calls))
  const controller = new AbortController()
  controller.abort()

  await assert.rejects(api.query("user", "user.getById", { id: "1" }, { signal: controller.signal }), /Request aborted/)
  assert.equal(calls.length, 1)
})

test("a pre-aborted signal forwarded through the facade reaches cancellation on emit", async () => {
  const calls: RecordedCall[] = []
  const api = typed<SampleContract>(makeSignalAwareClient(calls))
  const controller = new AbortController()
  controller.abort()

  await assert.rejects(api.emit("user", "user.touch", { id: "1" }, { signal: controller.signal }), /Emit aborted/)
  assert.equal(calls.length, 1)
})
