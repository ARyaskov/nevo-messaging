import { test } from "node:test"
import assert from "node:assert/strict"
import { BaseMessageController } from "../src/common/base.controller"
import { RedisIdempotencyStore, type IdempotencyRedisLike } from "../src/common/idempotency-store"
import { ErrorCode } from "../src/common/error-code"
import type { MessageResponse, ServiceMethodMapping } from "../src/common/types"

function fakeRedis(): IdempotencyRedisLike & { storage: Map<string, string> } {
  const storage = new Map<string, string>()
  return {
    storage,
    async get(key) {
      return storage.get(key) ?? null
    },
    async set(key, value, opts) {
      if (opts.ifNotExists && storage.has(key)) return null
      storage.set(key, value)
      return "OK"
    },
    async del(key) {
      return storage.delete(key) ? 1 : 0
    }
  } as IdempotencyRedisLike & { storage: Map<string, string> }
}

// Minimal concrete subclass — BaseMessageController is abstract + never extended
// in the repo, so this also asserts the refactor kept it extensible.
class TestController extends BaseMessageController {
  constructor(svc: any, store?: RedisIdempotencyStore<MessageResponse>) {
    const handlers: ServiceMethodMapping = { "echo.run": { serviceMethod: "run" } }
    super("base-svc", [svc], handlers, {
      idempotencyStore: store,
      tracing: { enabled: false },
      devtools: false
    })
  }
  protected extractMessageData(data: any) {
    return { method: data.method, uuid: data.uuid, params: data.params, meta: data.meta }
  }
  async handleMessage(data: any): Promise<MessageResponse> {
    return this.processMessage(data)
  }
}

test("BaseMessageController dedups across two replicas via the shared two-tier runtime (awaited write)", async () => {
  let calls = 0
  // The replica identity lives on the service, not in the payload: identical params
  // are what a real timeout-retry sends, and differing ones are now a conflict.
  class Svc {
    constructor(private readonly replica: string) {}
    async run() {
      calls++
      return { handledBy: this.replica, n: calls }
    }
  }
  const shared = fakeRedis()
  const store = () => new RedisIdempotencyStore<any>({ client: shared, enabled: true, ttlMs: 60_000 })
  const a = new TestController(new Svc("A"), store())
  const b = new TestController(new Svc("B"), store())

  // Same wire-level idempotency key and payload, distinct uuids (a timeout retry).
  const msg = (uuid: string) => ({ method: "echo.run", uuid, params: { job: 1 }, meta: { idempotencyKey: "job-1" } })
  const r1 = await a.handleMessage(msg("uuid-1"))
  const r2 = await b.handleMessage(msg("uuid-2"))

  assert.equal(calls, 1, "handler ran on exactly one replica")
  // Because the distributed write is AWAITED before replica A returns, the result is
  // already in Redis when replica B claims — no re-execution window.
  assert.equal((r1.params.result as any).handledBy, "A")
  assert.equal((r2.params.result as any).handledBy, "A", "replica B served replica A's stored result")
})

test("BaseMessageController rejects one idempotency key reused for a different payload", async () => {
  let calls = 0
  class Svc {
    async run(p: any) {
      calls++
      return { got: p }
    }
  }
  const ctrl = new TestController(new Svc(), new RedisIdempotencyStore<any>({ client: fakeRedis(), enabled: true, ttlMs: 60_000 }))

  const first = await ctrl.handleMessage({ method: "echo.run", uuid: "u1", params: { amount: 10 }, meta: { idempotencyKey: "k" } })
  assert.deepEqual(first.params.result, { got: { amount: 10 } })

  const conflicting = await ctrl.handleMessage({ method: "echo.run", uuid: "u2", params: { amount: 999 }, meta: { idempotencyKey: "k" } })
  assert.equal(conflicting.params.result, "error")
  assert.equal(conflicting.params.error?.code, ErrorCode.IDEMPOTENCY_KEY_CONFLICT)
  assert.equal(calls, 1, "the conflicting request must not execute")

  // The original entry survives: a genuine retry still replays it.
  const retry = await ctrl.handleMessage({ method: "echo.run", uuid: "u3", params: { amount: 10 }, meta: { idempotencyKey: "k" } })
  assert.deepEqual(retry.params.result, { got: { amount: 10 } })
  assert.equal(calls, 1)
})

test("BaseMessageController concurrent same-key requests execute the handler exactly once", async () => {
  let calls = 0
  let release!: () => void
  const gate = new Promise<void>((r) => {
    release = r
  })
  class Svc {
    async run() {
      calls++
      await gate
      return { n: calls }
    }
  }
  const ctrl = new TestController(new Svc(), new RedisIdempotencyStore<MessageResponse>({ client: fakeRedis(), enabled: true, ttlMs: 60_000 }))
  const msg = (uuid: string) => ({ method: "echo.run", uuid, params: {}, meta: { idempotencyKey: "k" } })

  const p1 = ctrl.handleMessage(msg("u1"))
  const p2 = ctrl.handleMessage(msg("u2"))
  await new Promise((r) => setTimeout(r, 25))
  release()
  const [r1, r2] = await Promise.all([p1, p2])

  assert.equal(calls, 1)
  assert.deepEqual(r1.params.result, r2.params.result)
})
