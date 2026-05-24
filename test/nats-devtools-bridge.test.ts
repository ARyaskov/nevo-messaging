import { test } from "node:test"
import assert from "node:assert/strict"
import { EventEmitter } from "node:events"
import { DevToolsBus } from "../src/common/devtools"
import { NatsDevToolsAdapter } from "../src/transports/nats/devtools.adapter"
import { JsonCodec } from "../src/common/codec"

function makeFakeNats(subjectToListeners: Map<string, EventEmitter> = new Map()): any {
  const codec = new JsonCodec()
  return {
    subscribe(subject: string) {
      let ee = subjectToListeners.get(subject)
      if (!ee) { ee = new EventEmitter(); subjectToListeners.set(subject, ee) }
      const queue: any[] = []
      let resolveNext: ((v: any) => void) | null = null
      let done = false

      ee.on("msg", (data) => {
        if (done) return
        if (resolveNext) { const r = resolveNext; resolveNext = null; r({ value: { data }, done: false }) }
        else queue.push(data)
      })

      const sub: any = {
        unsubscribe() { done = true; if (resolveNext) { const r = resolveNext; resolveNext = null; r({ value: undefined, done: true }) } },
        [Symbol.asyncIterator]() { return this },
        next() {
          if (done) return Promise.resolve({ value: undefined, done: true })
          if (queue.length) return Promise.resolve({ value: { data: queue.shift() }, done: false })
          return new Promise((res) => { resolveNext = res })
        }
      }
      return sub
    },
    publish(subject: string, data: any) {
      let ee = subjectToListeners.get(subject)
      if (!ee) { ee = new EventEmitter(); subjectToListeners.set(subject, ee) }
      ee.emit("msg", data instanceof Uint8Array ? data : Buffer.from(data))
    },
    drain: async () => {}
  }
}

test("bridges local events to nats and remote back into bus", async () => {
  const codec = new JsonCodec()
  const subjectMap = new Map<string, EventEmitter>()
  const ncA = makeFakeNats(subjectMap)
  const ncB = makeFakeNats(subjectMap)

  const busA = new DevToolsBus({ originId: "instance-a" })
  const busB = new DevToolsBus({ originId: "instance-b" })

  const adapterA = new NatsDevToolsAdapter(ncA, { bus: busA, codec })
  const adapterB = new NatsDevToolsAdapter(ncB, { bus: busB, codec })
  const detachA = await adapterA.attach()
  const detachB = await adapterB.attach()

  const received: any[] = []
  busB.on((e) => { if (e.method) received.push(e) })

  busA.publish({ ts: Date.now(), type: "request", method: "foo", service: "user", status: "ok" })

  await new Promise((r) => setTimeout(r, 50))

  assert.equal(received.length, 1)
  assert.equal(received[0].method, "foo")
  assert.equal(received[0].origin, "instance-a")

  busA.publish({ ts: Date.now(), type: "request", method: "loop", origin: "instance-a" })
  await new Promise((r) => setTimeout(r, 20))
  const allLocalA = busA.recent(100).map((e) => e.method).filter(Boolean)
  assert.ok(allLocalA.includes("loop"))

  await detachA()
  await detachB()
})

test("event from same origin is not ingested twice", async () => {
  const bus = new DevToolsBus({ originId: "x" })
  const evt = { ts: Date.now(), type: "request" as const, method: "m", origin: "x" }
  bus.ingestRemote(evt)
  assert.equal(bus.recent().length, 0)
})
