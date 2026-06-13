import { test } from "node:test"
import assert from "node:assert/strict"
import { WsSignalRouter } from "../src/transports/websocket/ws.signal-router.decorator"
import { NevoWsClient } from "../src/transports/websocket/nevo-ws.client"

class UserService {
  async ping(params: any) {
    return { pong: params }
  }
}

test("ws router: publish fans out to subscribed clients and skips the RPC handler", async () => {
  class Ctrl {
    svc = new UserService()
  }
  const decorate = WsSignalRouter([UserService], {
    serviceName: "user",
    port: 0,
    devtools: false,
    tracing: { enabled: false }
  })
  decorate(Ctrl)

  const ctrl: any = new Ctrl()
  await ctrl.onModuleInit()
  if (!ctrl.wsHttpServer.listening) {
    await new Promise<void>((resolve) => ctrl.wsHttpServer.once("listening", resolve))
  }
  const port = (ctrl.wsHttpServer.address() as any).port
  const url = `ws://127.0.0.1:${port}`

  const subscriber = new NevoWsClient({ user: url }, { timeoutMs: 5000, devtools: false })
  const publisher = new NevoWsClient({ user: url }, { timeoutMs: 5000, devtools: false })

  try {
    const received = Promise.withResolvers<any>()
    const failTimer = setTimeout(() => received.reject(new Error("publish was not delivered to subscriber")), 4000)

    await subscriber.subscribe("user", "user.updated", undefined, (data) => {
      received.resolve(data)
    })
    await new Promise((resolve) => setTimeout(resolve, 150))

    await publisher.publish("user", "user.updated", { id: 7 })

    const data = await received.promise
    clearTimeout(failTimer)
    assert.deepEqual(data, { id: 7 })
  } finally {
    await subscriber.close(1000)
    await publisher.close(1000)
    await ctrl.onModuleDestroy()
  }
})

test("ws router: publish does not reach clients subscribed to a different method", async () => {
  class Ctrl {
    svc = new UserService()
  }
  const decorate = WsSignalRouter([UserService], {
    serviceName: "user",
    port: 0,
    devtools: false,
    tracing: { enabled: false }
  })
  decorate(Ctrl)

  const ctrl: any = new Ctrl()
  await ctrl.onModuleInit()
  if (!ctrl.wsHttpServer.listening) {
    await new Promise<void>((resolve) => ctrl.wsHttpServer.once("listening", resolve))
  }
  const port = (ctrl.wsHttpServer.address() as any).port
  const url = `ws://127.0.0.1:${port}`

  const subscriber = new NevoWsClient({ user: url }, { timeoutMs: 5000, devtools: false })
  const publisher = new NevoWsClient({ user: url }, { timeoutMs: 5000, devtools: false })

  try {
    let delivered = 0
    await subscriber.subscribe("user", "user.deleted", undefined, () => {
      delivered++
    })
    await new Promise((resolve) => setTimeout(resolve, 150))

    await publisher.publish("user", "user.updated", { id: 8 })
    await new Promise((resolve) => setTimeout(resolve, 300))

    assert.equal(delivered, 0)
  } finally {
    await subscriber.close(1000)
    await publisher.close(1000)
    await ctrl.onModuleDestroy()
  }
})
