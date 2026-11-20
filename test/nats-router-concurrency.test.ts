import "reflect-metadata"
import assert from "node:assert/strict"
import { test } from "node:test"
import { JsonCodec } from "../src/common"
import { bindSignalRouterForTesting } from "../src/signal-router.utils"
import { NatsSignalRouter } from "../src/transports/nats/nats.signal-router.decorator"

test("NATS router runs independent requests within its concurrency limit", { timeout: 5000 }, async () => {
  class Service {}
  class Controller {}
  const codec = new JsonCodec()
  const releaseSlow = Promise.withResolvers<void>()
  const fastReply = Promise.withResolvers<void>()
  const lastReply = Promise.withResolvers<void>()
  const replies: string[] = []
  let active = 0
  let maxActive = 0
  const nc = {
    subscribe: () => ({
      async *[Symbol.asyncIterator]() {
        for (const id of ["slow", "fast", "last"]) yield { data: codec.encode(id), reply: "inbox" }
      },
      unsubscribe() {}
    }),
    publish(_subject: string, data: Uint8Array) {
      const id = codec.decode<string>(data)
      replies.push(id)
      if (id === "fast") fastReply.resolve()
      if (id === "slow") lastReply.resolve()
    }
  }
  NatsSignalRouter(Service, { codec, concurrency: 2 })(Controller)
  bindSignalRouterForTesting(Controller)
  const router = Object.assign(new Controller(), {
    __nevoNatsUniversalClient: { ensureConnection: async () => nc },
    async handleSignalMessage(id: string) {
      maxActive = Math.max(maxActive, ++active)
      if (id === "slow") await releaseSlow.promise
      active--
      return id
    }
  }) as Controller & { onModuleInit(): Promise<void>; onModuleDestroy(): Promise<void> }
  await router.onModuleInit()
  try {
    await fastReply.promise
    assert.equal(replies[0], "fast")
    assert.equal(maxActive, 2)
  } finally {
    releaseSlow.resolve()
    await lastReply.promise
    await router.onModuleDestroy()
  }
})
