import "reflect-metadata"
import assert from "node:assert/strict"
import { test } from "node:test"
import { JsonCodec, maybeDecompress } from "../src/common"
import { bindSignalRouterForTesting } from "../src/signal-router.utils"
import { NatsSignalRouter } from "../src/transports/nats/nats.signal-router.decorator"

test("NATS encodes a fresh reply envelope for equal results", { timeout: 15_000 }, async () => {
  class Service {}
  class Controller {}
  const codec = new JsonCodec()
  const requests = [
    { uuid: "first", method: "get", params: { result: 1 } },
    { uuid: "second", method: "get", params: { result: "1" } },
    { uuid: "third", method: "get", params: { result: 1 } }
  ]
  const responses: unknown[] = []
  const { promise, resolve } = Promise.withResolvers<void>()
  const nc = {
    subscribe: () => ({
      async *[Symbol.asyncIterator]() {
        for (const request of requests) yield { data: codec.encode(request), reply: "inbox" }
        resolve()
      },
      unsubscribe() {}
    }),
    publish(_subject: string, data: Uint8Array, options?: { headers?: { get(name: string): string } }) {
      responses.push(codec.decode(maybeDecompress(data, options?.headers?.get("content-encoding"))))
    }
  }
  NatsSignalRouter(Service, { codec, compression: { enabled: true, threshold: 1 }, responseCache: { enabled: true } })(Controller)
  bindSignalRouterForTesting(Controller)
  const router = Object.assign(new Controller(), {
    __nevoNatsUniversalClient: { ensureConnection: async () => nc },
    handleSignalMessage: async (message: unknown) => message
  }) as Controller & { onModuleInit(): Promise<void>; onModuleDestroy(): Promise<void> }
  await router.onModuleInit()
  try {
    await promise
    assert.deepEqual(responses, requests)
  } finally {
    await router.onModuleDestroy()
  }
})
