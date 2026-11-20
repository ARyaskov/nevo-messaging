import assert from "node:assert/strict"
import { test } from "node:test"
import type { NatsConnection } from "@nats-io/nats-core"
import { ErrorCode, JsonCodec } from "../src/common"
import { NevoNatsClient } from "../src/transports/nats/nevo-nats.client"

test("query stream reports remote errors after yielding successful data", { timeout: 5000 }, async () => {
  const codec = new JsonCodec()
  const nc = {
    status: async function* () {},
    drain: async () => {},
    publish() {},
    subscribe() {
      return {
        unsubscribe() {},
        async *[Symbol.asyncIterator]() {
          yield { data: codec.encode({ params: { result: "error" } }) }
          yield { data: codec.encode({ params: { result: "error", error: { code: ErrorCode.BAD_REQUEST, message: "invalid item" } } }) }
        }
      }
    }
  } as unknown as NatsConnection
  const client = new NevoNatsClient(["catalog"], { codec }, nc)
  try {
    const items: string[] = []
    const { promise, resolve } = Promise.withResolvers<{ count: number; error?: unknown }>()
    await client.subscribeQuery<string>(
      "catalog",
      "get",
      {},
      (item) => {
        items.push(item)
      },
      resolve
    )
    const summary = await promise
    assert.deepEqual(items, ["error"])
    assert.equal(summary.count, 1)
    assert.match(String(summary.error), /invalid item/)
  } finally {
    await client.close()
  }
})
