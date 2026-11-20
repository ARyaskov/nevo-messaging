import assert from "node:assert/strict"
import { test } from "node:test"
import type { NatsConnection } from "@nats-io/nats-core"
import { JsonCodec } from "../src/common"
import { NevoNatsClient } from "../src/transports/nats/nevo-nats.client"

test("query stream releases its inbox after a publish error and after an end marker", { timeout: 5000 }, async () => {
  const codec = new JsonCodec()
  let active = 0
  let failPublish = true
  const nc = {
    status: async function* () {},
    drain: async () => {},
    publish() {
      if (failPublish) throw new Error("publish failed")
    },
    subscribe() {
      active++
      return {
        unsubscribe: () => active--,
        async *[Symbol.asyncIterator]() {
          yield { data: codec.encode({ meta: { headers: { "nevo-stream-end": "1" } } }) }
        }
      }
    }
  } as unknown as NatsConnection
  const client = new NevoNatsClient(["catalog"], { codec }, nc)
  try {
    await assert.rejects(
      client.subscribeQuery("catalog", "get", {}, () => {}),
      /publish failed/
    )
    assert.equal(active, 0)
    failPublish = false
    const { promise, resolve } = Promise.withResolvers<void>()
    await client.subscribeQuery("catalog", "get", {}, () => {}, resolve)
    await promise
    assert.equal(active, 0)
  } finally {
    await client.close()
  }
})
