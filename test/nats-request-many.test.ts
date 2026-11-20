import assert from "node:assert/strict"
import { test } from "node:test"
import type { NatsConnection } from "@nats-io/nats-core"
import { JsonCodec } from "../src/common"
import { NevoNatsClient } from "../src/transports/nats/nevo-nats.client"

test("requestMany awaits the NATS iterator and unwraps replies", async () => {
  const codec = new JsonCodec()
  const replies = [{}, { params: { result: "error", error: { code: 1, message: "failed" } } }, { params: { result: "error" } }]
  const connection = {
    status: async function* () {},
    drain: async () => {},
    async requestMany(subject: string, _payload: Uint8Array, options: { strategy: string; maxMessages: number }) {
      assert.equal(subject, "catalog-events.scatter")
      assert.equal(options.strategy, "count")
      assert.equal(options.maxMessages, replies.length)
      return (async function* () {
        for (const reply of replies) yield { data: codec.encode(reply) }
      })()
    }
  } as unknown as NatsConnection
  const client = new NevoNatsClient(["catalog"], { codec }, connection)
  try {
    assert.deepEqual(await client.requestMany("catalog", "get", {}, { maxMessages: replies.length }), ["error"])
  } finally {
    await client.close()
  }
})
