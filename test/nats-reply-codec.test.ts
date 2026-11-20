import assert from "node:assert/strict"
import { test } from "node:test"
import type { NatsConnection } from "@nats-io/nats-core"
import { JsonCodec, MessagePackCodec } from "../src/common"
import { NevoNatsClient } from "../src/transports/nats/nevo-nats.client"

test("a JSON NATS client decodes a cached MessagePack reply by its content type", async () => {
  const msgpack = new MessagePackCodec()
  const result = { id: 2n ** 80n, literal: "@@nevo:bigint:123" }
  const connection = {
    status: async function* () {},
    drain: async () => {},
    async request() {
      return { data: msgpack.encode({ params: { result } }), headers: new Map([["content-type", "application/msgpack"]]) }
    }
  } as unknown as NatsConnection
  const client = new NevoNatsClient(["catalog"], { codec: new JsonCodec() }, connection)
  try {
    assert.deepEqual(await client.query("catalog", "get", {}), result)
  } finally {
    await client.close()
  }
})
