import assert from "node:assert/strict"
import { test } from "node:test"
import type { NatsConnection } from "@nats-io/nats-core"
import { JsonCodec } from "../src/common"
import { NevoNatsClient } from "../src/transports/nats/nevo-nats.client"

for (const operation of ["emitBatch", "publishBatch"] as const) {
  test(`NATS ${operation} flushes bounded windows in input order`, async () => {
    let encodesSinceFlush = 0
    let peakEncodes = 0
    class TrackingCodec extends JsonCodec {
      override encode(value: unknown) {
        peakEncodes = Math.max(peakEncodes, ++encodesSinceFlush)
        return super.encode(value)
      }
    }
    const codec = new TrackingCodec()
    const received: number[] = []
    const nc = {
      status: async function* () {},
      drain: async () => {},
      flush: async () => {
        encodesSinceFlush = 0
      },
      publish(_subject: string, payload: Uint8Array) {
        received.push(codec.decode<{ params: number }>(payload).params)
      }
    } as unknown as NatsConnection
    const client = new NevoNatsClient(["catalog"], { codec, compression: { enabled: true, async: true, threshold: 10_000 } }, nc)
    try {
      const values = Array.from({ length: 73 }, (_, i) => i)
      await client[operation](values.map((params) => ({ serviceName: "catalog", method: "changed", params })))
      assert.deepEqual(received, values)
      assert.equal(peakEncodes, 16)
      assert.equal(encodesSinceFlush, 0)
    } finally {
      await client.close()
    }
  })
}
