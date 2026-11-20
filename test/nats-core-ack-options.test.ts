import assert from "node:assert/strict"
import { test } from "node:test"
import { ErrorCode } from "../src/common"
import { NevoNatsClient } from "../src/transports/nats/nevo-nats.client"

test("Core NATS rejects durable subscription options before connecting", async () => {
  const client = new NevoNatsClient(["catalog"])
  for (const options of [{ ack: true }, { durableKey: "catalog-events" }]) {
    await assert.rejects(
      client.subscribe("catalog", "changed", options, () => {}),
      { code: ErrorCode.BAD_REQUEST }
    )
  }
})
