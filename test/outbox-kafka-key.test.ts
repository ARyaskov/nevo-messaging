import assert from "node:assert/strict"
import { test } from "node:test"
import type { ClientKafka } from "@nestjs/microservices"
import { of } from "rxjs"
import { JsonCodec } from "../src/common/codec"
import { InMemoryOutboxStore, Outbox } from "../src/common/outbox"
import { NevoKafkaClient } from "../src/transports/kafka/nevo-kafka.client"

test("an ordered outbox sends the same Kafka key for all events in its partition", async () => {
  const records: { key: string; value: Buffer }[] = []
  const kafka = {
    subscribeToResponseOf() {},
    emit(_topic: string, record: { key: string; value: Buffer }) {
      records.push(record)
      return of(undefined)
    }
  } as unknown as ClientKafka
  const codec = new JsonCodec()
  const client = new NevoKafkaClient(kafka, ["catalog"], { codec, discovery: { enabled: false } })
  const outbox = new Outbox(new InMemoryOutboxStore(), client)
  await outbox.enqueue("catalog", "created", {}, { partitionKey: "item:42" })
  await outbox.enqueue("catalog", "updated", {}, { partitionKey: "item:42" })
  assert.deepEqual(await outbox.flushOnce(), { published: 2, failed: 0 })
  assert.deepEqual(
    records.map(({ key }) => key),
    ["item:42", "item:42"]
  )
})
