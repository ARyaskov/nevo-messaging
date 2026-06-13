import { test, mock } from "node:test"
import assert from "node:assert/strict"
import { NevoKafkaClient } from "../src/transports/kafka/nevo-kafka.client"
import { JsonCodec } from "../src/common/codec"

// These tests exercise the consumer-side logic of NevoKafkaClient against a faked
// kafkajs. We never connect to a broker: the fake consumer just captures the
// `eachMessage` callback registered by `consumer.run(...)` so the test can drive
// it directly with hand-crafted messages, and we hand it a `pause` stub matching
// kafkajs' contract (pause() pauses the partition and returns a resume thunk).

interface FakeConsumer {
  _eachMessage?: (payload: any) => Promise<void>
  _subscribedTopics: string[]
  _committed: any[]
  connect(): Promise<void>
  disconnect(): Promise<void>
  subscribe(opts: { topic: string }): Promise<void>
  run(opts: { eachMessage: (payload: any) => Promise<void> }): Promise<void>
  commitOffsets(offsets: any[]): Promise<void>
  pause(tp: any): () => void
  resume(tp: any): void
}

function makeFakeConsumer(): FakeConsumer {
  const consumer: FakeConsumer = {
    _eachMessage: undefined,
    _subscribedTopics: [],
    _committed: [],
    async connect() {},
    async disconnect() {},
    async subscribe({ topic }) {
      consumer._subscribedTopics.push(topic)
    },
    async run({ eachMessage }) {
      consumer._eachMessage = eachMessage
    },
    async commitOffsets(offsets) {
      consumer._committed.push(...offsets)
    },
    pause() {
      return () => {}
    },
    resume() {}
  }
  return consumer
}

function makeFakeKafka() {
  const consumers: FakeConsumer[] = []
  return {
    consumers,
    consumer() {
      const c = makeFakeConsumer()
      consumers.push(c)
      return c
    },
    producer() {
      return { async connect() {}, async disconnect() {}, async send() {}, async sendBatch() {} }
    }
  }
}

const SILENT_LOGGER: any = {
  trace() {},
  debug() {},
  info() {},
  warn() {},
  error() {},
  fatal() {},
  child() {
    return SILENT_LOGGER
  }
}

function makeClient(fakeKafka: ReturnType<typeof makeFakeKafka>, codec: JsonCodec): NevoKafkaClient {
  const fakeClientKafka: any = { subscribeToResponseOf() {}, emit() {}, send() {} }
  const client = new NevoKafkaClient(fakeClientKafka, ["svc"], {
    discovery: { enabled: false },
    devtools: false,
    codec,
    logger: SILENT_LOGGER,
    retry: { baseMs: 5, maxMs: 10, jitter: false }
  } as any)
  // Swap the real (unconnected) kafkajs instance for our fake so subscribe() builds
  // fake consumers whose eachMessage we can invoke by hand.
  ;(client as any).sharedKafkaForSubs = fakeKafka
  return client
}

function encodeMsg(codec: JsonCodec, method: string, params: any = {}, meta: any = {}): Buffer {
  return Buffer.from(codec.encode({ uuid: "u1", method, params, meta }))
}

test("plain subscribe: a manual-ack handler error schedules a resume instead of leaving the partition paused", async () => {
  const codec = new JsonCodec()
  const fakeKafka = makeFakeKafka()
  const client = makeClient(fakeKafka, codec)
  try {
    let handlerCalls = 0
    await client.subscribe("svc", "doit", { ack: true, maxDeliveryAttempts: 3 }, async () => {
      handlerCalls++
      throw new Error("boom")
    })

    const consumer = fakeKafka.consumers[0]
    const topic = consumer._subscribedTopics[0]

    let pauseCalls = 0
    let resumeCalls = 0
    const pause = () => {
      pauseCalls++
      return () => {
        resumeCalls++
      }
    }

    mock.timers.enable({ apis: ["setTimeout"] })
    try {
      await consumer._eachMessage!({
        topic,
        partition: 0,
        message: { value: encodeMsg(codec, "doit", { n: 1 }), offset: "0", headers: {} },
        pause
      })

      assert.equal(handlerCalls, 1)
      assert.equal(pauseCalls, 1, "the partition should be paused on handler failure")
      assert.equal(resumeCalls, 0, "resume must be deferred behind a backoff, not called inline")

      mock.timers.tick(60_000)
      assert.equal(resumeCalls, 1, "the partition must be resumed after the backoff (no permanent stall)")
    } finally {
      mock.timers.reset()
    }
  } finally {
    await client.close()
  }
})

test("sticky subscribe: a message matching no handler does not retain a deliveryCounts entry", async () => {
  const codec = new JsonCodec()
  const fakeKafka = makeFakeKafka()
  const client = makeClient(fakeKafka, codec)
  try {
    await client.subscribe("svc", "wanted", { groupId: "g1", ack: true }, async () => {})

    const consumer = fakeKafka.consumers[0]
    const topic = consumer._subscribedTopics[0]
    const group = (client as any).stickyGroups.get("g1")

    // A message whose method matches none of the registered handlers must be
    // dropped without leaving a counter behind (the leak this guards against).
    await consumer._eachMessage!({
      topic,
      partition: 0,
      message: { value: encodeMsg(codec, "unwanted", {}), offset: "7", headers: {} },
      pause: () => () => {}
    })

    assert.equal(group.deliveryCounts.size, 0, "non-matching messages must not accumulate counter entries")
  } finally {
    await client.close()
  }
})

test("sticky subscribe: a manual-ack handler error schedules a resume and retains the counter for retry", async () => {
  const codec = new JsonCodec()
  const fakeKafka = makeFakeKafka()
  const client = makeClient(fakeKafka, codec)
  try {
    await client.subscribe("svc", "wanted", { groupId: "g2", ack: true, maxDeliveryAttempts: 3 }, async () => {
      throw new Error("boom")
    })

    const consumer = fakeKafka.consumers[0]
    const topic = consumer._subscribedTopics[0]
    const group = (client as any).stickyGroups.get("g2")

    let pauseCalls = 0
    let resumeCalls = 0
    const pause = () => {
      pauseCalls++
      return () => {
        resumeCalls++
      }
    }

    mock.timers.enable({ apis: ["setTimeout"] })
    try {
      await consumer._eachMessage!({
        topic,
        partition: 0,
        message: { value: encodeMsg(codec, "wanted", {}), offset: "3", headers: {} },
        pause
      })

      assert.equal(pauseCalls, 1, "the partition should be paused on handler failure")
      assert.equal(resumeCalls, 0, "resume must be deferred behind a backoff")
      assert.equal(group.deliveryCounts.size, 1, "a retrying message must keep its counter so attempts accumulate")

      mock.timers.tick(60_000)
      assert.equal(resumeCalls, 1, "the partition must be resumed after the backoff")
    } finally {
      mock.timers.reset()
    }
  } finally {
    await client.close()
  }
})
