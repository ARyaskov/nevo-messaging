import "reflect-metadata"
import { test } from "node:test"
import assert from "node:assert/strict"
import { gzipSync } from "node:zlib"
import { randomBytes } from "node:crypto"
import { KafkaSignalRouter } from "../src/transports/kafka/kafka.signal-router.decorator"
import { addSignalMetadata } from "../src/signal.decorator"
import { JsonCodec } from "../src/common/codec"

// These tests drive the real KafkaSignalRouter decode path end-to-end without a
// broker: we hand the wrapped `handleSignalMessage` a synthetic kafkajs-shaped
// message and assert the gzip payload round-trips. Compressed payloads inflate
// off the event loop via the async branch and are stashed on the message object
// under a private Symbol; the synchronous extractor then reuses that buffer.

const SILENT_LOGGER: any = {
  trace() {},
  debug() {},
  info() {},
  warn() {},
  error() {},
  fatal() {},
  child() {
    return SILENT_LOGGER
  },
  isLevelEnabled() {
    return false
  }
}

class EchoService {
  echo(params: any) {
    return params
  }
}

// A fresh decorated controller per test so signal metadata does not leak between
// cases. The service is exposed as an instance property so findServiceInstances
// discovers it by type.
function makeController() {
  class Controller {
    svc = new EchoService()

    handleSignalMessage(_data: any): any {}
  }
  // Register the signal directly (not via @Signal) so the wiring is independent
  // of the host toolchain's decorator mode — the test runner emits TC39 standard
  // decorators, under which a legacy method decorator cannot reach the class.
  addSignalMetadata(Controller, "doThing", "echo")
  KafkaSignalRouter(EchoService, { codec: new JsonCodec(), logger: SILENT_LOGGER, debug: false })(Controller)
  return new Controller() as any
}

// Builds a kafkajs-shaped message whose gzip-compressed value carries `payload`.
function gzipMessage(payload: unknown): any {
  const encoded = new JsonCodec().encode(payload)
  const value = gzipSync(Buffer.from(encoded))
  return { value, headers: { "content-encoding": Buffer.from("gzip") } }
}

// Returns the buffer the wrapper stashed under its private Symbol, or null.
function stashedBuffer(message: any): Uint8Array | null {
  for (const sym of Object.getOwnPropertySymbols(message)) {
    const v = message[sym]
    if (v instanceof Uint8Array) return v
  }
  return null
}

test("large gzip payload decodes through the async branch", async () => {
  const big = randomBytes(64 * 1024).toString("hex")
  const payload = { method: "doThing", uuid: "u-big", params: { result: big }, meta: { tenantId: "t1" } }
  const message = gzipMessage(payload)
  assert.ok(message.value.byteLength > 1024)

  const controller = makeController()
  const response = await controller.handleSignalMessage(message)

  // The handler ran, which means the payload decoded correctly end-to-end.
  assert.equal(response.uuid, "u-big")
  assert.equal(response.method, "doThing")
  assert.deepEqual(response.params.result, { result: big })

  // The async branch must have inflated off the loop and stashed the buffer.
  const stashed = stashedBuffer(message)
  assert.ok(stashed, "large payload should be pre-inflated via the async branch")
  assert.equal(new JsonCodec().decode(stashed!).method, "doThing")
})

test("small gzip payload also decodes through the async branch", async () => {
  const payload = { method: "doThing", uuid: "u-small", params: { result: "hi" }, meta: {} }
  const message = gzipMessage(payload)

  const controller = makeController()
  const response = await controller.handleSignalMessage(message)

  assert.equal(response.uuid, "u-small")
  assert.equal(response.method, "doThing")
  assert.deepEqual(response.params.result, { result: "hi" })

  assert.ok(stashedBuffer(message), "small compressed payload should be pre-inflated asynchronously")
})

test("uncompressed (identity) payload skips both decompress branches", async () => {
  const payload = { method: "doThing", uuid: "u-id", params: { result: 42 }, meta: {} }
  const value = Buffer.from(new JsonCodec().encode(payload))
  const message: any = { value, headers: {} }

  const controller = makeController()
  const response = await controller.handleSignalMessage(message)

  assert.equal(response.uuid, "u-id")
  assert.deepEqual(response.params.result, { result: 42 })
  assert.equal(stashedBuffer(message), null, "identity payload should not be pre-inflated")
})
