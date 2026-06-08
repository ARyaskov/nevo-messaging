import "reflect-metadata"
import { test } from "node:test"
import assert from "node:assert/strict"
import { gzipSync } from "node:zlib"
import { randomBytes } from "node:crypto"
import { KafkaSignalRouter } from "../src/transports/kafka/kafka.signal-router.decorator"
import { addSignalMetadata } from "../src/signal.decorator"
import { JsonCodec } from "../src/common/codec"
import { ASYNC_DECOMPRESS_THRESHOLD } from "../src/common/compression"

// These tests drive the real KafkaSignalRouter decode path end-to-end without a
// broker: we hand the wrapped `handleSignalMessage` a synthetic kafkajs-shaped
// message and assert the gzip payload round-trips. The wrapper inflates large
// (>= ASYNC_DECOMPRESS_THRESHOLD) payloads off the event loop via the async
// branch and stashes the result on the message object under a private Symbol;
// the synchronous extractor then reuses it. Smaller payloads inflate inline and
// never get a stash. We detect which branch ran by scanning the message's own
// Symbol keys for the stashed Uint8Array — present only on the async path.

const SILENT_LOGGER: any = {
  trace() {}, debug() {}, info() {}, warn() {}, error() {}, fatal() {},
  child() { return SILENT_LOGGER }, isLevelEnabled() { return false }
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
  // Pad the params so the *compressed* value clears the async threshold. The
  // branch keys off the COMPRESSED byte length, so the filler must be effectively
  // incompressible — repeated text would gzip down to a few bytes. Hex-encoded
  // random bytes are ~incompressible, so the gzip output stays well above the
  // threshold and forces the async inflate path.
  const big = randomBytes(ASYNC_DECOMPRESS_THRESHOLD * 4).toString("hex")
  const payload = { method: "doThing", uuid: "u-big", params: { result: big }, meta: { tenantId: "t1" } }
  const message = gzipMessage(payload)
  assert.ok(message.value.byteLength >= ASYNC_DECOMPRESS_THRESHOLD, "compressed value must clear the async threshold")

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

test("small gzip payload decodes through the sync branch", async () => {
  const payload = { method: "doThing", uuid: "u-small", params: { result: "hi" }, meta: {} }
  const message = gzipMessage(payload)
  assert.ok(message.value.byteLength < ASYNC_DECOMPRESS_THRESHOLD, "compressed value must stay below the async threshold")

  const controller = makeController()
  const response = await controller.handleSignalMessage(message)

  assert.equal(response.uuid, "u-small")
  assert.equal(response.method, "doThing")
  assert.deepEqual(response.params.result, { result: "hi" })

  // No async pre-inflate: nothing stashed, the sync branch handled it.
  assert.equal(stashedBuffer(message), null, "small payload should not be pre-inflated")
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
