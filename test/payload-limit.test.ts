import { test } from "node:test"
import assert from "node:assert/strict"
import { enforcePayloadLimit } from "../src/common/payload-limit"
import { PayloadTooLargeError } from "../src/common/errors"

test("within limit OK", () => {
  enforcePayloadLimit(Buffer.alloc(10), 1024)
})

test("exceeded throws PayloadTooLargeError", () => {
  assert.throws(() => enforcePayloadLimit(Buffer.alloc(100), 10), PayloadTooLargeError)
})

test("string limit", () => {
  assert.throws(() => enforcePayloadLimit("x".repeat(100), 10), PayloadTooLargeError)
})
