import { test } from "node:test"
import assert from "node:assert/strict"
import { maybeCompress, maybeDecompress, resolveCompressionOptions } from "../src/common/compression"

test("compress + decompress round trip", () => {
  const opts = resolveCompressionOptions({ enabled: true, algorithm: "gzip", threshold: 1 })
  const original = Buffer.from("hello world ".repeat(100))
  const { data, encoding } = maybeCompress(original, opts)
  assert.equal(encoding, "gzip")
  assert.ok(data.byteLength < original.byteLength)
  const decoded = maybeDecompress(data, encoding)
  assert.equal(Buffer.from(decoded).toString(), original.toString())
})

test("below threshold passes through", () => {
  const opts = resolveCompressionOptions({ enabled: true, algorithm: "gzip", threshold: 10_000 })
  const original = Buffer.from("small")
  const { data, encoding } = maybeCompress(original, opts)
  assert.equal(encoding, "identity")
  assert.equal(data, original)
})
