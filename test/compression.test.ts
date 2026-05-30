import { test } from "node:test"
import assert from "node:assert/strict"
import { gzipSync, deflateSync } from "node:zlib"
import { maybeCompress, maybeDecompress, maybeDecompressAsync, resolveCompressionOptions } from "../src/common/compression"
import { PayloadTooLargeError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"

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

test("gzip bomb is rejected with PAYLOAD_TOO_LARGE, not allocated", () => {
  // 32 MiB of zeros compresses to a few KB. With a 1 MiB output cap the inflate
  // is aborted by zlib's maxOutputLength before the full buffer can ever be
  // materialised — so the receiver rejects instead of allocating 32 MiB.
  const bomb = gzipSync(Buffer.alloc(32 * 1024 * 1024))
  assert.ok(bomb.byteLength < 1024 * 1024)
  assert.throws(
    () => maybeDecompress(bomb, "gzip", 1024 * 1024),
    (err: unknown) => err instanceof PayloadTooLargeError && err.code === ErrorCode.PAYLOAD_TOO_LARGE
  )
})

test("deflate bomb is rejected with PAYLOAD_TOO_LARGE", () => {
  const bomb = deflateSync(Buffer.alloc(32 * 1024 * 1024))
  assert.throws(
    () => maybeDecompress(bomb, "deflate", 1024 * 1024),
    (err: unknown) => err instanceof PayloadTooLargeError
  )
})

test("async gzip bomb is rejected with PAYLOAD_TOO_LARGE", async () => {
  const bomb = gzipSync(Buffer.alloc(32 * 1024 * 1024))
  await assert.rejects(
    () => maybeDecompressAsync(bomb, "gzip", 1024 * 1024),
    (err: unknown) => err instanceof PayloadTooLargeError
  )
})

test("round trip still works under the output cap", () => {
  const opts = resolveCompressionOptions({ enabled: true, algorithm: "gzip", threshold: 1 })
  const original = Buffer.from("hello world ".repeat(1000))
  const { data, encoding } = maybeCompress(original, opts)
  const decoded = maybeDecompress(data, encoding, 1024 * 1024)
  assert.equal(Buffer.from(decoded).toString(), original.toString())
})

test("async round trip works under the output cap", async () => {
  const opts = resolveCompressionOptions({ enabled: true, algorithm: "gzip", threshold: 1 })
  const original = Buffer.from("hello world ".repeat(1000))
  const { data, encoding } = maybeCompress(original, opts)
  const decoded = await maybeDecompressAsync(data, encoding, 1024 * 1024)
  assert.equal(Buffer.from(decoded).toString(), original.toString())
})
