import { test } from "node:test"
import assert from "node:assert/strict"
import { serializeBigInt, deserializeBigInt, stringifyWithBigInt, parseWithBigInt } from "../src/common/bigint.utils"

test("serializeBigInt with new sentinel and round trip", () => {
  const obj = { id: 9007199254740993n, name: "Alice", list: [1n, 2n, 3n] }
  const serialized = serializeBigInt(obj)
  assert.equal(serialized.id, "@@nevo:bigint:9007199254740993")
  assert.deepEqual(serialized.list, ["@@nevo:bigint:1", "@@nevo:bigint:2", "@@nevo:bigint:3"])

  const back = deserializeBigInt(serialized)
  assert.equal(back.id, 9007199254740993n)
  assert.deepEqual(back.list, [1n, 2n, 3n])
})

test("legacy '123n' is not auto-decoded unless acceptLegacy is true", () => {
  const evil = { version: "123n", comment: "build 9876n" }
  const back = deserializeBigInt(evil)
  assert.equal(back.version, "123n")
  assert.equal(back.comment, "build 9876n")

  const legacyDecoded = deserializeBigInt(evil, { acceptLegacy: true })
  assert.equal(legacyDecoded.version, 123n)
})

test("stringify/parse round-trips BigInt", () => {
  const x = { id: 42n, nested: { sum: 100n } }
  const s = stringifyWithBigInt(x)
  const y = parseWithBigInt(s)
  assert.equal(y.id, 42n)
  assert.equal(y.nested.sum, 100n)
})
