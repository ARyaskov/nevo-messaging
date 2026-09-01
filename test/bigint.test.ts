import { test } from "node:test"
import assert from "node:assert/strict"
import {
  serializeBigInt,
  deserializeBigInt,
  stringifyWithBigInt,
  parseWithBigInt,
  mayContainWireSentinel,
  BIGINT_SENTINEL,
  STRING_ESCAPE,
  SENTINEL_PREFIX
} from "../src/common/bigint.utils"
import { MessagePackCodec } from "../src/common/codec"

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

test("deserializeBigInt returns untouched containers unchanged", () => {
  const inner = { a: 1, b: [1, 2, 3] }
  const obj = { inner, list: [inner] }
  const back = deserializeBigInt(obj)
  assert.equal(back, obj, "a payload with no sentinel must not be reallocated")
  assert.equal(back.inner, inner)
})

test("deserializeBigInt reallocates only the path that changed", () => {
  const untouched = { deep: { x: 1 } }
  const obj = { untouched, changed: { id: `${BIGINT_SENTINEL}7` } }
  const back = deserializeBigInt(obj)
  assert.notEqual(back, obj)
  assert.equal(back.untouched, untouched, "sibling subtrees are shared, not cloned")
  assert.equal(back.changed.id, 7n)
})

test("deserializeBigInt still sanitises an own __proto__ data property", () => {
  const hostile = JSON.parse('{"__proto__": {"polluted": true}, "safe": 1}')
  const back = deserializeBigInt(hostile)
  assert.equal(back.safe, 1)
  assert.equal(Object.getPrototypeOf(back), Object.prototype)
  assert.equal(({} as any).polluted, undefined)
})

test("every wire sentinel shares the prefix the fast-path probe scans for", () => {
  assert.ok(BIGINT_SENTINEL.startsWith(SENTINEL_PREFIX))
  assert.ok(STRING_ESCAPE.startsWith(SENTINEL_PREFIX))
})

test("mayContainWireSentinel gates the decode walk without false negatives", () => {
  const enc = new TextEncoder()
  assert.equal(mayContainWireSentinel(enc.encode('{"a":1}')), false)
  assert.equal(mayContainWireSentinel(enc.encode(`{"a":"${BIGINT_SENTINEL}1"}`)), true)
  assert.equal(mayContainWireSentinel(enc.encode(`{"a":"${STRING_ESCAPE}x"}`)), true)

  // A subarray view must be scanned over its own window only.
  const backing = enc.encode(`AAAA${BIGINT_SENTINEL}1`)
  assert.equal(mayContainWireSentinel(backing.subarray(0, 4)), false)
  assert.equal(mayContainWireSentinel(backing.subarray(4)), true)
})

test("msgpack round-trips BigInt through the pre-scanned decode path", () => {
  const codec = new MessagePackCodec()
  const payload = {
    id: 9007199254740993n,
    nested: { amounts: [1n, 2n], label: "plain" },
    literal: `${BIGINT_SENTINEL}not-a-number`,
    escaped: `${STRING_ESCAPE}already-escaped`
  }
  const back = codec.decode<typeof payload>(codec.encode(payload))
  assert.equal(back.id, 9007199254740993n)
  assert.deepEqual(back.nested.amounts, [1n, 2n])
  assert.equal(back.nested.label, "plain")
  assert.equal(back.literal, `${BIGINT_SENTINEL}not-a-number`)
  assert.equal(back.escaped, `${STRING_ESCAPE}already-escaped`)
})

test("msgpack fast path leaves a sentinel-free payload semantically identical", () => {
  const codec = new MessagePackCodec()
  const payload = { a: 1, b: "two", c: [3, { d: true, e: null }], f: { g: "@@nevo-lookalike" } }
  assert.deepEqual(codec.decode(codec.encode(payload)), payload)
})
