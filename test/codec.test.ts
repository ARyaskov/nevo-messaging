import { test } from "node:test"
import assert from "node:assert/strict"
import { JsonCodec, JsonCodecFast, MessagePackCodec, type Codec } from "../src/common/codec"

test("JsonCodec round trips with BigInt", () => {
  const c = new JsonCodec()
  const buf = c.encode({ id: 12345678901234567890n, items: [1n, 2n] })
  const out: any = c.decode(buf)
  assert.equal(out.id, 12345678901234567890n)
  assert.deepEqual(out.items, [1n, 2n])
})

test("MessagePackCodec round trips with BigInt (if installed)", () => {
  let c: MessagePackCodec
  try {
    c = new MessagePackCodec()
    const buf = c.encode({ id: 9007199254740997n, name: "x" })
    const out: any = c.decode(buf)
    assert.equal(out.id, 9007199254740997n)
    assert.equal(out.name, "x")
  } catch (err: any) {
    if (/Missing optional dependency/.test(err?.message)) return
    throw err
  }
})

test("JsonCodec on parse error throws PARSE_ERROR", () => {
  const c = new JsonCodec()
  assert.throws(() => c.decode("not-json"), /PARSE/i)
})

test("all built-in codecs share Date/undefined/BigInt wire semantics", () => {
  const codecs: Codec[] = [new JsonCodec(), new JsonCodecFast(), new MessagePackCodec()]
  for (const codec of codecs) {
    const value = {
      date: new Date("2026-01-02T03:04:05.000Z"),
      missing: undefined,
      items: [undefined, 1n],
      huge: (1n << 100n) + 123n
    }
    const out = codec.decode<any>(codec.encode(value))
    assert.deepEqual(out, {
      date: "2026-01-02T03:04:05.000Z",
      items: [null, 1n],
      huge: (1n << 100n) + 123n
    })
  }
})

test("JsonCodec does not decode legacy-looking strings unless explicitly migrated", () => {
  const codec = new JsonCodec()
  assert.deepEqual(codec.decode(codec.encode({ version: "42n" })), { version: "42n" })
})
