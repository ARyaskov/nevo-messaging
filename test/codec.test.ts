import { test } from "node:test"
import assert from "node:assert/strict"
import { JsonCodec, MessagePackCodec } from "../src/common/codec"

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
