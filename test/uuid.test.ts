import { test } from "node:test"
import assert from "node:assert/strict"
import { uuidv7, uuidv7Timestamp } from "../src/common/uuid"

test("uuidv7 format and version/variant nibbles", () => {
  const u = uuidv7()
  assert.match(u, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
})

test("uuidv7 is monotonic within and across ms", () => {
  const ids: string[] = []
  for (let i = 0; i < 5000; i++) ids.push(uuidv7())
  for (let i = 1; i < ids.length; i++) {
    assert.ok(ids[i] >= ids[i - 1], `monotonic broken at i=${i}: ${ids[i - 1]} vs ${ids[i]}`)
  }
})

test("uuidv7Timestamp recovers timestamp roughly", () => {
  const before = Date.now()
  const u = uuidv7()
  const ts = uuidv7Timestamp(u)
  const after = Date.now()
  assert.ok(ts >= before - 1 && ts <= after + 1, `ts ${ts} out of [${before}, ${after}]`)
})

test("uuidv7 returns unique values", () => {
  const set = new Set<string>()
  for (let i = 0; i < 10_000; i++) set.add(uuidv7())
  assert.equal(set.size, 10_000)
})
