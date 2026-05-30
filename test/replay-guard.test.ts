import { test } from "node:test"
import assert from "node:assert/strict"
import { ReplayGuard } from "../src/common/replay-protection"

test("blocks duplicate uuid in window", () => {
  const g = new ReplayGuard({ enabled: true, windowMs: 60_000 })
  g.check("uuid-1", Date.now())
  assert.throws(() => g.check("uuid-1", Date.now()), /replay window/i)
})

test("blocks old timestamps outside window", () => {
  const g = new ReplayGuard({ enabled: true, windowMs: 1000 })
  assert.throws(() => g.check("uuid-x", Date.now() - 10_000), /replay window/)
})

test("disabled does nothing", () => {
  const g = new ReplayGuard({ enabled: false })
  g.check("u", Date.now())
  g.check("u", Date.now())
})

test("rejects missing uuid when enabled (fail closed)", () => {
  const g = new ReplayGuard({ enabled: true, windowMs: 60_000 })
  assert.throws(() => g.check(undefined, Date.now()), /missing uuid/i)
})

test("rejects missing ts when enabled (fail closed)", () => {
  const g = new ReplayGuard({ enabled: true, windowMs: 60_000 })
  assert.throws(() => g.check("uuid-1", undefined), /missing ts/i)
})

test("rejects non-finite ts when enabled (fail closed)", () => {
  const g = new ReplayGuard({ enabled: true, windowMs: 60_000 })
  assert.throws(() => g.check("uuid-1", Number.NaN), /missing ts/i)
})

test("disabled allows missing uuid and ts", () => {
  const g = new ReplayGuard({ enabled: false })
  g.check(undefined, undefined)
})
