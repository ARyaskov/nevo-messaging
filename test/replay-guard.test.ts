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
