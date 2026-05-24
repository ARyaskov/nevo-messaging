import { test } from "node:test"
import assert from "node:assert/strict"
import { matchesFilter } from "../src/common/subscription-filters"

test("no filter matches everything", () => {
  assert.equal(matchesFilter(undefined, { headers: { a: "b" } }), true)
})

test("header equality and regex", () => {
  assert.equal(matchesFilter({ headers: { x: "1" } }, { headers: { x: "1" } }), true)
  assert.equal(matchesFilter({ headers: { x: "1" } }, { headers: { x: "2" } }), false)
  assert.equal(matchesFilter({ headers: { x: /^[0-9]+$/ } }, { headers: { x: "42" } }), true)
})

test("meta field match", () => {
  assert.equal(matchesFilter({ meta: { tenantId: "t1" } }, { tenantId: "t1" } as any), true)
  assert.equal(matchesFilter({ meta: { tenantId: "t1" } }, { tenantId: "t2" } as any), false)
})
