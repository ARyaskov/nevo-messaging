import { test } from "node:test"
import assert from "node:assert/strict"
import { levenshteinDistance, suggestClosestMethod } from "../src/common/levenshtein"

test("levenshteinDistance basics", () => {
  assert.equal(levenshteinDistance("kitten", "sitting"), 3)
  assert.equal(levenshteinDistance("flaw", "lawn"), 2)
  assert.equal(levenshteinDistance("", "abc"), 3)
  assert.equal(levenshteinDistance("abc", "abc"), 0)
})

test("suggestClosestMethod", () => {
  assert.equal(suggestClosestMethod("user.getByI", ["user.getById", "user.delete"]), "user.getById")
  assert.equal(suggestClosestMethod("absolutelyDifferent", ["foo", "bar"]), null)
  assert.equal(suggestClosestMethod("ping", []), null)
})
