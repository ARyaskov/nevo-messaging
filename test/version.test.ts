import { test } from "node:test"
import assert from "node:assert/strict"
import { parseMethod, formatMethod, isVersionCompatible, DEFAULT_METHOD_VERSION } from "../src/common/version"

test("parseMethod basics", () => {
  assert.deepEqual(parseMethod("user.getById"), { name: "user.getById", version: null })
  assert.deepEqual(parseMethod("user.getById@v2"), { name: "user.getById", version: "v2" })
  assert.deepEqual(parseMethod("svc.do@2024-01-01"), { name: "svc.do", version: "2024-01-01" })
})

test("formatMethod", () => {
  assert.equal(formatMethod("user.x", null), "user.x")
  assert.equal(formatMethod("user.x", "v3"), "user.x@v3")
})

test("isVersionCompatible", () => {
  assert.equal(isVersionCompatible(null, "v1"), true)
  assert.equal(isVersionCompatible("v1", "v1"), true)
  assert.equal(isVersionCompatible("v1", "v2"), false)
  assert.equal(isVersionCompatible(DEFAULT_METHOD_VERSION, null), true)
})
