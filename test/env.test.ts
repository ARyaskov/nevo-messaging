import { test } from "node:test"
import assert from "node:assert/strict"
import { IS_PROD, IS_DEV, isProduction, isDevelopment } from "../src/common/env"

test("env module exports cached booleans", () => {
  assert.equal(typeof IS_PROD, "boolean")
  assert.equal(typeof IS_DEV, "boolean")
  assert.equal(IS_DEV, !IS_PROD)
  assert.equal(isProduction(), IS_PROD)
  assert.equal(isDevelopment(), IS_DEV)
})

test("cache is read once at module load", () => {
  const before = isProduction()
  const prev = process.env.NODE_ENV
  process.env.NODE_ENV = prev === "production" ? "development" : "production"
  assert.equal(isProduction(), before, "cached value must not flip when NODE_ENV changes after load")
  process.env.NODE_ENV = prev
})
