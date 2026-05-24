import { test } from "node:test"
import assert from "node:assert/strict"
import { ErrorCode, isRetryable } from "../src/common/error-code"

test("retryable codes", () => {
  assert.equal(isRetryable(ErrorCode.TIMEOUT), true)
  assert.equal(isRetryable(ErrorCode.SERVICE_UNAVAILABLE), true)
  assert.equal(isRetryable(ErrorCode.CONNECTION_LOST), true)
  assert.equal(isRetryable(ErrorCode.INTERNAL), true)
})

test("non-retryable", () => {
  assert.equal(isRetryable(ErrorCode.VALIDATION_FAILED), false)
  assert.equal(isRetryable(ErrorCode.UNAUTHORIZED), false)
  assert.equal(isRetryable(ErrorCode.METHOD_NOT_FOUND), false)
})
