import { test } from "node:test"
import assert from "node:assert/strict"
import { ErrorCode, isRetryable } from "../src/common/error-code"
import { resolveRetryOptions, shouldRetry } from "../src/common/retry"
import { MessagingError } from "../src/common/errors"

test("retryable codes", () => {
  assert.equal(isRetryable(ErrorCode.TIMEOUT), true)
  assert.equal(isRetryable(ErrorCode.SERVICE_UNAVAILABLE), true)
  assert.equal(isRetryable(ErrorCode.CONNECTION_LOST), true)
})

test("non-retryable", () => {
  assert.equal(isRetryable(ErrorCode.VALIDATION_FAILED), false)
  assert.equal(isRetryable(ErrorCode.UNAUTHORIZED), false)
  assert.equal(isRetryable(ErrorCode.METHOD_NOT_FOUND), false)
})

test("INTERNAL is not retried by default but can be opted into", () => {
  assert.equal(isRetryable(ErrorCode.INTERNAL), false)

  const opts = resolveRetryOptions({ retryOnCodes: [ErrorCode.INTERNAL] })
  assert.equal(shouldRetry(new MessagingError(ErrorCode.INTERNAL, { message: "boom" }), opts), true)

  const defaults = resolveRetryOptions()
  assert.equal(shouldRetry(new MessagingError(ErrorCode.INTERNAL, { message: "boom" }), defaults), false)
  assert.equal(shouldRetry(new MessagingError(ErrorCode.INTERNAL, { message: "boom", retryable: true }), defaults), true)
})
