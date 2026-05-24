import { test } from "node:test"
import assert from "node:assert/strict"
import { resolveRetryOptions, shouldRetry, withRetry } from "../src/common/retry"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"

test("withRetry succeeds without retries", async () => {
  let n = 0
  const r = await withRetry(async () => { n++; return "ok" }, resolveRetryOptions({ maxAttempts: 3 }))
  assert.equal(r, "ok")
  assert.equal(n, 1)
})

test("withRetry retries on TIMEOUT", async () => {
  let n = 0
  const r = await withRetry(async () => {
    n++
    if (n < 3) throw new MessagingError(ErrorCode.TIMEOUT, { message: "x" })
    return "ok"
  }, resolveRetryOptions({ maxAttempts: 5, baseMs: 1, maxMs: 2, jitter: false }))
  assert.equal(r, "ok")
  assert.equal(n, 3)
})

test("withRetry stops on non-retryable", async () => {
  let n = 0
  await assert.rejects(async () => {
    await withRetry(async () => {
      n++
      throw new MessagingError(ErrorCode.VALIDATION_FAILED, { message: "x" })
    }, resolveRetryOptions({ maxAttempts: 3, baseMs: 1 }))
  })
  assert.equal(n, 1)
})

test("shouldRetry custom codes", () => {
  const opts = resolveRetryOptions({ maxAttempts: 3, retryOnCodes: [ErrorCode.BAD_REQUEST] })
  assert.equal(shouldRetry(new MessagingError(ErrorCode.BAD_REQUEST), opts), true)
  assert.equal(shouldRetry(new MessagingError(ErrorCode.VALIDATION_FAILED), opts), false)
})
