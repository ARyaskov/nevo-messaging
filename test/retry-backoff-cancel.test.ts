import { test } from "node:test"
import assert from "node:assert/strict"
import { withRetry, resolveRetryOptions } from "../src/common/retry"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"

// Fixed, non-jittered 300ms backoff so an abort timed at 50ms lands strictly
// inside the post-failure sleep, whatever the scheduler does to the attempt.
const opts = resolveRetryOptions({ maxAttempts: 3, baseMs: 300, maxMs: 300, jitter: false, retryOnCodes: [ErrorCode.TIMEOUT] })

test("abort during backoff rejects with the SAME CANCELLED contract as the other phases", async () => {
  const ac = new AbortController()
  let attempts = 0
  const p = withRetry(
    async () => {
      attempts++
      throw new MessagingError(ErrorCode.TIMEOUT, { message: "boom", retryable: true })
    },
    opts,
    ac.signal
  )
  // Attempt 1 fails immediately; the 300ms backoff sleep then starts, and the
  // abort at 50ms must surface CANCELLED — not the native ABORT_ERR — and
  // must not let the loop continue.
  setTimeout(() => ac.abort(), 50)
  await assert.rejects(
    p,
    (err: unknown) =>
      err instanceof MessagingError && err.code === ErrorCode.CANCELLED && err.message === "Retry cancelled" && err.name === "MessagingError"
  )
  assert.equal(attempts, 1, "a cancelled backoff must not start another attempt")
})

test("regression guard: abort before dispatch already produced the same CANCELLED contract", async () => {
  const ac = new AbortController()
  ac.abort()
  let attempts = 0
  await assert.rejects(
    () =>
      withRetry(
        async () => {
          attempts++
          return "ok"
        },
        opts,
        ac.signal
      ),
    (err: unknown) => err instanceof MessagingError && err.code === ErrorCode.CANCELLED
  )
  assert.equal(attempts, 0, "the pre-dispatch phase must not run the work")
})
