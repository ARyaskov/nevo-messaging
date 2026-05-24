import { test } from "node:test"
import assert from "node:assert/strict"
import { CircuitBreakerRegistry } from "../src/common/circuit-breaker"
import { MessagingError, CircuitOpenError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"

test("opens after threshold failures and blocks", () => {
  const cb = new CircuitBreakerRegistry({ enabled: true, failureThreshold: 2, resetTimeoutMs: 1000 })
  const key = "svc:method"
  cb.before(key)
  cb.onFailure(key, new Error("boom"))
  cb.before(key)
  cb.onFailure(key, new Error("boom"))
  assert.throws(() => cb.before(key), CircuitOpenError)
})

test("validation errors do not count as failures", () => {
  const cb = new CircuitBreakerRegistry({ enabled: true, failureThreshold: 1, resetTimeoutMs: 1000 })
  const key = "svc:method"
  cb.before(key)
  cb.onFailure(key, new MessagingError(ErrorCode.VALIDATION_FAILED))
  cb.before(key) // still allowed
})

test("resets after timeout", async () => {
  const cb = new CircuitBreakerRegistry({ enabled: true, failureThreshold: 1, resetTimeoutMs: 30 })
  const key = "svc:method"
  cb.before(key)
  cb.onFailure(key, new Error("boom"))
  assert.throws(() => cb.before(key), CircuitOpenError)
  await new Promise((r) => setTimeout(r, 60))
  cb.before(key)
  cb.onSuccess(key)
})
