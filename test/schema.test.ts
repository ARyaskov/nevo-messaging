import { test } from "node:test"
import assert from "node:assert/strict"
import { toValidator } from "../src/common/schema"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"

test("zod-like schema validator via safeParse success", () => {
  const fakeZod = {
    safeParse: (v: any) => v && typeof v.id === "number" ? { success: true, data: { id: v.id } } : { success: false, error: { issues: [{ path: ["id"], message: "expected number" }] } }
  }
  const v = toValidator(fakeZod)!
  assert.deepEqual(v.parse({ id: 1 }), { id: 1 })
})

test("zod-like schema validator via safeParse failure throws VALIDATION_FAILED", () => {
  const fakeZod = {
    safeParse: () => ({ success: false, error: { issues: [{ message: "bad" }] } })
  }
  const v = toValidator(fakeZod)!
  try {
    v.parse({})
    assert.fail("should have thrown")
  } catch (err: any) {
    assert.ok(err instanceof MessagingError)
    assert.equal(err.code, ErrorCode.VALIDATION_FAILED)
  }
})

test("parse() shape used directly", () => {
  const fakeZod = { parse: (v: any) => ({ ok: v }) }
  const v = toValidator(fakeZod)!
  assert.deepEqual(v.parse(1), { ok: 1 })
})
