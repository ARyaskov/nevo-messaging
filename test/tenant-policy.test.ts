import { test } from "node:test"
import assert from "node:assert/strict"
import {
  TenantPolicyRegistry,
  setTenantPolicyRegistry,
  getTenantPolicyRegistry,
  assertTenantAllowed,
  buildResilienceKey
} from "../src/common/tenant-policy"
import { MessagingError } from "../src/common/errors"
import { ErrorCode } from "../src/common/error-code"

test("policy registry returns allowed by default", () => {
  const r = new TenantPolicyRegistry()
  assert.equal(r.isAllowed("user", "t-1"), true)
})

test("policy registry blocks disabled tenants", () => {
  const r = new TenantPolicyRegistry()
  r.setEnabled("user", "noisy", false, "abuse")
  assert.equal(r.isAllowed("user", "noisy"), false)
  assert.equal(r.isAllowed("user", "other"), true)
})

test("assertTenantAllowed throws UNAUTHORIZED with reason", () => {
  setTenantPolicyRegistry(new TenantPolicyRegistry())
  getTenantPolicyRegistry().setEnabled("billing", "evicted", false, "non-payment")
  try {
    assertTenantAllowed("billing", "evicted")
    assert.fail("expected throw")
  } catch (err) {
    assert.ok(err instanceof MessagingError)
    assert.equal((err as MessagingError).code, ErrorCode.UNAUTHORIZED)
    assert.match((err as MessagingError).message, /evicted/)
    assert.match((err as MessagingError).message, /non-payment/)
  }
})

test("assertTenantAllowed no-ops when tenantId is undefined", () => {
  assertTenantAllowed("any", undefined)
})

test("buildResilienceKey appends declared dimensions", () => {
  const key = buildResilienceKey({ service: "user", method: "user.getById", tenantId: "tnt-1", callerService: "frontend" }, [
    "service",
    "method",
    "tenantId"
  ])
  assert.equal(key, "user:user.getById:tnt-1")
})

test("buildResilienceKey defaults to service:method when keyBy is empty", () => {
  const key = buildResilienceKey({ service: "user", method: "user.getById" }, [])
  assert.equal(key, "user:user.getById")
})

test("registry list returns flattened entries", () => {
  const r = new TenantPolicyRegistry()
  r.setEnabled("a", "t1", false)
  r.setEnabled("a", "t2", true)
  const list = r.list()
  assert.equal(list.length, 2)
  const t1 = list.find((e) => e.tenantId === "t1")!
  assert.equal(t1.enabled, false)
  assert.equal(t1.serviceName, "a")
})
