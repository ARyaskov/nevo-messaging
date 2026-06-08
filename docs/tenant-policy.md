# Tenant policy & kill-switch

`TenantPolicyRegistry` is a per-tenant runtime policy store with a DevTools-driven **kill-switch**: block or unblock a single tenant on a single service at runtime, without a redeploy. It also supplies the `keyBy` dimension builder that the resilience registries (circuit breaker, backpressure, adaptive) and the rate limiter use to isolate state per tenant/caller.

Source: `src/common/tenant-policy.ts`.

## The registry

A `TenantPolicy` is intentionally tiny:

```ts
interface TenantPolicy {
  enabled?: boolean   // default true
  reason?: string
  updatedAt?: number  // stamped automatically on set
}
```

Policies are keyed by `(serviceName, tenantId)`:

```ts
import { TenantPolicyRegistry } from "@riaskov/nevo-messaging"

const registry = new TenantPolicyRegistry()

registry.setEnabled("billing", "tenant-42", false, "abuse investigation")
registry.isAllowed("billing", "tenant-42")   // false
registry.isAllowed("billing", "tenant-7")    // true  (no policy = allowed)
registry.isAllowed("billing", undefined)     // true  (no tenant = not scoped)

registry.list()      // [{ serviceName, tenantId, enabled, reason, updatedAt }, …]
registry.remove("billing", "tenant-42")
registry.clear()
```

| Method | Description |
|---|---|
| `set(service, tenantId, policy)` | Write a policy; `updatedAt` is stamped to now. |
| `get(service, tenantId)` | Read the raw policy (or `undefined`). |
| `setEnabled(service, tenantId, enabled, reason?)` | Block/unblock in one call. |
| `isAllowed(service, tenantId)` | `false` only when a policy exists with `enabled === false`. Missing tenant or missing policy → `true`. |
| `list()` | All policies, flattened with their keys. |
| `remove(service, tenantId)` | Delete one policy. |
| `clear()` | Drop all policies. |

### Global registry

A process-wide singleton backs the kill-switch and the DevTools UI:

```ts
import { getTenantPolicyRegistry, setTenantPolicyRegistry } from "@riaskov/nevo-messaging"

getTenantPolicyRegistry().setEnabled("billing", "tenant-42", false)
// or swap in your own (e.g. one backed by a shared cache):
setTenantPolicyRegistry(myRegistry)
```

## The `assertTenantAllowed` kill-switch

The enforcement point is a single guard:

```ts
import { assertTenantAllowed } from "@riaskov/nevo-messaging"

assertTenantAllowed("billing", meta.tenantId)
// throws MessagingError(UNAUTHORIZED) iff a policy exists with enabled === false
```

Semantics:

- No `tenantId` → returns immediately (the call isn't tenant-scoped).
- No policy, or `enabled !== false` → allowed.
- `enabled === false` → throws `MessagingError(ErrorCode.UNAUTHORIZED)` with a message that includes the tenant, service, and the policy `reason` if set. The error is **non-retryable** (`retryable: false`) — a disabled tenant should fail fast, not retry.

This is a **kill-switch, not an authorization system**: a tenant is allowed by default and only blocked when an operator explicitly disables it (typically through the [DevTools UI](./devtools.md)). It is the fastest possible lever to shed a single noisy or abusive tenant during an incident, and it composes with — rather than replaces — your ACL and rate limiting.

## Interaction with ACL and rate-limit

The three mechanisms are layered and independent:

| Layer | Question it answers | Failure |
|---|---|---|
| [ACL](./access-control.md) | *Is this caller allowed to invoke this method at all?* | `UNAUTHORIZED` |
| **Tenant kill-switch** | *Is this tenant currently enabled on this service?* | `UNAUTHORIZED` (non-retryable) |
| [Rate limit](./rate-limiting.md) | *Has this key exceeded its budget right now?* | `RATE_LIMITED` (retryable, with `retryAfterMs`) |

The kill-switch is a binary on/off applied per tenant. Rate limiting is a continuous budget that can itself be **keyed by tenant** (see below) so one tenant's traffic can't starve another's. Use the kill-switch to fully stop a tenant; use tenant-keyed rate limits to fairly share capacity among tenants that are all still enabled.

## `keyBy` resilience dimensions

`buildResilienceKey` turns a call context into the key used by the circuit-breaker, backpressure, and adaptive registries — controlling **how finely resilience state is partitioned**.

```ts
import { buildResilienceKey } from "@riaskov/nevo-messaging"

const ctx = { service: "billing", method: "billing.charge", callerService: "web", tenantId: "t-42" }

buildResilienceKey(ctx)                              // "billing:billing.charge"  (default)
buildResilienceKey(ctx, ["service", "method", "tenantId"])  // "billing:billing.charge:t-42"
buildResilienceKey(ctx, ["tenantId"])               // "t-42"
```

The available `TenantKeyDimension`s are `"service"`, `"method"`, `"callerService"`, and `"tenantId"`. When `keyBy` is omitted or empty it defaults to `["service", "method"]`. Missing values fall back to `"anon"` (callerService) or `"no-tenant"` (tenantId).

Why this matters: with the default `service:method` key, **one** misbehaving tenant can trip a circuit breaker that then rejects *every* tenant's calls to that method. Adding `tenantId` to `keyBy` gives each tenant its own breaker / backpressure window / adaptive tuner, so one tenant's failures or overload are contained to that tenant. The same dimension list drives the Redis rate limiter's `keyBy`, so you can keep all four mechanisms partitioned consistently.

```ts
@CircuitBreaker({ keyBy: ["service", "method", "tenantId"] })
@Backpressure({ keyBy: ["service", "method", "tenantId"], maxInflight: 200 })
async charge(input: ChargeInput) { /* … */ }
```

## See also

- [multi-tenant.md](./multi-tenant.md) — the broader multi-tenancy story
- [access-control.md](./access-control.md) — per-method caller authorization
- [rate-limiting.md](./rate-limiting.md) — tenant-keyed rate limits
- [resilience-decorators.md](./resilience-decorators.md) — `keyBy` on `@CircuitBreaker` / `@Backpressure` / `@Adaptive`
- [devtools.md](./devtools.md) — flipping the kill-switch from the dashboard
