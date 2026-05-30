import { MessagingError } from "./errors"
import { ErrorCode } from "./error-code"

/**
 * Per-tenant runtime policy registry with a DevTools-driven kill-switch.
 */

export type TenantKeyDimension = "service" | "method" | "callerService" | "tenantId"

export interface TenantPolicy {
  /** Whether requests for this tenant are allowed. Default true. */
  enabled?: boolean
  reason?: string
  updatedAt?: number
}

export class TenantPolicyRegistry {
  private readonly map = new Map<string, TenantPolicy>()

  private k(serviceName: string, tenantId: string): string { return `${serviceName}::${tenantId}` }

  set(serviceName: string, tenantId: string, policy: TenantPolicy): void {
    this.map.set(this.k(serviceName, tenantId), { ...policy, updatedAt: Date.now() })
  }

  get(serviceName: string, tenantId: string): TenantPolicy | undefined {
    return this.map.get(this.k(serviceName, tenantId))
  }

  list(): Array<TenantPolicy & { serviceName: string; tenantId: string }> {
    const out: Array<TenantPolicy & { serviceName: string; tenantId: string }> = []
    for (const [key, policy] of this.map.entries()) {
      const [serviceName, tenantId] = key.split("::")
      out.push({ ...policy, serviceName, tenantId })
    }
    return out
  }

  /** Block / unblock a tenant in one call. */
  setEnabled(serviceName: string, tenantId: string, enabled: boolean, reason?: string): void {
    this.set(serviceName, tenantId, { enabled, reason })
  }

  isAllowed(serviceName: string, tenantId: string | undefined): boolean {
    if (!tenantId) return true
    const policy = this.get(serviceName, tenantId)
    return policy?.enabled !== false
  }

  remove(serviceName: string, tenantId: string): boolean {
    return this.map.delete(this.k(serviceName, tenantId))
  }

  clear(): void { this.map.clear() }
}

let globalRegistry: TenantPolicyRegistry | null = null

export function getTenantPolicyRegistry(): TenantPolicyRegistry {
  if (!globalRegistry) globalRegistry = new TenantPolicyRegistry()
  return globalRegistry
}

export function setTenantPolicyRegistry(r: TenantPolicyRegistry): void { globalRegistry = r }

/** Throw `UNAUTHORIZED` when the caller's tenant has been administratively disabled. */
export function assertTenantAllowed(serviceName: string, tenantId: string | undefined): void {
  if (!tenantId) return
  const policy = getTenantPolicyRegistry().get(serviceName, tenantId)
  if (policy?.enabled === false) {
    throw new MessagingError(ErrorCode.UNAUTHORIZED, {
      message: `Tenant ${tenantId} is disabled for service ${serviceName}${policy.reason ? `: ${policy.reason}` : ""}`,
      tenantId,
      retryable: false
    })
  }
}

/** Build a key from `keyBy` dimensions for circuit-breaker / backpressure / adaptive registries. */
export interface ResilienceKeyContext {
  service: string
  method: string
  callerService?: string
  tenantId?: string
}

export function buildResilienceKey(ctx: ResilienceKeyContext, keyBy?: TenantKeyDimension[]): string {
  const dims = keyBy && keyBy.length > 0 ? keyBy : ["service", "method"]
  const parts: string[] = []
  for (const d of dims) {
    switch (d) {
      case "service": parts.push(ctx.service); break
      case "method": parts.push(ctx.method); break
      case "callerService": parts.push(ctx.callerService ?? "anon"); break
      case "tenantId": parts.push(ctx.tenantId ?? "no-tenant"); break
    }
  }
  return parts.join(":")
}
