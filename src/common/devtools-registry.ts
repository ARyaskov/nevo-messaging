import { getDevToolsBus, DevToolsBus, DevToolsEvent } from "./devtools"
import type { AccessControlConfig } from "./types"
import type { SignalMetadata } from "../signal.decorator"
import type { CircuitState } from "./circuit-breaker"
import { getTenantPolicyRegistry, type TenantPolicy } from "./tenant-policy"

export interface ServiceMethodInfo {
  signalName: string
  version?: string
  hasSchema?: boolean
  description?: string
}

export interface ServiceInfo {
  serviceName: string
  instanceId?: string
  transport?: string
  topic?: string
  registeredAt: number
  capabilities?: string[]
  methods: ServiceMethodInfo[]
  accessControl?: AccessControlConfig
}

export interface CircuitInfo {
  key: string
  service: string
  method: string
  state: CircuitState
  failures: number
  successes: number
  openedAt?: number
  closedAt?: number
  lastTransitionAt: number
  lastError?: string
}

export class DevToolsRegistry {
  private readonly services = new Map<string, ServiceInfo>()
  private readonly circuits = new Map<string, CircuitInfo>()

  registerService(info: Omit<ServiceInfo, "registeredAt">): void {
    const existing = this.services.get(info.serviceName)
    const next: ServiceInfo = {
      ...info,
      registeredAt: existing?.registeredAt ?? Date.now()
    }
    this.services.set(info.serviceName, next)
  }

  unregisterService(serviceName: string): void {
    this.services.delete(serviceName)
  }

  getService(name: string): ServiceInfo | undefined {
    return this.services.get(name)
  }

  listServices(): ServiceInfo[] {
    return this.services.values().toArray()
  }

  recordCircuit(serviceMethod: string, state: CircuitState, extra?: Partial<CircuitInfo>): void {
    const [service = "unknown", method = "unknown"] = serviceMethod.split(":")
    const now = Date.now()
    const prev = this.circuits.get(serviceMethod)
    const next: CircuitInfo = {
      key: serviceMethod,
      service,
      method,
      state,
      failures: extra?.failures ?? prev?.failures ?? 0,
      successes: extra?.successes ?? prev?.successes ?? 0,
      openedAt: state === "open" ? now : prev?.openedAt,
      closedAt: state === "closed" ? now : prev?.closedAt,
      lastTransitionAt: now,
      lastError: extra?.lastError ?? prev?.lastError
    }
    this.circuits.set(serviceMethod, next)
  }

  getCircuit(serviceMethod: string): CircuitInfo | undefined {
    return this.circuits.get(serviceMethod)
  }

  listCircuits(): CircuitInfo[] {
    return this.circuits.values().toArray()
  }

  reset(): void {
    this.services.clear()
    this.circuits.clear()
  }

  // ---------------------------------------------------------------------
  // Tenant policies — proxied through the global TenantPolicyRegistry so
  // DevTools `/api/tenants` POST can disable a tenant without restarts.
  // ---------------------------------------------------------------------

  listTenantPolicies(): Array<TenantPolicy & { serviceName: string; tenantId: string }> {
    return getTenantPolicyRegistry().list()
  }

  setTenantPolicy(serviceName: string, tenantId: string, policy: TenantPolicy): void {
    getTenantPolicyRegistry().set(serviceName, tenantId, policy)
  }

  setTenantEnabled(serviceName: string, tenantId: string, enabled: boolean, reason?: string): void {
    getTenantPolicyRegistry().setEnabled(serviceName, tenantId, enabled, reason)
  }
}

let global: DevToolsRegistry | null = null

export function getDevToolsRegistry(): DevToolsRegistry {
  if (!global) global = new DevToolsRegistry()
  return global
}

export function setDevToolsRegistry(r: DevToolsRegistry): void {
  global = r
}

export function publishDevToolsEvent(event: DevToolsEvent, bus: DevToolsBus = getDevToolsBus()): void {
  bus.publish(event)
}

export function describeMethodsFromSignals(signals: SignalMetadata[]): ServiceMethodInfo[] {
  return signals
    .filter((s) => !s.signalName.startsWith("nevo."))
    .map((s) => ({
      signalName: s.signalName,
      version: s.version,
      hasSchema: Boolean(s.options?.schema)
    }))
}
