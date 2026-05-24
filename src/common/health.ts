export const NEVO_HEALTH_METHOD = "nevo.health"
export const NEVO_LIVENESS_METHOD = "nevo.live"
export const NEVO_READINESS_METHOD = "nevo.ready"

export type HealthKind = "liveness" | "readiness" | "both"

export interface HealthStatus {
  status: "ok" | "degraded" | "down"
  service: string
  instanceId?: string
  version?: string
  uptimeMs: number
  ts: number
  checks?: Record<string, { status: "ok" | "down"; message?: string; kind?: HealthKind }>
}

export type HealthCheckResult = { status: "ok" | "down"; message?: string }
export type HealthCheckFn = () => Promise<HealthCheckResult> | HealthCheckResult

interface RegisteredCheck {
  fn: HealthCheckFn
  kind: HealthKind
  timeoutMs?: number
}

export class HealthRegistry {
  private readonly checks = new Map<string, RegisteredCheck>()
  private readonly startedAt = Date.now()
  private readonly serviceName: string
  private readonly instanceId?: string
  private readonly version?: string

  constructor(opts: { serviceName: string; instanceId?: string; version?: string }) {
    this.serviceName = opts.serviceName
    this.instanceId = opts.instanceId
    this.version = opts.version
  }

  register(name: string, fn: HealthCheckFn, opts?: { kind?: HealthKind; timeoutMs?: number }): void {
    this.checks.set(name, { fn, kind: opts?.kind ?? "both", timeoutMs: opts?.timeoutMs })
  }

  unregister(name: string): void {
    this.checks.delete(name)
  }

  private async runOne(name: string, entry: RegisteredCheck): Promise<HealthCheckResult> {
    try {
      if (!entry.timeoutMs || entry.timeoutMs <= 0) return await entry.fn()
      const { promise, resolve, reject } = Promise.withResolvers<HealthCheckResult>()
      const timer = setTimeout(() => reject(new Error(`Check "${name}" timed out after ${entry.timeoutMs}ms`)), entry.timeoutMs)
      Promise.resolve(entry.fn()).then(resolve, reject).finally(() => clearTimeout(timer))
      return await promise
    } catch (err: any) {
      return { status: "down", message: err?.message ?? "check failed" }
    }
  }

  async report(kindFilter: HealthKind | "all" = "all"): Promise<HealthStatus> {
    const results: Record<string, { status: "ok" | "down"; message?: string; kind?: HealthKind }> = {}
    let overall: "ok" | "degraded" | "down" = "ok"
    for (const [name, entry] of this.checks.entries()) {
      if (kindFilter !== "all" && entry.kind !== "both" && entry.kind !== kindFilter) continue
      const r = await this.runOne(name, entry)
      results[name] = { ...r, kind: entry.kind }
      if (r.status === "down") overall = overall === "down" ? "down" : "degraded"
    }
    if (Object.values(results).some((r) => r.status === "down")) {
      const allDown = Object.values(results).every((r) => r.status === "down")
      overall = allDown ? "down" : "degraded"
    }
    return {
      status: overall,
      service: this.serviceName,
      instanceId: this.instanceId,
      version: this.version,
      uptimeMs: Date.now() - this.startedAt,
      ts: Date.now(),
      checks: results
    }
  }

  async liveness(): Promise<HealthStatus> { return this.report("liveness") }
  async readiness(): Promise<HealthStatus> { return this.report("readiness") }
}
