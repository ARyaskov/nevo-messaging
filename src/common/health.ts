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
  cacheMs?: number
  cache?: { result: HealthCheckResult; at: number }
  inFlight?: Promise<HealthCheckResult>
}

export class HealthRegistry {
  private readonly checks = new Map<string, RegisteredCheck>()
  private readonly startedAt = Date.now()
  private readonly serviceName: string
  private readonly instanceId?: string
  private readonly version?: string
  private readonly defaultTimeoutMs: number
  private readonly defaultCacheMs: number

  constructor(opts: { serviceName: string; instanceId?: string; version?: string; timeoutMs?: number; cacheMs?: number }) {
    this.serviceName = opts.serviceName
    this.instanceId = opts.instanceId
    this.version = opts.version
    this.defaultTimeoutMs = opts.timeoutMs ?? 3000
    this.defaultCacheMs = opts.cacheMs ?? 1000
  }

  register(name: string, fn: HealthCheckFn, opts?: { kind?: HealthKind; timeoutMs?: number; cacheMs?: number }): void {
    this.checks.set(name, { fn, kind: opts?.kind ?? "both", timeoutMs: opts?.timeoutMs, cacheMs: opts?.cacheMs })
  }

  unregister(name: string): void {
    this.checks.delete(name)
  }

  private async runOne(name: string, entry: RegisteredCheck): Promise<HealthCheckResult> {
    const timeoutMs = entry.timeoutMs ?? this.defaultTimeoutMs
    try {
      const invoke = (async () => entry.fn())()
      if (!timeoutMs || timeoutMs <= 0) return await invoke
      const { promise, resolve, reject } = Promise.withResolvers<HealthCheckResult>()
      const timer = setTimeout(() => reject(new Error(`Check "${name}" timed out after ${timeoutMs}ms`)), timeoutMs)
      invoke.then(resolve, reject).finally(() => clearTimeout(timer))
      return await promise
    } catch (err: any) {
      return { status: "down", message: err?.message ?? "check failed" }
    }
  }

  // Per-check result cache + single-flight coalescing: rapid/concurrent probes
  // reuse a fresh cached result and never start more than one probe at a time.
  private runCached(name: string, entry: RegisteredCheck): Promise<HealthCheckResult> {
    const cacheMs = entry.cacheMs ?? this.defaultCacheMs
    if (cacheMs > 0 && entry.cache && Date.now() - entry.cache.at < cacheMs) {
      return Promise.resolve(entry.cache.result)
    }
    if (entry.inFlight) {
      // A probe is already running — serve the last result if we have one, else join it.
      return entry.cache ? Promise.resolve(entry.cache.result) : entry.inFlight
    }
    const inFlight = (async () => {
      try {
        const result = await this.runOne(name, entry)
        entry.cache = { result, at: Date.now() }
        return result
      } finally {
        entry.inFlight = undefined
      }
    })()
    entry.inFlight = inFlight
    return inFlight
  }

  async report(kindFilter: HealthKind | "all" = "all"): Promise<HealthStatus> {
    const selected: Array<[string, RegisteredCheck]> = []
    for (const [name, entry] of this.checks.entries()) {
      if (kindFilter !== "all" && entry.kind !== "both" && entry.kind !== kindFilter) continue
      selected.push([name, entry])
    }
    const settled = await Promise.all(
      selected.map(async ([name, entry]) => {
        const r = await this.runCached(name, entry)
        return [name, { ...r, kind: entry.kind }] as const
      })
    )
    const results: Record<string, { status: "ok" | "down"; message?: string; kind?: HealthKind }> = {}
    for (const [name, r] of settled) results[name] = r
    let overall: "ok" | "degraded" | "down" = "ok"
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
