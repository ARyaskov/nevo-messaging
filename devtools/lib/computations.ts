import type { DevToolsEvent } from "@riaskov/nevo-messaging"

export interface MethodStats {
  service: string
  method: string
  total: number
  success: number
  error: number
  errorRate: number
  rps: number
  durations: number[]
  p50: number
  p95: number
  p99: number
  avg: number
  lastTs: number
}

export interface OverviewStats {
  totalEvents: number
  totalErrors: number
  services: number
  methods: number
  rps: number
  errorRate: number
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))
  return sorted[idx]
}

export function computeMethodStats(events: DevToolsEvent[], windowMs = 60_000): MethodStats[] {
  const now = Date.now()
  const groups = new Map<string, MethodStats>()
  for (const e of events) {
    if (!e.method || !e.service) continue
    if (e.type !== "request" && e.type !== "response" && e.type !== "error") continue
    const key = `${e.service}:${e.method}`
    let s = groups.get(key)
    if (!s) {
      s = { service: e.service, method: e.method, total: 0, success: 0, error: 0, errorRate: 0, rps: 0, durations: [], p50: 0, p95: 0, p99: 0, avg: 0, lastTs: 0 }
      groups.set(key, s)
    }
    s.total++
    if (e.status === "ok") s.success++
    else if (e.status === "error") s.error++
    if (typeof e.durationMs === "number") s.durations.push(e.durationMs)
    if (e.ts > s.lastTs) s.lastTs = e.ts
  }
  for (const s of groups.values()) {
    s.errorRate = s.total ? s.error / s.total : 0
    const sorted = s.durations.slice().sort((a, b) => a - b)
    s.p50 = percentile(sorted, 50)
    s.p95 = percentile(sorted, 95)
    s.p99 = percentile(sorted, 99)
    s.avg = sorted.length ? sorted.reduce((a, b) => a + b, 0) / sorted.length : 0
    const recent = events.filter((e) => e.service === s.service && e.method === s.method && now - e.ts <= windowMs && (e.type === "request" || e.type === "response" || e.type === "error")).length
    s.rps = recent / (windowMs / 1000)
  }
  return [...groups.values()]
}

export function computeOverview(events: DevToolsEvent[], windowMs = 60_000): OverviewStats {
  const total = events.length
  const errors = events.filter((e) => e.status === "error" || e.type === "error").length
  const services = new Set<string>()
  const methods = new Set<string>()
  let recent = 0
  const now = Date.now()
  for (const e of events) {
    if (e.service) services.add(e.service)
    if (e.service && e.method) methods.add(`${e.service}:${e.method}`)
    if (now - e.ts <= windowMs && (e.type === "request" || e.type === "response" || e.type === "error")) recent++
  }
  return {
    totalEvents: total,
    totalErrors: errors,
    services: services.size,
    methods: methods.size,
    rps: recent / (windowMs / 1000),
    errorRate: total > 0 ? errors / total : 0
  }
}

export interface ServiceStats {
  serviceName: string
  totalCalls: number
  errors: number
  errorRate: number
  rps: number
  topMethods: { method: string; total: number; errorRate: number; p95: number }[]
  lastTs: number
}

export function computeServiceStats(events: DevToolsEvent[], windowMs = 60_000): ServiceStats[] {
  const stats = computeMethodStats(events, windowMs)
  const byService = new Map<string, ServiceStats>()
  for (const s of stats) {
    let v = byService.get(s.service)
    if (!v) {
      v = { serviceName: s.service, totalCalls: 0, errors: 0, errorRate: 0, rps: 0, topMethods: [], lastTs: 0 }
      byService.set(s.service, v)
    }
    v.totalCalls += s.total
    v.errors += s.error
    v.rps += s.rps
    if (s.lastTs > v.lastTs) v.lastTs = s.lastTs
    v.topMethods.push({ method: s.method, total: s.total, errorRate: s.errorRate, p95: s.p95 })
  }
  for (const v of byService.values()) {
    v.errorRate = v.totalCalls > 0 ? v.errors / v.totalCalls : 0
    v.topMethods.sort((a, b) => b.total - a.total)
    v.topMethods = v.topMethods.slice(0, 8)
  }
  return [...byService.values()]
}

export type RankBy = "slowest" | "most-called" | "most-errors" | "error-rate"

export function rankMethods(events: DevToolsEvent[], rankBy: RankBy, limit = 25, windowMs = 60_000): MethodStats[] {
  const stats = computeMethodStats(events, windowMs)
  const filtered = rankBy === "error-rate" ? stats.filter((s) => s.total >= 5) : stats
  const sorted = filtered.sort((a, b) => {
    switch (rankBy) {
      case "slowest": return b.p95 - a.p95
      case "most-called": return b.total - a.total
      case "most-errors": return b.error - a.error
      case "error-rate": return b.errorRate - a.errorRate
    }
  })
  return sorted.slice(0, limit)
}

export interface CircuitView {
  key: string
  service: string
  method: string
  state: "closed" | "open" | "half-open"
  failures: number
  lastTransitionAt: number
  history: { ts: number; from: string; to: string }[]
}

export function deriveCircuitStates(events: DevToolsEvent[]): CircuitView[] {
  const map = new Map<string, CircuitView>()
  for (const e of events) {
    if (e.type !== "circuit") continue
    const key = (e.extra?.key as string) ?? `${e.service}:${e.method}`
    let v = map.get(key)
    if (!v) {
      v = { key, service: e.service ?? "", method: e.method ?? "", state: "closed", failures: 0, lastTransitionAt: 0, history: [] }
      map.set(key, v)
    }
    v.state = (e.extra?.to as any) ?? v.state
    v.failures = (e.extra?.failures as number) ?? v.failures
    v.lastTransitionAt = e.ts
    v.history.push({ ts: e.ts, from: String(e.extra?.from ?? ""), to: String(e.extra?.to ?? "") })
  }
  for (const v of map.values()) v.history = v.history.slice(-20)
  return [...map.values()].sort((a, b) => b.lastTransitionAt - a.lastTransitionAt)
}

export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return "—"
  if (ms < 1) return `${(ms * 1000).toFixed(0)}µs`
  if (ms < 1000) return `${ms.toFixed(1)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

export function formatErrorRate(r: number): string {
  if (!Number.isFinite(r) || r === 0) return "0%"
  if (r < 0.01) return `${(r * 100).toFixed(2)}%`
  if (r < 0.1) return `${(r * 100).toFixed(1)}%`
  return `${(r * 100).toFixed(0)}%`
}

export function formatRelative(ts: number): string {
  const diff = Date.now() - ts
  if (diff < 1000) return "just now"
  if (diff < 60_000) return `${Math.floor(diff / 1000)}s ago`
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`
  return `${Math.floor(diff / 86_400_000)}d ago`
}
