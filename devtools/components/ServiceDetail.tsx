"use client"

import { useMemo } from "react"
import type { DevToolsEvent, ServiceInfo, CircuitInfo } from "@riaskov/nevo-messaging"
import { useEvents } from "@/lib/use-events"
import { computeMethodStats, formatDuration, formatErrorRate, formatRelative } from "@/lib/computations"

interface Props {
  serviceName: string
  initialEvents: DevToolsEvent[]
  initialServiceInfo: ServiceInfo | undefined
  initialCircuits: CircuitInfo[]
}

export function ServiceDetail({ serviceName, initialEvents, initialServiceInfo, initialCircuits }: Props) {
  const events = useEvents({ initial: initialEvents, maxEvents: 5000 })

  const serviceEvents = useMemo(() => events.filter((e) => e.service === serviceName), [events, serviceName])

  const methodStats = useMemo(() => computeMethodStats(serviceEvents).sort((a, b) => b.total - a.total), [serviceEvents])

  const totals = useMemo(() => {
    const total = methodStats.reduce((s, m) => s + m.total, 0)
    const errors = methodStats.reduce((s, m) => s + m.error, 0)
    const rps = methodStats.reduce((s, m) => s + m.rps, 0)
    const avgP95 = methodStats.length ? methodStats.reduce((s, m) => s + m.p95, 0) / methodStats.length : 0
    return { total, errors, rps, avgP95 }
  }, [methodStats])

  const recentErrors = useMemo(() => serviceEvents.filter((e) => e.type === "error" || e.status === "error").slice(-12).reverse(), [serviceEvents])

  return (
    <>
      <h1 className="nv-h1 nv-mono">{serviceName}</h1>

      <section className="nv-grid" style={{ marginBottom: 16 }}>
        <div className="nv-card"><h2>Calls</h2><div className="nv-value">{totals.total}</div></div>
        <div className="nv-card"><h2>Errors</h2><div className="nv-value" style={{ color: "var(--err)" }}>{totals.errors}</div></div>
        <div className="nv-card"><h2>~RPS (60s)</h2><div className="nv-value">{totals.rps.toFixed(2)}</div></div>
        <div className="nv-card"><h2>Avg p95</h2><div className="nv-value">{formatDuration(totals.avgP95)}</div></div>
        <div className="nv-card"><h2>Methods declared</h2><div className="nv-value">{initialServiceInfo?.methods.length ?? "—"}</div></div>
      </section>

      {initialServiceInfo?.accessControl?.rules?.length ? (
        <section className="nv-card" style={{ marginBottom: 16 }}>
          <h2>ACL rules ({initialServiceInfo.accessControl.rules.length})</h2>
          <table className="nv-table">
            <thead><tr><th>Topic</th><th>Method</th><th>Allow</th><th>Deny</th></tr></thead>
            <tbody>
              {initialServiceInfo.accessControl.rules.map((r, i) => (
                <tr key={i}>
                  <td className="nv-mono">{r.topic ?? "*"}</td>
                  <td className="nv-mono">{r.method ?? "*"}</td>
                  <td>{r.allow?.join(", ") || <span className="nv-muted">—</span>}</td>
                  <td>{r.deny?.join(", ") || <span className="nv-muted">—</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      ) : null}

      <section className="nv-card" style={{ marginBottom: 16 }}>
        <h2>Methods</h2>
        {methodStats.length === 0 ? (
          <div className="nv-empty">No traffic recorded yet for this service.</div>
        ) : (
          <table className="nv-table">
            <thead>
              <tr>
                <th>Method</th>
                <th>Calls</th>
                <th>Errors</th>
                <th>Err rate</th>
                <th>~RPS</th>
                <th>p50</th>
                <th>p95</th>
                <th>p99</th>
                <th>avg</th>
                <th>Last</th>
              </tr>
            </thead>
            <tbody>
              {methodStats.map((s) => (
                <tr key={s.method}>
                  <td className="nv-mono">{s.method}</td>
                  <td>{s.total}</td>
                  <td>{s.error}</td>
                  <td>{formatErrorRate(s.errorRate)}</td>
                  <td>{s.rps.toFixed(2)}</td>
                  <td>{formatDuration(s.p50)}</td>
                  <td>{formatDuration(s.p95)}</td>
                  <td>{formatDuration(s.p99)}</td>
                  <td>{formatDuration(s.avg)}</td>
                  <td className="nv-muted" suppressHydrationWarning>{formatRelative(s.lastTs)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {initialCircuits.length > 0 && (
        <section className="nv-card" style={{ marginBottom: 16 }}>
          <h2>Circuit breakers</h2>
          <table className="nv-table">
            <thead><tr><th>Method</th><th>State</th><th>Failures</th><th>Last transition</th></tr></thead>
            <tbody>
              {initialCircuits.map((c) => (
                <tr key={c.key}>
                  <td className="nv-mono">{c.method}</td>
                  <td>
                    {c.state === "closed" ? <span className="nv-badge ok">closed</span>
                      : c.state === "half-open" ? <span className="nv-badge warn">half-open</span>
                      : <span className="nv-badge err">open</span>}
                  </td>
                  <td>{c.failures}</td>
                  <td className="nv-muted" suppressHydrationWarning>{formatRelative(c.lastTransitionAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}

      {recentErrors.length > 0 && (
        <section className="nv-card">
          <h2>Recent errors</h2>
          <table className="nv-table">
            <thead><tr><th>Time</th><th>Method</th><th>Code</th><th>Message</th><th>Duration</th></tr></thead>
            <tbody>
              {recentErrors.map((e, i) => (
                <tr key={`${e.ts}-${i}`}>
                  <td className="nv-mono nv-muted" suppressHydrationWarning>{new Date(e.ts).toLocaleTimeString()}</td>
                  <td className="nv-mono">{e.method}</td>
                  <td>{e.error?.code ?? "—"}</td>
                  <td>{e.error?.message ?? <span className="nv-muted">—</span>}</td>
                  <td>{e.durationMs ? formatDuration(e.durationMs) : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
    </>
  )
}
