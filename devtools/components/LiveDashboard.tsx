"use client"

import { useMemo, useState } from "react"
import type { DevToolsEvent } from "@riaskov/nevo-messaging"
import { useEvents } from "@/lib/use-events"
import { computeOverview, formatDuration, formatErrorRate, rankMethods } from "@/lib/computations"
import { EventTypeBadge } from "./EventTypeBadge"
import { EventBodyModal } from "./EventBodyModal"

interface Props { initialEvents: DevToolsEvent[] }

// Build a deep link to /methods that highlights one (service, method) row.
// Service may be undefined for events without a normalized service field.
function methodLink(service: string | undefined, method: string): string {
  const params = new URLSearchParams()
  if (service) params.set("service", service)
  params.set("method", method)
  return `/methods?${params.toString()}`
}

export function LiveDashboard({ initialEvents }: Props) {
  const events = useEvents({ initial: initialEvents, maxEvents: 5000 })

  const overview = useMemo(() => computeOverview(events), [events])
  const slowest = useMemo(() => rankMethods(events, "slowest", 5), [events])
  const mostErrors = useMemo(() => rankMethods(events, "most-errors", 5), [events])

  const recent = useMemo(() => events.slice().reverse().slice(0, 50), [events])

  // Single shared modal for the "view body" action — clicking the `{}` button
  // on any row opens the same modal pointed at the chosen event.
  const [activeEvent, setActiveEvent] = useState<DevToolsEvent | null>(null)

  return (
    <div>
      <section className="nv-grid" style={{ marginBottom: 24 }}>
        <div className="nv-card"><h2>Total events</h2><div className="nv-value">{overview.totalEvents}</div></div>
        <div className="nv-card"><h2>Errors</h2><div className="nv-value" style={{ color: "var(--err)" }}>{overview.totalErrors}</div></div>
        <div className="nv-card"><h2>Err rate</h2><div className="nv-value">{formatErrorRate(overview.errorRate)}</div></div>
        <div className="nv-card"><h2>Services</h2><div className="nv-value"><a href="/services">{overview.services}</a></div></div>
        <div className="nv-card"><h2>Methods</h2><div className="nv-value"><a href="/methods">{overview.methods}</a></div></div>
        <div className="nv-card"><h2>~RPS (60s)</h2><div className="nv-value">{overview.rps.toFixed(2)}</div></div>
      </section>

      <section className="nv-grid" style={{ marginBottom: 24 }}>
        <div className="nv-card" style={{ gridColumn: "span 2" }}>
          <h2>Top 5 slowest (p95)</h2>
          {slowest.length === 0 ? <div className="nv-empty">No data</div> : (
            <table className="nv-table">
              <thead><tr><th>Service</th><th>Method</th><th>p95</th><th>p99</th><th>Calls</th></tr></thead>
              <tbody>
                {slowest.map((s) => (
                  <tr key={`${s.service}:${s.method}`}>
                    <td className="nv-mono"><a href={`/services/${encodeURIComponent(s.service)}`}>{s.service}</a></td>
                    <td className="nv-mono"><a href={methodLink(s.service, s.method)}>{s.method}</a></td>
                    <td>{formatDuration(s.p95)}</td>
                    <td>{formatDuration(s.p99)}</td>
                    <td>{s.total}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        <div className="nv-card" style={{ gridColumn: "span 2" }}>
          <h2>Top 5 by errors</h2>
          {mostErrors.length === 0 ? <div className="nv-empty">No errors recorded</div> : (
            <table className="nv-table">
              <thead><tr><th>Service</th><th>Method</th><th>Errors</th><th>Err rate</th><th>Calls</th></tr></thead>
              <tbody>
                {mostErrors.map((s) => (
                  <tr key={`${s.service}:${s.method}`}>
                    <td className="nv-mono"><a href={`/services/${encodeURIComponent(s.service)}`}>{s.service}</a></td>
                    <td className="nv-mono"><a href={methodLink(s.service, s.method)}>{s.method}</a></td>
                    <td>{s.error}</td>
                    <td>{formatErrorRate(s.errorRate)}</td>
                    <td>{s.total}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </section>

      <section className="nv-card">
        <h2 style={{ marginBottom: 12 }}>Recent traffic</h2>
        {recent.length === 0 ? (
          <div className="nv-empty">No events yet. Connect a service to start streaming.</div>
        ) : (
          <table className="nv-table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Type</th>
                <th>Service</th>
                <th>Method</th>
                <th>Status</th>
                <th>Duration</th>
                <th>UUID</th>
                <th>Chain</th>
                <th>Body</th>
              </tr>
            </thead>
            <tbody>
              {recent.map((e, i) => (
                <tr key={`${e.ts}-${i}`}>
                  <td className="nv-mono nv-muted" suppressHydrationWarning>{new Date(e.ts).toLocaleTimeString()}</td>
                  <td><EventTypeBadge type={e.type} /></td>
                  <td className="nv-mono">{e.service ? <a href={`/services/${encodeURIComponent(e.service)}`}>{e.service}</a> : "-"}</td>
                  <td className="nv-mono">
                    {e.method ? <a href={methodLink(e.service, e.method)}>{e.method}</a> : "-"}
                  </td>
                  <td>
                    {e.status === "ok" ? <span className="nv-badge ok">ok</span> : e.status === "error" ? <span className="nv-badge err">error</span> : "-"}
                  </td>
                  <td className="nv-mono">{e.durationMs ? formatDuration(e.durationMs) : "-"}</td>
                  <td className="nv-mono nv-muted">{e.uuid ? <a href={`/trace?uuid=${encodeURIComponent(e.uuid)}`}>{e.uuid}</a> : ""}</td>
                  <td className="nv-mono nv-muted" style={{ fontSize: 11 }}>
                    {e.chainId ? <a href={`/traces?chain=${encodeURIComponent(e.chainId)}`} title={e.chainId}>{e.chainId.slice(0, 8)}…</a> : ""}
                  </td>
                  <td>
                    <button
                      type="button"
                      className="nv-view-body"
                      onClick={() => setActiveEvent(e)}
                      title="View event body"
                      aria-label="View event body"
                    >
                      {"{ }"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <EventBodyModal event={activeEvent} onClose={() => setActiveEvent(null)} />
    </div>
  )
}
