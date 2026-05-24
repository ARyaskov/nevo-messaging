"use client"

import { useMemo } from "react"
import type { DevToolsEvent, CircuitInfo } from "@riaskov/nevo-messaging"
import { useEvents } from "@/lib/use-events"
import { deriveCircuitStates, formatRelative } from "@/lib/computations"

interface Props {
  initialEvents: DevToolsEvent[]
  initialCircuits: CircuitInfo[]
}

export function CircuitDashboard({ initialEvents, initialCircuits }: Props) {
  const events = useEvents({ initial: initialEvents, maxEvents: 5000 })

  const derived = useMemo(() => deriveCircuitStates(events), [events])

  const merged = useMemo(() => {
    const map = new Map<string, { key: string; service: string; method: string; state: string; failures: number; lastTransitionAt: number; lastError?: string }>()
    for (const c of initialCircuits) {
      map.set(c.key, { key: c.key, service: c.service, method: c.method, state: c.state, failures: c.failures, lastTransitionAt: c.lastTransitionAt, lastError: c.lastError })
    }
    for (const c of derived) {
      const existing = map.get(c.key)
      if (!existing || c.lastTransitionAt >= existing.lastTransitionAt) {
        map.set(c.key, { key: c.key, service: c.service, method: c.method, state: c.state, failures: c.failures, lastTransitionAt: c.lastTransitionAt })
      }
    }
    return [...map.values()].sort((a, b) => b.lastTransitionAt - a.lastTransitionAt)
  }, [initialCircuits, derived])

  const counts = useMemo(() => {
    let open = 0, half = 0, closed = 0
    for (const c of merged) {
      if (c.state === "open") open++
      else if (c.state === "half-open") half++
      else closed++
    }
    return { open, half, closed, total: merged.length }
  }, [merged])

  return (
    <>
      <section className="nv-grid" style={{ marginBottom: 16 }}>
        <div className="nv-card"><h2>Total circuits</h2><div className="nv-value">{counts.total}</div></div>
        <div className="nv-card"><h2>Open</h2><div className="nv-value" style={{ color: "var(--err)" }}>{counts.open}</div></div>
        <div className="nv-card"><h2>Half-open</h2><div className="nv-value" style={{ color: "var(--warn)" }}>{counts.half}</div></div>
        <div className="nv-card"><h2>Closed</h2><div className="nv-value" style={{ color: "var(--ok)" }}>{counts.closed}</div></div>
      </section>

      <section className="nv-card" style={{ marginBottom: 16 }}>
        <h2>Current state</h2>
        {merged.length === 0 ? (
          <div className="nv-empty">No circuits have transitioned yet. Enable a circuit breaker in client options and trigger a few failures.</div>
        ) : (
          <table className="nv-table">
            <thead>
              <tr><th>Service</th><th>Method</th><th>State</th><th>Failures</th><th>Last transition</th><th>Last error</th></tr>
            </thead>
            <tbody>
              {merged.map((c) => (
                <tr key={c.key}>
                  <td className="nv-mono"><a href={`/services/${encodeURIComponent(c.service)}`}>{c.service}</a></td>
                  <td className="nv-mono">{c.method}</td>
                  <td>
                    {c.state === "closed" ? <span className="nv-badge ok">closed</span>
                      : c.state === "half-open" ? <span className="nv-badge warn">half-open</span>
                      : <span className="nv-badge err">open</span>}
                  </td>
                  <td>{c.failures}</td>
                  <td className="nv-muted" suppressHydrationWarning>{c.lastTransitionAt ? formatRelative(c.lastTransitionAt) : "—"}</td>
                  <td className="nv-muted">{c.lastError ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section className="nv-card">
        <h2>Recent transitions</h2>
        {derived.length === 0 || derived.every((d) => d.history.length === 0) ? (
          <div className="nv-empty">No transitions captured in current event window.</div>
        ) : (
          <table className="nv-table">
            <thead>
              <tr><th>Time</th><th>Service</th><th>Method</th><th>From</th><th>To</th></tr>
            </thead>
            <tbody>
              {derived.flatMap((c) => c.history.slice().reverse().slice(0, 5).map((h) => (
                <tr key={`${c.key}:${h.ts}`}>
                  <td className="nv-mono nv-muted" suppressHydrationWarning>{new Date(h.ts).toLocaleTimeString()}</td>
                  <td className="nv-mono">{c.service}</td>
                  <td className="nv-mono">{c.method}</td>
                  <td>{h.from || "—"}</td>
                  <td>
                    {h.to === "closed" ? <span className="nv-badge ok">closed</span>
                      : h.to === "half-open" ? <span className="nv-badge warn">half-open</span>
                      : <span className="nv-badge err">open</span>}
                  </td>
                </tr>
              )))}
            </tbody>
          </table>
        )}
      </section>
    </>
  )
}
