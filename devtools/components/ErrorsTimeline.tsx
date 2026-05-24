"use client"

import { useMemo, useState } from "react"
import type { DevToolsEvent } from "@riaskov/nevo-messaging"
import { useEvents } from "@/lib/use-events"
import { formatDuration, formatRelative } from "@/lib/computations"

interface Props { initialEvents: DevToolsEvent[] }

export function ErrorsTimeline({ initialEvents }: Props) {
  const events = useEvents({ initial: initialEvents, maxEvents: 5000 })
  const [serviceFilter, setServiceFilter] = useState<string>("")
  const [codeFilter, setCodeFilter] = useState<string>("")

  const errors = useMemo(() => events.filter((e) => e.type === "error" || e.status === "error"), [events])

  const services = useMemo(() => Array.from(new Set(errors.map((e) => e.service).filter(Boolean))) as string[], [errors])
  const codes = useMemo(() => Array.from(new Set(errors.map((e) => e.error?.code).filter((c) => c !== undefined))) as number[], [errors])

  const filtered = useMemo(() => {
    return errors
      .filter((e) => !serviceFilter || e.service === serviceFilter)
      .filter((e) => !codeFilter || String(e.error?.code ?? "") === codeFilter)
      .slice(-500)
      .reverse()
  }, [errors, serviceFilter, codeFilter])

  const summary = useMemo(() => {
    const grouped = new Map<string, { code: number | string; message: string; count: number; lastTs: number }>()
    for (const e of errors) {
      const code = e.error?.code ?? "-"
      const message = e.error?.message ?? e.method ?? "unknown"
      const k = `${code}:${message}`
      const v = grouped.get(k) ?? { code, message, count: 0, lastTs: 0 }
      v.count++
      if (e.ts > v.lastTs) v.lastTs = e.ts
      grouped.set(k, v)
    }
    return [...grouped.values()].sort((a, b) => b.count - a.count).slice(0, 8)
  }, [errors])

  return (
    <>
      <section className="nv-grid" style={{ marginBottom: 16 }}>
        <div className="nv-card"><h2>Errors (window)</h2><div className="nv-value" style={{ color: "var(--err)" }}>{errors.length}</div></div>
        <div className="nv-card"><h2>Unique services</h2><div className="nv-value">{services.length}</div></div>
        <div className="nv-card"><h2>Unique codes</h2><div className="nv-value">{codes.length}</div></div>
      </section>

      {summary.length > 0 && (
        <section className="nv-card" style={{ marginBottom: 16 }}>
          <h2>Top errors</h2>
          <table className="nv-table">
            <thead><tr><th>Count</th><th>Code</th><th>Message</th><th>Last</th></tr></thead>
            <tbody>
              {summary.map((g, i) => (
                <tr key={i}>
                  <td>{g.count}</td>
                  <td>{g.code}</td>
                  <td className="nv-mono">{g.message}</td>
                  <td className="nv-muted" suppressHydrationWarning>{formatRelative(g.lastTs)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}

      <section className="nv-card">
        <div className="nv-row" style={{ marginBottom: 12, gap: 12 }}>
          <select
            value={serviceFilter}
            onChange={(e) => setServiceFilter(e.target.value)}
            style={{ padding: "6px 10px", background: "var(--panel-2)", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 6 }}
          >
            <option value="">All services</option>
            {services.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
          <select
            value={codeFilter}
            onChange={(e) => setCodeFilter(e.target.value)}
            style={{ padding: "6px 10px", background: "var(--panel-2)", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 6 }}
          >
            <option value="">All codes</option>
            {codes.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          <span className="nv-muted">{filtered.length} errors shown</span>
        </div>

        {filtered.length === 0 ? (
          <div className="nv-empty">No matching errors.</div>
        ) : (
          <table className="nv-table">
            <thead>
              <tr><th>Time</th><th>Service</th><th>Method</th><th>Code</th><th>Message</th><th>Duration</th><th>UUID</th></tr>
            </thead>
            <tbody>
              {filtered.map((e, i) => (
                <tr key={`${e.ts}-${i}`}>
                  <td className="nv-mono nv-muted" suppressHydrationWarning>{new Date(e.ts).toLocaleTimeString()}</td>
                  <td className="nv-mono">{e.service ? <a href={`/services/${encodeURIComponent(e.service)}`}>{e.service}</a> : "—"}</td>
                  <td className="nv-mono">{e.method ?? "—"}</td>
                  <td>{e.error?.code ?? "—"}</td>
                  <td>{e.error?.message ?? <span className="nv-muted">—</span>}</td>
                  <td>{e.durationMs ? formatDuration(e.durationMs) : "—"}</td>
                  <td className="nv-mono nv-muted">{e.uuid ?? ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </>
  )
}
