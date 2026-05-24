"use client"

import { useMemo, useState } from "react"
import type { DevToolsEvent } from "@riaskov/nevo-messaging"
import { useEvents } from "@/lib/use-events"
import { formatDuration, formatRelative } from "@/lib/computations"
import { EventTypeBadge } from "./EventTypeBadge"
import { EventBodyModal } from "./EventBodyModal"

interface Props {
  initialEvents: DevToolsEvent[]
  initialUuid?: string
}

export function TraceViewer({ initialEvents, initialUuid }: Props) {
  const events = useEvents({ initial: initialEvents, maxEvents: 5000 })
  const [uuid, setUuid] = useState(initialUuid ?? "")
  const [activeEvent, setActiveEvent] = useState<DevToolsEvent | null>(null)

  const trace = useMemo(() => {
    if (!uuid) return null
    const matches = events.filter((e) => e.uuid === uuid).sort((a, b) => a.ts - b.ts)
    if (matches.length === 0) return null
    const firstTs = matches[0].ts
    const lastTs = matches[matches.length - 1].ts
    return { matches, firstTs, totalMs: lastTs - firstTs }
  }, [events, uuid])

  return (
    <>
      <section className="nv-card" style={{ marginBottom: 16 }}>
        <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <span className="nv-muted" style={{ fontSize: 12 }}>UUID</span>
          <input
            value={uuid}
            onChange={(e) => setUuid(e.target.value)}
            placeholder="Paste a request uuid from the Overview / Errors page"
            style={{ padding: "8px 12px", background: "var(--panel-2)", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 6, fontFamily: "ui-monospace" }}
          />
        </label>
      </section>

      {!uuid ? (
        <div className="nv-empty">Enter a UUID above to drill into a request trace.</div>
      ) : !trace ? (
        <div className="nv-empty">No events with that UUID in the current buffer. Trace may have aged out.</div>
      ) : (
        <section className="nv-card">
          <div className="nv-row" style={{ marginBottom: 12, gap: 24 }}>
            <span><strong>Spans:</strong> {trace.matches.length}</span>
            <span><strong>Total duration:</strong> {formatDuration(trace.totalMs)}</span>
            <span className="nv-muted" suppressHydrationWarning>Started {formatRelative(trace.firstTs)}</span>
          </div>
          <table className="nv-table">
            <thead><tr><th>Δt</th><th>Type</th><th>Role</th><th>Service</th><th>Method</th><th>Duration</th><th>Status</th><th>Error</th><th>Body</th></tr></thead>
            <tbody>
              {trace.matches.map((e, i) => (
                <tr key={i}>
                  <td className="nv-mono nv-muted">{formatDuration(e.ts - trace.firstTs)}</td>
                  <td><EventTypeBadge type={e.type} /></td>
                  <td>{(e.extra as any)?.role ?? "server"}</td>
                  <td className="nv-mono">{e.service ?? "-"}</td>
                  <td className="nv-mono">{e.method ?? "-"}</td>
                  <td>{e.durationMs ? formatDuration(e.durationMs) : "-"}</td>
                  <td>{e.status === "ok" ? <span className="nv-badge ok">ok</span> : e.status === "error" ? <span className="nv-badge err">error</span> : "-"}</td>
                  <td>{e.error?.message ?? "-"}</td>
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
        </section>
      )}

      <EventBodyModal event={activeEvent} onClose={() => setActiveEvent(null)} />
    </>
  )
}
