"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import type { DevToolsEvent } from "@riaskov/nevo-messaging"
import { useEvents } from "@/lib/use-events"
import { rankMethods, RankBy, formatDuration, formatErrorRate, formatRelative } from "@/lib/computations"

interface Props {
  initialEvents: DevToolsEvent[]
  highlightService?: string
  highlightMethod?: string
}

const TABS: { key: RankBy; label: string; help: string }[] = [
  { key: "slowest", label: "Slowest", help: "Top methods sorted by p95 latency" },
  { key: "most-called", label: "Most called", help: "Total invocations across recent events" },
  { key: "most-errors", label: "Most errors", help: "Absolute count of failed responses" },
  { key: "error-rate", label: "Worst error rate", help: "Error % among methods with ≥5 calls" }
]

export function MethodsLeaderboard({ initialEvents, highlightService, highlightMethod }: Props) {
  const events = useEvents({ initial: initialEvents, maxEvents: 5000 })
  const [tab, setTab] = useState<RankBy>("slowest")
  const data = useMemo(() => rankMethods(events, tab, 50), [events, tab])
  const help = TABS.find((t) => t.key === tab)!.help

  // The highlight key persists across tab switches so the user can flip
  // between Slowest / Most called / Most errors and still see "their" row.
  const highlightKey = highlightService && highlightMethod
    ? `${highlightService}:${highlightMethod}`
    : null

  // Notify the user when the deep-linked row is not in the current tab's
  // top-50 cut — common when navigating from the dashboard's "Recent traffic"
  // to a method that is rare overall.
  const notFoundInCurrentTab = highlightKey !== null && !data.some((s) => `${s.service}:${s.method}` === highlightKey)

  const highlightedRowRef = useRef<HTMLTableRowElement | null>(null)
  useEffect(() => {
    if (!highlightedRowRef.current) return
    highlightedRowRef.current.scrollIntoView({ behavior: "smooth", block: "center" })
  }, [highlightKey, tab])

  return (
    <section className="nv-card">
      <div className="nv-row" style={{ marginBottom: 12, gap: 8, flexWrap: "wrap" }}>
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            style={{
              padding: "6px 12px", borderRadius: 6, cursor: "pointer",
              background: tab === t.key ? "var(--accent)" : "var(--panel-2)",
              color: tab === t.key ? "white" : "var(--text)",
              border: "1px solid var(--border)",
              fontSize: 13
            }}
          >
            {t.label}
          </button>
        ))}
      </div>
      <p className="nv-muted" style={{ marginBottom: 12 }}>{help}</p>

      {highlightKey && notFoundInCurrentTab && (
        <div
          className="nv-muted"
          style={{
            marginBottom: 12, padding: "8px 12px", borderRadius: 6,
            border: "1px dashed var(--border)", background: "var(--panel-2)",
            fontSize: 13
          }}
        >
          <strong style={{ color: "var(--text)" }}>{highlightService}.{highlightMethod}</strong>
          {" "}is not in the top-50 for &laquo;{TABS.find((t) => t.key === tab)!.label}&raquo;. Try another tab.
        </div>
      )}

      {data.length === 0 ? (
        <div className="nv-empty">No method traffic recorded yet.</div>
      ) : (
        <table className="nv-table">
          <thead>
            <tr>
              <th>#</th><th>Service</th><th>Method</th>
              <th>Calls</th><th>Errors</th><th>Err rate</th>
              <th>p50</th><th>p95</th><th>p99</th>
              <th>~RPS</th><th>Last</th>
            </tr>
          </thead>
          <tbody>
            {data.map((s, i) => {
              const key = `${s.service}:${s.method}`
              const isHighlight = key === highlightKey
              return (
                <tr
                  key={key}
                  ref={isHighlight ? highlightedRowRef : null}
                  className={isHighlight ? "nv-highlight" : undefined}
                >
                  <td className="nv-muted">{i + 1}</td>
                  <td className="nv-mono"><a href={`/services/${encodeURIComponent(s.service)}`}>{s.service}</a></td>
                  <td className="nv-mono">{s.method}</td>
                  <td>{s.total}</td>
                  <td>{s.error}</td>
                  <td>{formatErrorRate(s.errorRate)}</td>
                  <td>{formatDuration(s.p50)}</td>
                  <td>{formatDuration(s.p95)}</td>
                  <td>{formatDuration(s.p99)}</td>
                  <td>{s.rps.toFixed(2)}</td>
                  <td className="nv-muted" suppressHydrationWarning>{formatRelative(s.lastTs)}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}
    </section>
  )
}
