"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import type { DevToolsEvent } from "@riaskov/nevo-messaging"
import { useEvents } from "@/lib/use-events"
import { formatDuration, formatRelative } from "@/lib/computations"
import { EventTypeBadge } from "./EventTypeBadge"
import { EventBodyModal } from "./EventBodyModal"

interface Chain {
  chainId: string
  events: DevToolsEvent[]
  startTs: number
  endTs: number
  totalMs: number
  rootService?: string
  rootMethod?: string
  callCount: number
  errorCount: number
}

interface Props {
  initialEvents: DevToolsEvent[]
  initialChainId?: string
}

/**
 * Group events by `chainId`, then within each chain sort by timestamp.
 *
 * Events with no chainId are silently skipped — they came from a client that
 * predates the chain-context plumbing (e.g. a custom adapter publishing
 * directly to the bus). The Recent traffic page still shows them.
 */
function groupByChain(events: DevToolsEvent[]): Chain[] {
  const map = new Map<string, DevToolsEvent[]>()
  for (const e of events) {
    if (!e.chainId) continue
    let arr = map.get(e.chainId)
    if (!arr) { arr = []; map.set(e.chainId, arr) }
    arr.push(e)
  }
  const out: Chain[] = []
  for (const [chainId, evs] of map) {
    evs.sort((a, b) => a.ts - b.ts)
    const root = evs[0]
    const errorCount = evs.reduce((n, e) => n + (e.status === "error" || e.type === "error" ? 1 : 0), 0)
    out.push({
      chainId,
      events: evs,
      startTs: evs[0].ts,
      endTs: evs[evs.length - 1].ts,
      totalMs: evs[evs.length - 1].ts - evs[0].ts,
      rootService: root.service,
      rootMethod: root.method,
      callCount: evs.length,
      errorCount
    })
  }
  return out.sort((a, b) => b.startTs - a.startTs)
}

/**
 * For a given chain, count how many request/response pairs it represents.
 * Each call should produce two events (client-side `request`, server-side
 * `response`), so #pairs ≈ events / 2. We surface the raw event count too so
 * misshapen chains (e.g. only request events because the server isn't bridged)
 * are visible.
 */
function pairCount(events: DevToolsEvent[]): number {
  const byUuid = new Map<string, number>()
  for (const e of events) {
    if (!e.uuid) continue
    byUuid.set(e.uuid, (byUuid.get(e.uuid) ?? 0) + 1)
  }
  let pairs = 0
  for (const n of byUuid.values()) if (n >= 2) pairs++
  return pairs
}

export function TracesView({ initialEvents, initialChainId }: Props) {
  const events = useEvents({ initial: initialEvents, maxEvents: 5000 })
  const chains = useMemo(() => groupByChain(events), [events])
  const [openChain, setOpenChain] = useState<string | null>(initialChainId ?? null)
  const [activeEvent, setActiveEvent] = useState<DevToolsEvent | null>(null)
  const [filter, setFilter] = useState<string>("")

  // Auto-scroll to the highlighted chain when arriving via deep link.
  const highlightedRowRef = useRef<HTMLTableRowElement | null>(null)
  useEffect(() => {
    if (initialChainId && highlightedRowRef.current) {
      highlightedRowRef.current.scrollIntoView({ behavior: "smooth", block: "center" })
    }
  }, [initialChainId, chains.length])

  const filtered = useMemo(() => {
    if (!filter.trim()) return chains
    const q = filter.trim().toLowerCase()
    return chains.filter((c) =>
      c.chainId.toLowerCase().includes(q) ||
      (c.rootService ?? "").toLowerCase().includes(q) ||
      (c.rootMethod ?? "").toLowerCase().includes(q) ||
      c.events.some((e) => (e.method ?? "").toLowerCase().includes(q) || (e.service ?? "").toLowerCase().includes(q))
    )
  }, [chains, filter])

  return (
    <>
      <section className="nv-card" style={{ marginBottom: 16 }}>
        <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <span className="nv-muted" style={{ fontSize: 12 }}>Filter</span>
          <input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="chain id / service / method"
            style={{
              padding: "8px 12px",
              background: "var(--panel-2)",
              color: "var(--text)",
              border: "1px solid var(--border)",
              borderRadius: 6,
              fontFamily: "ui-monospace"
            }}
          />
        </label>
      </section>

      {filtered.length === 0 ? (
        <div className="nv-empty">
          {chains.length === 0
            ? "No chains recorded yet. Make a query through a Nevo client — the chain id is stamped automatically."
            : "No chains match the filter."}
        </div>
      ) : (
        <section className="nv-card">
          <table className="nv-table">
            <thead>
              <tr>
                <th style={{ width: 28 }}></th>
                <th>Started</th>
                <th>Root service</th>
                <th>Root method</th>
                <th>Calls</th>
                <th>Errors</th>
                <th>Total</th>
                <th>Chain id</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((chain) => {
                const isOpen = openChain === chain.chainId
                const isHighlight = chain.chainId === initialChainId
                return (
                  <>
                    <tr
                      key={chain.chainId}
                      ref={isHighlight ? highlightedRowRef : null}
                      className={isHighlight ? "nv-highlight" : undefined}
                      style={{ cursor: "pointer" }}
                      onClick={() => setOpenChain(isOpen ? null : chain.chainId)}
                    >
                      <td className="nv-muted" style={{ textAlign: "center", fontFamily: "ui-monospace" }}>
                        {isOpen ? "▾" : "▸"}
                      </td>
                      <td className="nv-mono nv-muted" suppressHydrationWarning>{formatRelative(chain.startTs)}</td>
                      <td className="nv-mono">{chain.rootService ?? "-"}</td>
                      <td className="nv-mono">{chain.rootMethod ?? "-"}</td>
                      <td>
                        {chain.callCount}
                        {(() => {
                          const pairs = pairCount(chain.events)
                          return pairs > 0 && pairs * 2 !== chain.callCount ? (
                            <span className="nv-muted" style={{ marginLeft: 6, fontSize: 11 }}>
                              ({pairs} pair{pairs === 1 ? "" : "s"})
                            </span>
                          ) : null
                        })()}
                      </td>
                      <td>{chain.errorCount > 0 ? <span className="nv-badge err">{chain.errorCount}</span> : <span className="nv-muted">0</span>}</td>
                      <td className="nv-mono">{formatDuration(chain.totalMs)}</td>
                      <td className="nv-mono nv-muted" style={{ fontSize: 11 }}>{chain.chainId}</td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td colSpan={8} style={{ padding: 0, background: "var(--bg)" }}>
                          <ChainDetail chain={chain} onViewBody={setActiveEvent} />
                        </td>
                      </tr>
                    )}
                  </>
                )
              })}
            </tbody>
          </table>
        </section>
      )}

      <EventBodyModal event={activeEvent} onClose={() => setActiveEvent(null)} />
    </>
  )
}

interface ChainDetailProps {
  chain: Chain
  onViewBody: (event: DevToolsEvent) => void
}

/**
 * One row per event in the chain, ordered by timestamp.
 * The Δt column shows offset from the chain's start, not absolute time, so the
 * sequencing is immediately obvious. Each row carries a "view body" button
 * sharing the same modal as Recent traffic.
 */
function ChainDetail({ chain, onViewBody }: ChainDetailProps) {
  return (
    <div style={{ padding: "12px 16px" }}>
      <table className="nv-table">
        <thead>
          <tr>
            <th style={{ width: 64 }}>Δt</th>
            <th>Type</th>
            <th>Role</th>
            <th>Service</th>
            <th>Method</th>
            <th>Duration</th>
            <th>Status</th>
            <th>Error</th>
            <th style={{ width: 60 }}>Body</th>
          </tr>
        </thead>
        <tbody>
          {chain.events.map((e, i) => (
            <tr key={`${e.ts}-${e.uuid ?? "-"}-${i}`}>
              <td className="nv-mono nv-muted">{formatDuration(e.ts - chain.startTs)}</td>
              <td><EventTypeBadge type={e.type} /></td>
              <td className="nv-muted">{(e.extra as any)?.role ?? "server"}</td>
              <td className="nv-mono">{e.service ?? "-"}</td>
              <td className="nv-mono">{e.method ?? "-"}</td>
              <td className="nv-mono">{e.durationMs ? formatDuration(e.durationMs) : "-"}</td>
              <td>{e.status === "ok" ? <span className="nv-badge ok">ok</span> : e.status === "error" ? <span className="nv-badge err">error</span> : "-"}</td>
              <td className="nv-muted" style={{ fontSize: 12 }}>{e.error?.message ?? "-"}</td>
              <td>
                <button
                  type="button"
                  className="nv-view-body"
                  onClick={(ev) => { ev.stopPropagation(); onViewBody(e) }}
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
    </div>
  )
}
