"use client"

import { useState } from "react"

interface Props {
  services: { name: string; methods: string[] }[]
}

export function ReplayConsole({ services }: Props) {
  const [service, setService] = useState(services[0]?.name ?? "")
  const [method, setMethod] = useState(services[0]?.methods?.[0] ?? "")
  const [paramsText, setParamsText] = useState('{\n  "id": 1\n}')
  const [status, setStatus] = useState<string>("")

  const currentMethods = services.find((s) => s.name === service)?.methods ?? []

  const submit = async () => {
    setStatus("sending…")
    let params: unknown = {}
    try { params = JSON.parse(paramsText) } catch { setStatus("Invalid JSON"); return }
    const res = await fetch("/api/replay", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ service, method, params })
    })
    const json = await res.json().catch(() => null)
    setStatus(res.ok ? `Queued: ${JSON.stringify(json)}` : `Error: ${res.status}`)
  }

  return (
    <section className="nv-card">
      <div className="nv-row" style={{ gap: 12, marginBottom: 12, flexWrap: "wrap" }}>
        <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <span className="nv-muted" style={{ fontSize: 12 }}>Service</span>
          <select value={service} onChange={(e) => { setService(e.target.value); setMethod(services.find((s) => s.name === e.target.value)?.methods?.[0] ?? "") }}
            style={{ padding: "6px 10px", background: "var(--panel-2)", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 6 }}>
            {services.map((s) => <option key={s.name} value={s.name}>{s.name}</option>)}
          </select>
        </label>
        <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <span className="nv-muted" style={{ fontSize: 12 }}>Method</span>
          <select value={method} onChange={(e) => setMethod(e.target.value)}
            style={{ padding: "6px 10px", background: "var(--panel-2)", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 6 }}>
            {currentMethods.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
        </label>
      </div>
      <label style={{ display: "block", marginBottom: 12 }}>
        <span className="nv-muted" style={{ fontSize: 12 }}>Params (JSON)</span>
        <textarea
          value={paramsText}
          onChange={(e) => setParamsText(e.target.value)}
          rows={8}
          style={{ width: "100%", padding: 8, fontFamily: "ui-monospace", background: "var(--panel-2)", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 6 }}
        />
      </label>
      <button onClick={submit}
        style={{ padding: "8px 16px", background: "var(--accent)", color: "white", border: "none", borderRadius: 6, cursor: "pointer" }}>
        Replay
      </button>
      {status && <div className="nv-muted" style={{ marginTop: 12 }}>{status}</div>}
    </section>
  )
}
