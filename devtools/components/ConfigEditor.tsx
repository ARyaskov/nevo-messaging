"use client"

import { useMemo, useState } from "react"
import type { ServiceInfo } from "@riaskov/nevo-messaging"

interface Props { services: ServiceInfo[] }

export function ConfigEditor({ services }: Props) {
  const [selected, setSelected] = useState(services[0]?.serviceName ?? "")
  const current = useMemo(() => services.find((s) => s.serviceName === selected), [services, selected])
  const [json, setJson] = useState(() => current ? JSON.stringify(current.accessControl ?? { rules: [], allowAllByDefault: true }, null, 2) : "{}")
  const [status, setStatus] = useState<string>("")

  const submit = async () => {
    setStatus("saving…")
    let parsed: unknown
    try { parsed = JSON.parse(json) } catch { setStatus("Invalid JSON"); return }
    const res = await fetch("/api/config", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ service: selected, accessControl: parsed })
    })
    setStatus(res.ok ? "Applied (in-process). Persistent storage not implemented — restart will revert." : `Error: ${res.status}`)
  }

  if (!services.length) return <div className="nv-empty">No services registered yet.</div>

  return (
    <>
      <label style={{ display: "flex", flexDirection: "column", gap: 4, marginBottom: 12 }}>
        <span className="nv-muted" style={{ fontSize: 12 }}>Service</span>
        <select
          value={selected}
          onChange={(e) => {
            setSelected(e.target.value)
            const info = services.find((s) => s.serviceName === e.target.value)
            setJson(JSON.stringify(info?.accessControl ?? { rules: [], allowAllByDefault: true }, null, 2))
          }}
          style={{ padding: "6px 10px", background: "var(--panel-2)", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 6, width: 280 }}
        >
          {services.map((s) => <option key={s.serviceName} value={s.serviceName}>{s.serviceName}</option>)}
        </select>
      </label>

      <label style={{ display: "block", marginBottom: 12 }}>
        <span className="nv-muted" style={{ fontSize: 12 }}>ACL config (JSON)</span>
        <textarea
          value={json}
          onChange={(e) => setJson(e.target.value)}
          rows={16}
          style={{ width: "100%", padding: 8, fontFamily: "ui-monospace", background: "var(--panel-2)", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 6 }}
        />
      </label>

      <button onClick={submit}
        style={{ padding: "8px 16px", background: "var(--accent)", color: "white", border: "none", borderRadius: 6, cursor: "pointer" }}>
        Apply
      </button>
      {status && <div className="nv-muted" style={{ marginTop: 12 }}>{status}</div>}
    </>
  )
}
