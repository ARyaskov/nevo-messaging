"use client"

import { useMemo, useState } from "react"
import type { ServiceInfo, AccessRule, AccessControlConfig } from "@riaskov/nevo-messaging"

interface Props { initialServices: ServiceInfo[] }

// Local copy of the framework's pure ACL evaluator.
// Inlined here because importing it from `@riaskov/nevo-messaging` would force
// webpack to bundle the framework's whole barrel into the client chunk, dragging
// in every transport (nats, kafka, fastify, …). The logic mirrors
// `src/common/access-control.ts:isAccessAllowed`.
function isAccessAllowed(
  config: AccessControlConfig | undefined,
  topic: string,
  method: string,
  callerService: string | undefined
): boolean {
  if (!config) return true
  const allowAllByDefault = config.allowAllByDefault !== false

  const candidates: AccessRule[] = []
  for (const rule of config.rules ?? []) {
    const topicMatches = !rule.topic || rule.topic === "*" || rule.topic === topic
    const methodMatches = !rule.method || rule.method === "*" || rule.method === method
    if (topicMatches && methodMatches) candidates.push(rule)
  }
  if (candidates.length === 0) return allowAllByDefault

  let matched = false
  let denied = false
  for (const rule of candidates) {
    matched = true
    if (listHasAcl(rule.deny, callerService)) { denied = true; continue }
    if (rule.allow && rule.allow.length > 0) {
      if (listHasAcl(rule.allow, callerService)) return true
      continue
    }
    return true
  }
  if (denied) return false
  return matched ? false : allowAllByDefault
}

function listHasAcl(list: string[] | undefined, value: string | undefined): boolean {
  if (!list || list.length === 0) return false
  if (list.includes("*")) return true
  if (!value) return false
  return list.includes(value)
}

export function AclInspector({ initialServices }: Props) {
  const withAcl = initialServices.filter((s) => s.accessControl?.rules?.length)
  const without = initialServices.filter((s) => !s.accessControl?.rules?.length)

  const [selectedService, setSelectedService] = useState<string>(withAcl[0]?.serviceName ?? "")
  const [topic, setTopic] = useState<string>("")
  const [method, setMethod] = useState<string>("user.ping")
  const [caller, setCaller] = useState<string>("frontend")

  const service = useMemo(() => initialServices.find((s) => s.serviceName === selectedService), [initialServices, selectedService])
  const config = service?.accessControl

  const decision = useMemo(() => {
    if (!service) return null
    const computedTopic = topic || service.topic || `${service.serviceName}-events`
    const allowed = isAccessAllowed(config, computedTopic, method, caller)
    const evaluation = (config?.rules ?? []).map((r, i) => {
      const topicMatches = !r.topic || r.topic === "*" || r.topic === computedTopic
      const methodMatches = !r.method || r.method === "*" || r.method === method
      const matched = topicMatches && methodMatches
      const denied = matched && r.deny?.length ? listHas(r.deny, caller) : false
      const allowed = matched && !denied && r.allow?.length ? listHas(r.allow, caller) : matched && !denied && !r.allow
      return { rule: r, idx: i, topicMatches, methodMatches, matched, denied, allowed: Boolean(allowed) }
    })
    return { allowed, evaluation, topic: computedTopic }
  }, [config, service, topic, method, caller])

  return (
    <>
      <section className="nv-card" style={{ marginBottom: 16 }}>
        <h2>Services with ACL</h2>
        {withAcl.length === 0 ? (
          <div className="nv-empty">No services have ACL configured.</div>
        ) : (
          <table className="nv-table">
            <thead><tr><th>Service</th><th>Topic</th><th>Rules</th><th>Default</th></tr></thead>
            <tbody>
              {withAcl.map((s) => (
                <tr key={s.serviceName}>
                  <td className="nv-mono">
                    <button
                      onClick={() => setSelectedService(s.serviceName)}
                      style={{ background: "none", border: "none", color: "var(--accent)", cursor: "pointer", padding: 0, font: "inherit" }}
                    >
                      {s.serviceName}
                    </button>
                  </td>
                  <td className="nv-mono">{s.topic ?? "—"}</td>
                  <td>{s.accessControl!.rules!.length}</td>
                  <td>{s.accessControl!.allowAllByDefault === false ? "deny" : "allow"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {without.length > 0 && (
        <section className="nv-card" style={{ marginBottom: 16 }}>
          <h2>Services without ACL ({without.length})</h2>
          <div className="nv-row" style={{ flexWrap: "wrap", gap: 8 }}>
            {without.map((s) => (
              <span key={s.serviceName} className="nv-mono" style={{ padding: "4px 8px", border: "1px solid var(--border)", borderRadius: 6 }}>{s.serviceName}</span>
            ))}
          </div>
        </section>
      )}

      {service && config && (
        <section className="nv-card">
          <h2>Simulator — {service.serviceName}</h2>
          <div className="nv-row" style={{ gap: 12, flexWrap: "wrap", marginBottom: 12 }}>
            <Field label="Topic" value={topic} onChange={setTopic} placeholder={service.topic ?? `${service.serviceName}-events`} />
            <Field label="Method" value={method} onChange={setMethod} />
            <Field label="Caller service" value={caller} onChange={setCaller} />
          </div>

          <div style={{ marginBottom: 12 }}>
            <strong>Decision: </strong>
            {decision?.allowed
              ? <span className="nv-badge ok">ALLOW</span>
              : <span className="nv-badge err">DENY</span>}
          </div>

          <table className="nv-table">
            <thead>
              <tr>
                <th>#</th><th>Topic</th><th>Method</th><th>Allow</th><th>Deny</th><th>Matches?</th>
              </tr>
            </thead>
            <tbody>
              {decision?.evaluation.map((row) => (
                <tr key={row.idx} style={{ background: row.matched ? "rgba(63, 185, 80, 0.04)" : undefined }}>
                  <td className="nv-muted">{row.idx + 1}</td>
                  <td className="nv-mono">{row.rule.topic ?? "*"}</td>
                  <td className="nv-mono">{row.rule.method ?? "*"}</td>
                  <td>{row.rule.allow?.join(", ") || <span className="nv-muted">—</span>}</td>
                  <td>{row.rule.deny?.join(", ") || <span className="nv-muted">—</span>}</td>
                  <td>
                    {!row.matched
                      ? <span className="nv-muted">no match</span>
                      : row.denied
                        ? <span className="nv-badge err">denied here</span>
                        : row.allowed
                          ? <span className="nv-badge ok">allowed here</span>
                          : <span className="nv-badge warn">matched (no allow list)</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
    </>
  )
}

function Field({ label, value, onChange, placeholder }: { label: string; value: string; onChange: (v: string) => void; placeholder?: string }) {
  return (
    <label style={{ display: "flex", flexDirection: "column", gap: 4, minWidth: 180 }}>
      <span className="nv-muted" style={{ fontSize: 12 }}>{label}</span>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        style={{ padding: "6px 10px", background: "var(--panel-2)", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 6 }}
      />
    </label>
  )
}

function listHas(list: string[], value: string): boolean {
  if (!list || list.length === 0) return false
  if (list.includes("*")) return true
  return list.includes(value)
}
