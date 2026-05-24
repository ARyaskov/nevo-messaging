"use client"

import { useMemo } from "react"
import type { DevToolsEvent, ServiceInfo } from "@riaskov/nevo-messaging"
import { useEvents } from "@/lib/use-events"
import { computeServiceStats, formatDuration, formatErrorRate, formatRelative } from "@/lib/computations"

interface Props {
  initialEvents: DevToolsEvent[]
  initialServices: ServiceInfo[]
}

export function ServicesList({ initialEvents, initialServices }: Props) {
  const events = useEvents({ initial: initialEvents, maxEvents: 5000 })
  const stats = useMemo(() => computeServiceStats(events), [events])

  const byName = useMemo(() => {
    const map = new Map<string, { info: ServiceInfo | undefined; stats?: typeof stats[number] }>()
    for (const info of initialServices) map.set(info.serviceName, { info })
    for (const s of stats) {
      const existing = map.get(s.serviceName) ?? { info: undefined }
      map.set(s.serviceName, { info: existing.info, stats: s })
    }
    return map
  }, [initialServices, stats])

  const rows = useMemo(() => [...byName.entries()].sort((a, b) => (b[1].stats?.totalCalls ?? 0) - (a[1].stats?.totalCalls ?? 0)), [byName])

  if (rows.length === 0) {
    return <div className="nv-empty">No services registered yet. Run a service that uses NatsSignalRouter / KafkaSignalRouter / HttpSignalRouter / SocketSignalRouter.</div>
  }

  return (
    <table className="nv-table">
      <thead>
        <tr>
          <th>Service</th>
          <th>Methods</th>
          <th>Calls</th>
          <th>Errors</th>
          <th>Err rate</th>
          <th>~RPS</th>
          <th>Last seen</th>
          <th>ACL</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {rows.map(([name, { info, stats }]) => (
          <tr key={name}>
            <td className="nv-mono"><a href={`/services/${encodeURIComponent(name)}`}>{name}</a></td>
            <td>{info?.methods.length ?? "—"}</td>
            <td>{stats?.totalCalls ?? 0}</td>
            <td>{stats?.errors ?? 0}</td>
            <td>{stats ? formatErrorRate(stats.errorRate) : "0%"}</td>
            <td>{stats ? stats.rps.toFixed(2) : "0"}</td>
            <td className="nv-muted" suppressHydrationWarning>{stats?.lastTs ? formatRelative(stats.lastTs) : "—"}</td>
            <td>{info?.accessControl?.rules?.length ? <span className="nv-badge warn">{info.accessControl.rules.length} rules</span> : <span className="nv-muted">none</span>}</td>
            <td><a className="nv-mono" href={`/services/${encodeURIComponent(name)}`}>→</a></td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
