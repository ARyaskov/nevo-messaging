import { snapshot, getRegistry } from "@/lib/devtools-source"
import { CircuitDashboard } from "@/components/CircuitDashboard"

export const dynamic = "force-dynamic"

export default function CircuitsPage() {
  const events = snapshot(2000)
  const circuits = getRegistry().listCircuits()
  return (
    <>
      <h1 className="nv-h1">Circuit Breakers</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Live state of every client-side circuit breaker. Open circuits short-circuit the next request
        and return <code>ErrorCode.CIRCUIT_OPEN</code> until the reset window elapses.
      </p>
      <CircuitDashboard initialEvents={events} initialCircuits={circuits} />
    </>
  )
}
