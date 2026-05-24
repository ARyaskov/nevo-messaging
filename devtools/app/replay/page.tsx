import { ReplayConsole } from "@/components/ReplayConsole"
import { getRegistry } from "@/lib/devtools-source"

export const dynamic = "force-dynamic"

export default function ReplayPage() {
  const services = getRegistry().listServices()
  return (
    <>
      <h1 className="nv-h1">Replay traffic</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Replay a query/emit against a service. The replay is recorded in the DevToolsBus as a <code>custom</code> event
        with <code>extra.kind = &quot;replay-requested&quot;</code>; a connected gateway can pick it up and actually fire
        the request.
      </p>
      <ReplayConsole services={services.map((s) => ({ name: s.serviceName, methods: s.methods.map((m) => m.signalName) }))} />
    </>
  )
}
