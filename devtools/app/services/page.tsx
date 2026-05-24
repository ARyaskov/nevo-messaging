import { snapshot, getRegistry } from "@/lib/devtools-source"
import { ServicesList } from "@/components/ServicesList"

export const dynamic = "force-dynamic"

export default function ServicesPage() {
  const events = snapshot(2000)
  const services = getRegistry().listServices()
  return (
    <>
      <h1 className="nv-h1">Services</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Registered services across all wired controllers. Stats are derived from the last 2 000 events
        in the bus (≈last few minutes of traffic).
      </p>
      <ServicesList initialEvents={events} initialServices={services} />
    </>
  )
}
