import { snapshot, getRegistry } from "@/lib/devtools-source"
import { ServiceDetail } from "@/components/ServiceDetail"
import { notFound } from "next/navigation"

export const dynamic = "force-dynamic"

interface Params { name: string }

export default async function ServiceDetailPage(props: { params: Promise<Params> }) {
  const { name } = await props.params
  const decoded = decodeURIComponent(name)
  const reg = getRegistry()
  const info = reg.getService(decoded)
  const events = snapshot(2000)

  if (!info) {
    const everSeen = events.some((e) => e.service === decoded)
    if (!everSeen) notFound()
  }

  return (
    <>
      <p className="nv-muted">
        <a href="/services">← All services</a>
      </p>
      <ServiceDetail
        serviceName={decoded}
        initialEvents={events}
        initialServiceInfo={info}
        initialCircuits={reg.listCircuits().filter((c) => c.service === decoded)}
      />
    </>
  )
}
