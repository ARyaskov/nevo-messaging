import { ConfigEditor } from "@/components/ConfigEditor"
import { getRegistry } from "@/lib/devtools-source"

export const dynamic = "force-dynamic"

export default function ConfigPage() {
  const services = getRegistry().listServices()
  return (
    <>
      <h1 className="nv-h1">Live config</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Edit ACL of a registered service at runtime. Changes update the in-process <code>DevToolsRegistry</code> entry,
        which the signal-router decorator reads on the next request.
      </p>
      <ConfigEditor services={services} />
    </>
  )
}
