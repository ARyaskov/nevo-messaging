import { getRegistry } from "@/lib/devtools-source"
import { AclInspector } from "@/components/AclInspector"

export const dynamic = "force-dynamic"

export default function AclPage() {
  const services = getRegistry().listServices()
  return (
    <>
      <h1 className="nv-h1">ACL Inspector</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Per-service access-control rules. Use the simulator at the bottom to test how a hypothetical
        caller would be evaluated against a service's rules.
      </p>
      <AclInspector initialServices={services} />
    </>
  )
}
