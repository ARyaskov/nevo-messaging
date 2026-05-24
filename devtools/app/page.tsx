import { snapshot } from "@/lib/devtools-source"
import { LiveDashboard } from "@/components/LiveDashboard"

export const dynamic = "force-dynamic"

export default function Page() {
  const initial = snapshot(500)
  return (
    <>
      <h1 className="nv-h1">Overview</h1>
      <LiveDashboard initialEvents={initial} />
    </>
  )
}
