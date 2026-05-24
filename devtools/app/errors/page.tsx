import { snapshot } from "@/lib/devtools-source"
import { ErrorsTimeline } from "@/components/ErrorsTimeline"

export const dynamic = "force-dynamic"

export default function ErrorsPage() {
  const events = snapshot(2000)
  return (
    <>
      <h1 className="nv-h1">Errors</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Latest failed responses across all services. Filter by service or method, group by error code.
      </p>
      <ErrorsTimeline initialEvents={events} />
    </>
  )
}
