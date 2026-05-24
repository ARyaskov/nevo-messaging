import { TraceViewer } from "@/components/TraceViewer"
import { snapshot } from "@/lib/devtools-source"

export const dynamic = "force-dynamic"

export default async function TracePage(props: { searchParams: Promise<{ uuid?: string }> }) {
  const { uuid } = await props.searchParams
  const events = snapshot(2000)
  return (
    <>
      <h1 className="nv-h1">Trace</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Reconstructs an end-to-end trace by joining client + server events with the same <code>uuid</code>.
        Pair this with W3C <code>traceparent</code> in <code>meta.trace</code> for fully distributed view.
      </p>
      <TraceViewer initialEvents={events} initialUuid={uuid} />
    </>
  )
}
