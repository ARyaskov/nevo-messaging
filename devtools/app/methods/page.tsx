import { snapshot } from "@/lib/devtools-source"
import { MethodsLeaderboard } from "@/components/MethodsLeaderboard"

export const dynamic = "force-dynamic"

interface SearchParams {
  service?: string
  method?: string
}

export default async function MethodsPage(props: { searchParams: Promise<SearchParams> }) {
  const { service, method } = await props.searchParams
  const events = snapshot(2000)
  return (
    <>
      <h1 className="nv-h1">Methods</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Top-N rankings across the last 2 000 events. Statistics include p50/p95/p99 latency and error rate per (service, method).
      </p>
      <MethodsLeaderboard
        initialEvents={events}
        highlightService={service}
        highlightMethod={method}
      />
    </>
  )
}
