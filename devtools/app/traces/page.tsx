import { snapshot } from "@/lib/devtools-source"
import { TracesView } from "@/components/TracesView"

export const dynamic = "force-dynamic"

interface SearchParams {
  chain?: string
}

export default async function TracesPage(props: { searchParams: Promise<SearchParams> }) {
  const { chain } = await props.searchParams
  const events = snapshot(5000)
  return (
    <>
      <h1 className="nv-h1">Traces</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Each row groups every envelope that belongs to one logical fan-out (request → downstream → downstream → response).
        The framework propagates a <code>chainId</code> via <code>AsyncLocalStorage</code>, so any service that calls another
        through a Nevo client inherits the same chain automatically.
      </p>
      <TracesView initialEvents={events} initialChainId={chain} />
    </>
  )
}
