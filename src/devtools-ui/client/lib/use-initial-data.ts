import { useEffect, useState } from "react"
import type { CircuitInfo, DevToolsEvent, ServiceInfo } from "../types"

export interface Loadable<T> {
  data: T | null
  error: string | null
}

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url, { cache: "no-store" })
  if (!res.ok) throw new Error(`${url} -> HTTP ${res.status}`)
  return (await res.json()) as T
}

function useJson<T>(url: string): Loadable<T> {
  const [state, setState] = useState<Loadable<T>>({ data: null, error: null })

  useEffect(() => {
    let cancelled = false
    setState({ data: null, error: null })

    getJson<T>(url).then(
      (data) => {
        if (!cancelled) setState({ data, error: null })
      },
      (err: unknown) => {
        if (!cancelled) setState({ data: null, error: err instanceof Error ? err.message : String(err) })
      }
    )

    return () => {
      cancelled = true
    }
  }, [url])

  return state
}

/**
 * The Next version rendered each page on the server with a bus snapshot already
 * in hand. As a static bundle we fetch that same snapshot once on mount, then
 * hand it to the view as `initialEvents` — after which the SSE stream takes over.
 */
export function useSnapshot(limit: number): Loadable<DevToolsEvent[]> {
  const { data, error } = useJson<{ events: DevToolsEvent[] }>(`/api/snapshot?limit=${limit}`)
  return { data: data ? data.events : null, error }
}

export interface RegistrySnapshot {
  services: ServiceInfo[]
  circuits: CircuitInfo[]
}

export function useRegistry(): Loadable<RegistrySnapshot> {
  return useJson<RegistrySnapshot>("/api/registry")
}
