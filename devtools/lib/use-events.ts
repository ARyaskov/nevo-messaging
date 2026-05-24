"use client"

import { useEffect, useRef, useState } from "react"
import type { DevToolsEvent } from "@riaskov/nevo-messaging"

interface UseEventsOptions {
  maxEvents?: number
  initial?: DevToolsEvent[]
}

export function useEvents(opts: UseEventsOptions = {}) {
  const max = opts.maxEvents ?? 2000
  const [events, setEvents] = useState<DevToolsEvent[]>(opts.initial ?? [])
  const ref = useRef<EventSource | null>(null)

  useEffect(() => {
    const source = new EventSource("/api/events")
    ref.current = source
    source.onmessage = (m) => {
      try {
        const e: DevToolsEvent = JSON.parse(m.data)
        setEvents((prev) => {
          const next = prev.length >= max ? prev.slice(prev.length - max + 1) : prev.slice()
          next.push(e)
          return next
        })
      } catch {}
    }
    return () => { source.close(); ref.current = null }
  }, [max])

  return events
}
