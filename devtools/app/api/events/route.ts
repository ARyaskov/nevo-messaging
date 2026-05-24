import { NextRequest } from "next/server"
import { configureSource } from "@/lib/devtools-source"

export const runtime = "nodejs"
export const dynamic = "force-dynamic"

export async function GET(req: NextRequest) {
  const bus = configureSource()
  const enc = new TextEncoder()

  let closed = false
  let cleanup: (() => void) | null = null

  const stream = new ReadableStream({
    start(controller) {
      // Helper: enqueue with closed-state guard. Races are unavoidable here —
      // a bus event can arrive between `cancel()` and clearing the bus listener.
      const safeEnqueue = (chunk: Uint8Array) => {
        if (closed) return
        try {
          controller.enqueue(chunk)
        } catch {
          // Controller already detached — silently drop and stop streaming.
          closed = true
        }
      }

      const off = bus.on((event) => {
        safeEnqueue(enc.encode(`data: ${JSON.stringify(event)}\n\n`))
      })

      const keepAlive = setInterval(() => {
        safeEnqueue(enc.encode(`: keep-alive\n\n`))
      }, 15000)

      // Detect client disconnect via the request AbortSignal — fires reliably
      // when the browser/proxy closes the connection.
      const onAbort = () => {
        if (closed) return
        closed = true
        cleanup?.()
        try { controller.close() } catch {}
      }
      req.signal.addEventListener("abort", onAbort, { once: true })

      cleanup = () => {
        off()
        clearInterval(keepAlive)
        req.signal.removeEventListener("abort", onAbort)
      }
    },
    cancel() {
      // Fires when the consumer (Response) tears down the stream.
      if (closed) return
      closed = true
      cleanup?.()
    }
  })

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache, no-transform",
      "connection": "keep-alive"
    }
  })
}
