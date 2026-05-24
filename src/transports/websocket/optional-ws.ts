type WsModule = typeof import("ws")

let cached: WsModule | null = null

export function getWsModule(): WsModule {
  if (cached) return cached
  try {
    cached = require("ws") as WsModule
    return cached
  } catch {
    throw new Error('Missing optional dependency "ws". Install it to use the WebSocket transport server.')
  }
}
