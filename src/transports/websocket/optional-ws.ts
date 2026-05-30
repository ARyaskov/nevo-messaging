import { createRequire } from "node:module"

const nodeRequire = createRequire(__filename)

type WsModule = typeof import("ws")

let cached: WsModule | null = null

export function getWsModule(): WsModule {
  if (cached) return cached
  try {
    cached = nodeRequire("ws") as WsModule
    return cached
  } catch {
    throw new Error('Missing optional dependency "ws". Install it to use the WebSocket transport server.')
  }
}
