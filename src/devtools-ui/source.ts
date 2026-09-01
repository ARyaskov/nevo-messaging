import { getDevToolsBus } from "../common/devtools"
import { getDevToolsRegistry } from "../common/devtools-registry"
import type { DevToolsBus, DevToolsEvent } from "../common/devtools"
import type { DevToolsRegistry } from "../common/devtools-registry"

export interface DevToolsUiSourceOptions {
  /** NATS servers to ingest DevTools events from. Empty/omitted keeps the UI in-process only. */
  natsServers?: string[]
  /** Subject the services publish to. Defaults to the adapter's `__nevo.devtools`. */
  subject?: string
}

let configured = false
let natsDetach: (() => Promise<void>) | null = null
let bridgeError: string | null = null
let bridgedServers: string[] = []

/**
 * Attaches the NATS adapter so events published by other processes land on this
 * process's bus. Embedded usage needs none of this — the UI reads the same
 * singleton bus the framework already writes to.
 */
async function bridgeNats(servers: string[], subject?: string): Promise<void> {
  try {
    // Pulls in `@nats-io/transport-node` + `@nats-io/nats-core`, which are optional
    // peers. Imported lazily so the UI still boots (in-process only) without them.
    const { wireDevToolsToNatsByConfig } = await import("../transports/nats/devtools.adapter")
    const { detach } = await wireDevToolsToNatsByConfig({
      bus: getDevToolsBus(),
      servers,
      ...(subject ? { subject } : {}),
      bridgeLocalEvents: true
    })
    natsDetach = detach
    bridgedServers = servers
  } catch (err) {
    bridgeError = err instanceof Error ? err.message : String(err)
    // Also surfaced via /api/health so the UI can explain an empty dashboard
    // instead of silently rendering nothing.
    console.error("[nevo-devtools] failed to bridge to NATS:", bridgeError)
  }
}

export function configureSource(opts: DevToolsUiSourceOptions = {}): DevToolsBus {
  const bus = getDevToolsBus()
  if (!configured) {
    configured = true
    const servers = opts.natsServers ?? []
    if (servers.length > 0) void bridgeNats(servers, opts.subject)
  }
  return bus
}

export function getRegistry(): DevToolsRegistry {
  return getDevToolsRegistry()
}

export function snapshot(limit = 500): DevToolsEvent[] {
  return getDevToolsBus().recent(limit)
}

export function sourceHealth(): { nats: { bridged: boolean; servers: string[]; error: string | null } } {
  return {
    nats: { bridged: natsDetach !== null, servers: bridgedServers, error: bridgeError }
  }
}

export async function teardownSource(): Promise<void> {
  if (!natsDetach) return
  try {
    await natsDetach()
  } catch {
    // Detach is best-effort during shutdown.
  }
  natsDetach = null
}
