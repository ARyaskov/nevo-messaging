import { DevToolsBus, DevToolsEvent, getDevToolsBus, getDevToolsRegistry, DevToolsRegistry } from "@riaskov/nevo-messaging"

let configured = false
let natsDetach: (() => Promise<void>) | null = null

async function maybeBridgeNats() {
  const servers = process.env.NEVO_DEVTOOLS_NATS_SERVERS || "nats://127.0.0.1:4222"
  if (!servers) return
  try {
    // Internally pulls in `@nats-io/transport-node` and `@nats-io/nats-core` —
    // install both in devtools/ before setting NEVO_DEVTOOLS_NATS_SERVERS.
    const { wireDevToolsToNatsByConfig } = await import("@riaskov/nevo-messaging")
    const { detach } = await wireDevToolsToNatsByConfig({
      bus: getDevToolsBus(),
      servers: servers.split(",").map((s) => s.trim()).filter(Boolean),
      bridgeLocalEvents: true
    })
    natsDetach = detach
  } catch (err) {
    console.error("[devtools] failed to bridge to NATS:", err)
  }
}

export function configureSource(): DevToolsBus {
  const bus = getDevToolsBus()
  if (!configured) {
    configured = true
    void maybeBridgeNats()
  }
  return bus
}

export function getRegistry(): DevToolsRegistry {
  configureSource()
  return getDevToolsRegistry()
}

export function snapshot(limit = 1000): DevToolsEvent[] {
  return configureSource().recent(limit)
}

export async function teardownSource(): Promise<void> {
  if (natsDetach) {
    try { await natsDetach() } catch {}
    natsDetach = null
  }
}
