import type { NatsConnection, Subscription as NatsSubscription } from "@nats-io/nats-core"
import { DevToolsBus, DevToolsEvent, DevToolsAdapter, getDevToolsBus } from "../../common/devtools"
import { getDefaultCodec, Codec } from "../../common/codec"
import { getDefaultLogger, NevoLogger } from "../../common/logger"
import { getNatsModule } from "../optional-deps"

export const DEFAULT_DEVTOOLS_SUBJECT = "__nevo.devtools"

export interface NatsDevToolsAdapterOptions {
  bus?: DevToolsBus
  subject?: string
  codec?: Codec
  logger?: NevoLogger
  bridgeLocalEvents?: boolean
  publishHeartbeat?: boolean
  heartbeatIntervalMs?: number
}

export class NatsDevToolsAdapter implements DevToolsAdapter {
  private readonly nc: NatsConnection
  private readonly bus: DevToolsBus
  private readonly subject: string
  private readonly codec: Codec
  private readonly logger: NevoLogger
  private readonly bridgeLocalEvents: boolean
  private readonly publishHeartbeat: boolean
  private readonly heartbeatIntervalMs: number
  private subscription?: NatsSubscription
  private localOff?: () => void
  private heartbeatTimer?: NodeJS.Timeout
  private attached = false

  constructor(nc: NatsConnection, opts?: NatsDevToolsAdapterOptions) {
    this.nc = nc
    this.bus = opts?.bus ?? getDevToolsBus()
    this.subject = opts?.subject ?? DEFAULT_DEVTOOLS_SUBJECT
    this.codec = opts?.codec ?? getDefaultCodec()
    this.logger = opts?.logger ?? getDefaultLogger().child({ component: "nats-devtools" })
    this.bridgeLocalEvents = opts?.bridgeLocalEvents !== false
    this.publishHeartbeat = opts?.publishHeartbeat === true
    this.heartbeatIntervalMs = opts?.heartbeatIntervalMs ?? 30_000
  }

  async attach(): Promise<() => Promise<void>> {
    if (this.attached) throw new Error("Adapter already attached")
    this.attached = true

    this.subscription = this.nc.subscribe(this.subject)
    const sub = this.subscription
    const codec = this.codec
    const bus = this.bus
    const logger = this.logger
    ;(async () => {
      for await (const msg of sub) {
        try {
          const event = codec.decode<DevToolsEvent>(msg.data)
          if (!event || typeof event !== "object") continue
          bus.ingestRemote(event)
        } catch (err) {
          logger.warn({ event: "devtools.decode_error", err: (err as Error)?.message }, "Failed to decode devtools event")
        }
      }
    })()

    if (this.bridgeLocalEvents) {
      this.localOff = this.bus.onLocal((event) => {
        try {
          const data = this.codec.encode(event)
          this.nc.publish(this.subject, data)
        } catch (err) {
          this.logger.warn({ event: "devtools.publish_error", err: (err as Error)?.message })
        }
      })
    }

    if (this.publishHeartbeat) {
      this.heartbeatTimer = setInterval(() => {
        this.bus.publish({ ts: Date.now(), type: "custom", extra: { kind: "heartbeat" } })
      }, this.heartbeatIntervalMs)
      if (typeof this.heartbeatTimer.unref === "function") this.heartbeatTimer.unref()
    }

    return async () => {
      if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
      if (this.localOff) this.localOff()
      if (this.subscription) {
        try { this.subscription.unsubscribe() } catch {}
      }
      this.attached = false
    }
  }
}

export async function wireDevToolsToNats(
  nc: NatsConnection,
  opts?: NatsDevToolsAdapterOptions
): Promise<{ detach: () => Promise<void> }> {
  const adapter = new NatsDevToolsAdapter(nc, opts)
  const detach = await adapter.attach()
  return { detach }
}

export async function wireDevToolsToNatsByConfig(
  opts: NatsDevToolsAdapterOptions & { servers?: string[] }
): Promise<{ detach: () => Promise<void> }> {
  const { connect } = getNatsModule()
  const nc = await connect({ servers: opts.servers && opts.servers.length > 0 ? opts.servers : ["nats://127.0.0.1:4222"] })
  const adapter = new NatsDevToolsAdapter(nc, opts)
  const detach = await adapter.attach()
  return {
    detach: async () => {
      await detach()
      try { await nc.drain() } catch {}
    }
  }
}
