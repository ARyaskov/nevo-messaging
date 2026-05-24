import type { NatsConnection } from "@nats-io/nats-core"
import { getJetStreamModule } from "../optional-deps"
import { Codec, getDefaultCodec, getDefaultLogger, NevoLogger, MessagingError, ErrorCode } from "../../common"

export interface JetStreamSetupOptions {
  streamName: string
  subjects: string[]
  retention?: "limits" | "interest" | "workqueue"
  maxAge?: number
  maxBytes?: number
  duplicateWindow?: number
}

export interface JetStreamConsumerOptions {
  streamName: string
  durableName: string
  filterSubject?: string
  ackWait?: number
  maxDeliver?: number
  deliverPolicy?: "all" | "last" | "new" | "by_start_sequence" | "by_start_time"
  optStartSeq?: number
}

export interface JsMessageContext {
  ack(): void
  nack(delayMs?: number): void
  term(): void
  working(): void
  meta: { seq: number; numDelivered: number; numPending: number }
}

export interface JsSubscription {
  unsubscribe(): Promise<void>
}

export class JetStreamHelper {
  private readonly nc: NatsConnection
  private readonly codec: Codec
  private readonly logger: NevoLogger

  constructor(nc: NatsConnection, opts?: { codec?: Codec; logger?: NevoLogger }) {
    this.nc = nc
    this.codec = opts?.codec ?? getDefaultCodec()
    this.logger = opts?.logger ?? getDefaultLogger().child({ component: "jetstream" })
  }

  async ensureStream(setup: JetStreamSetupOptions): Promise<void> {
    const js = getJetStreamModule() as any
    const jsm = await js.jetstreamManager(this.nc)
    const RetentionPolicy = js.RetentionPolicy
    const cfg: any = {
      name: setup.streamName,
      subjects: setup.subjects,
      retention:
        setup.retention === "interest"
          ? RetentionPolicy?.Interest ?? "interest"
          : setup.retention === "workqueue"
            ? RetentionPolicy?.Workqueue ?? "workqueue"
            : RetentionPolicy?.Limits ?? "limits",
      max_age: setup.maxAge,
      max_bytes: setup.maxBytes,
      duplicate_window: setup.duplicateWindow
    }
    try {
      await jsm.streams.info(setup.streamName)
      await jsm.streams.update(setup.streamName, cfg)
    } catch {
      await jsm.streams.add(cfg)
    }
  }

  async publish(subject: string, value: unknown, opts?: { msgId?: string; expectedStream?: string; expectedLastSeq?: number }): Promise<{ seq: number; stream: string; duplicate: boolean }> {
    const js = getJetStreamModule() as any
    const client = js.jetstream(this.nc)
    const data = this.codec.encode(value)
    const pubOpts: any = {}
    if (opts?.msgId) pubOpts.msgID = opts.msgId
    if (opts?.expectedStream) pubOpts.expect = { streamName: opts.expectedStream }
    if (opts?.expectedLastSeq) pubOpts.expect = { ...(pubOpts.expect ?? {}), lastSequence: opts.expectedLastSeq }
    const ack = await client.publish(subject, data, pubOpts)
    return { seq: ack.seq, stream: ack.stream, duplicate: ack.duplicate }
  }

  async ensureConsumer(opts: JetStreamConsumerOptions): Promise<void> {
    const js = getJetStreamModule() as any
    const jsm = await js.jetstreamManager(this.nc)
    const cfg: any = {
      durable_name: opts.durableName,
      filter_subject: opts.filterSubject,
      ack_wait: opts.ackWait,
      max_deliver: opts.maxDeliver,
      deliver_policy: opts.deliverPolicy,
      opt_start_seq: opts.optStartSeq
    }
    try {
      await jsm.consumers.info(opts.streamName, opts.durableName)
      await jsm.consumers.update(opts.streamName, opts.durableName, cfg)
    } catch {
      await jsm.consumers.add(opts.streamName, cfg)
    }
  }

  async pullSubscribe<T = unknown>(
    opts: JetStreamConsumerOptions & { batch?: number; expires?: number; idleHeartbeat?: number },
    handler: (data: T, ctx: JsMessageContext) => Promise<void> | void
  ): Promise<JsSubscription> {
    const js = getJetStreamModule() as any
    const client = js.jetstream(this.nc)
    await this.ensureConsumer(opts)
    const consumer = await client.consumers.get(opts.streamName, opts.durableName)
    let stopped = false

    const pump = async () => {
      while (!stopped) {
        try {
          const msgs = await consumer.consume({
            max_messages: opts.batch ?? 32,
            expires: opts.expires ?? 5000,
            idle_heartbeat: opts.idleHeartbeat ?? 1000
          })
          for await (const m of msgs) {
            if (stopped) break
            let value: T
            try {
              value = this.codec.decode<T>(m.data)
            } catch (err) {
              this.logger.error({ event: "jetstream.decode_error", err: (err as Error)?.message })
              m.term()
              continue
            }
            const info = m.info
            const ctx: JsMessageContext = {
              ack: () => m.ack(),
              nack: (delayMs?: number) => m.nak(delayMs),
              term: () => m.term(),
              working: () => m.working(),
              meta: { seq: info?.streamSequence ?? 0, numDelivered: info?.deliveryCount ?? 1, numPending: info?.pending ?? 0 }
            }
            try {
              await handler(value, ctx)
            } catch (err) {
              this.logger.error({ event: "jetstream.handler_error", err: (err as Error)?.message })
              m.nak()
            }
          }
        } catch (err) {
          if (!stopped) {
            this.logger.warn({ event: "jetstream.consume_loop_error", err: (err as Error)?.message })
            await new Promise((r) => setTimeout(r, 500))
          }
        }
      }
    }

    void pump()
    return { unsubscribe: async () => { stopped = true } }
  }
}

export function getJetStreamHelper(nc: NatsConnection, opts?: { codec?: Codec; logger?: NevoLogger }): JetStreamHelper {
  if (!nc) throw new MessagingError(ErrorCode.CONNECTION_LOST, { message: "NATS connection required for JetStream" })
  return new JetStreamHelper(nc, opts)
}
