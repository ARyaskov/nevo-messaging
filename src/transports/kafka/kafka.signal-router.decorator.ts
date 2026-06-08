import { Type } from "@nestjs/common"
import { MessagePattern } from "@nestjs/microservices"
import { createSignalRouterDecorator, SignalRouterOptions } from "../../signal-router.utils"
import { Codec, getCodec, getDefaultCodec, maybeCompress, maybeDecompress, maybeDecompressAsync, shouldDecompressAsync, enforcePayloadLimit, DEFAULT_MAX_PAYLOAD_BYTES, resolveCompressionOptions, getDefaultLogger, DlqRouter } from "../../common"
import { getKafkaModule } from "../optional-deps"

// Buffer the async wrapper pre-inflated, for the sync extractor to reuse.
const PREDECOMPRESSED = Symbol("nevo.kafka.predecompressed")

// Normalises a Kafka message value to a Buffer; null when there is no decodable value.
function kafkaValueBuffer(data: any): Buffer | null {
  if (!(data && data.value && (typeof data.value === "string" || data.value instanceof Buffer || data.value instanceof Uint8Array))) return null
  return data.value instanceof Buffer ? data.value : typeof data.value === "string" ? Buffer.from(data.value, "utf8") : Buffer.from(data.value)
}

const DEFAULT_KAFKA_HOST = process.env["KAFKA_HOST"] || "localhost"
const DEFAULT_KAFKA_PORT = process.env["KAFKA_PORT"] || "9092"

export interface KafkaSignalRouterOptions extends SignalRouterOptions {
  brokers?: string[]
  kafkaHost?: string
  kafkaPort?: string
  codec?: Codec | string
  compression?: { enabled?: boolean; algorithm?: "gzip" | "deflate"; threshold?: number; level?: number }
}

export function KafkaSignalRouter(serviceType: Type<any> | Type<any>[], options?: KafkaSignalRouterOptions) {
  const codec: Codec = typeof options?.codec === "string" ? getCodec(options.codec) : (options?.codec as Codec) || getDefaultCodec()
  const compression = resolveCompressionOptions(options?.compression)
  const maxPayloadBytes = options?.security?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
  const logger = options?.logger || getDefaultLogger().child({ component: "kafka-router" })
  const dlq = options?.dlq instanceof DlqRouter ? options.dlq : new DlqRouter({ enabled: (options?.dlq as any)?.enabled === true })

  return createSignalRouterDecorator(
    serviceType,
    { ...options, dlq, logger },
    (data) => {
      let messageData = data
      const buf = kafkaValueBuffer(data)
      if (buf) {
        try {
          const encoding = data?.headers?.["content-encoding"]?.toString?.()
          // Reuse the pre-inflated buffer when present; otherwise inflate synchronously.
          const predecompressed = (data as any)[PREDECOMPRESSED] as Uint8Array | undefined
          const decompressed = predecompressed ?? maybeDecompress(buf, encoding, maxPayloadBytes)
          enforcePayloadLimit(decompressed, maxPayloadBytes)
          messageData = codec.decode(decompressed)
        } catch (e) {
          logger.error({ event: "kafka.decode_error", err: (e as Error)?.message }, "Failed to decode message")
        }
      }
      return {
        method: messageData?.method,
        params: messageData?.params,
        uuid: messageData?.uuid,
        meta: messageData?.meta
      }
    },
    (target, eventPattern, handlerName) => {
      const originalMethod = target.prototype[handlerName]
      target.prototype.producer = null

      const originalOnModuleInit = target.prototype.onModuleInit || function () {}
      target.prototype.onModuleInit = async function () {
        await originalOnModuleInit.call(this)

        const brokers = options?.brokers && options.brokers.length > 0
          ? options.brokers
          : [`${options?.kafkaHost ? (process.env[options.kafkaHost] || DEFAULT_KAFKA_HOST) : DEFAULT_KAFKA_HOST}:${options?.kafkaPort ? (process.env[options.kafkaPort] || DEFAULT_KAFKA_PORT) : DEFAULT_KAFKA_PORT}`]

        const { Kafka } = getKafkaModule()
        const kafka = new Kafka({ clientId: `${eventPattern}-producer`, brokers })
        this.producer = kafka.producer()
        await this.producer.connect()
        if (options?.debug) {
          logger.debug({ event: "kafka.producer.connected", topic: eventPattern })
        }
      }

      const originalOnModuleDestroy = target.prototype.onModuleDestroy || function () {}
      target.prototype.onModuleDestroy = async function () {
        await originalOnModuleDestroy.call(this)
        if (this.producer) {
          try { await this.producer.disconnect() } catch {}
        }
      }

      target.prototype[handlerName] = async function (data: any) {
        // Inflate large compressed payloads off the event loop before the sync extractor runs.
        const buf = kafkaValueBuffer(data)
        if (buf) {
          const encoding = data?.headers?.["content-encoding"]?.toString?.()
          if (shouldDecompressAsync(buf.byteLength, encoding)) {
            try {
              (data as any)[PREDECOMPRESSED] = await maybeDecompressAsync(buf, encoding, maxPayloadBytes)
            } catch {
              // Fall back to the extractor's sync decode path.
            }
          }
        }

        const result = await originalMethod.call(this, data)

        if (result && this.producer) {
          try {
            const replyTopic = `${eventPattern}.reply`
            const encoded = codec.encode(result)
            const compressed = maybeCompress(encoded, compression)
            await this.producer.send({
              topic: replyTopic,
              messages: [{
                key: result.uuid || "",
                value: Buffer.from(compressed.data),
                headers: compressed.encoding !== "identity" ? { "content-encoding": compressed.encoding } : undefined
              }]
            })
            if (options?.debug) logger.debug({ event: "kafka.reply.sent", topic: replyTopic })
          } catch (err) {
            logger.error({ event: "kafka.reply.send_failed", err: (err as Error)?.message })
          }
        }

        return result
      }

      MessagePattern(eventPattern)(target.prototype, handlerName, Object.getOwnPropertyDescriptor(target.prototype, handlerName)!)
    }
  )
}
