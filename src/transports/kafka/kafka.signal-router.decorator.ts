import { Type } from "@nestjs/common"
import { MessagePattern } from "@nestjs/microservices"
import { createSignalRouterDecorator, SignalRouterOptions, getRouterRuntime } from "../../signal-router.utils"
import { Codec, getCodec, getDefaultCodec, maybeDecompress, maybeDecompressAsync, shouldDecompressAsync, enforcePayloadLimit } from "../../common"

// Buffer the async wrapper pre-inflated, for the sync extractor to reuse.
const PREDECOMPRESSED = Symbol("nevo.kafka.predecompressed")

// Normalises a Kafka message value to a Buffer; null when there is no decodable value.
function kafkaValueBuffer(data: any): Buffer | null {
  if (!(data && data.value && (typeof data.value === "string" || data.value instanceof Buffer || data.value instanceof Uint8Array))) return null
  return data.value instanceof Buffer ? data.value : typeof data.value === "string" ? Buffer.from(data.value, "utf8") : Buffer.from(data.value)
}

export interface KafkaSignalRouterOptions extends SignalRouterOptions {
  brokers?: string[]
  kafkaHost?: string
  kafkaPort?: string
  codec?: Codec | string
  compression?: { enabled?: boolean; algorithm?: "gzip" | "deflate"; threshold?: number; level?: number }
}

export function KafkaSignalRouter(serviceType: Type<any> | Type<any>[], options?: KafkaSignalRouterOptions) {
  const codec: Codec = typeof options?.codec === "string" ? getCodec(options.codec) : (options?.codec as Codec) || getDefaultCodec()

  return createSignalRouterDecorator(
    serviceType,
    options ?? {},
    (data, ctx) => {
      let messageData = data
      const buf = kafkaValueBuffer(data)
      if (buf) {
        try {
          const encoding = data?.headers?.["content-encoding"]?.toString?.()
          // Reuse the pre-inflated buffer when present; otherwise inflate synchronously.
          const predecompressed = (data as any)[PREDECOMPRESSED] as Uint8Array | undefined
          const decompressed = predecompressed ?? maybeDecompress(buf, encoding, ctx.maxPayloadBytes)
          enforcePayloadLimit(decompressed, ctx.maxPayloadBytes)
          messageData = codec.decode(decompressed)
        } catch (e) {
          ctx.logger.error({ event: "kafka.decode_error", err: (e as Error)?.message }, "Failed to decode message")
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

      target.prototype[handlerName] = async function (data: any, context?: any) {
        let message = data
        if (typeof data === "string" || data instanceof Uint8Array) {
          message = { value: data, headers: context?.getMessage?.()?.headers }
        }

        // Inflate large compressed payloads off the event loop before the sync extractor runs.
        const buf = kafkaValueBuffer(message)
        if (buf) {
          const encoding = message?.headers?.["content-encoding"]?.toString?.()
          if (shouldDecompressAsync(buf.byteLength, encoding)) {
            try {
              ;(message as any)[PREDECOMPRESSED] = await maybeDecompressAsync(buf, encoding, getRouterRuntime(this.constructor).maxPayloadBytes)
            } catch {
              // Fall back to the extractor's sync decode path.
            }
          }
        }

        return originalMethod.call(this, message)
      }

      MessagePattern(eventPattern)(target.prototype, handlerName, Object.getOwnPropertyDescriptor(target.prototype, handlerName)!)
    }
  )
}
