import { Type } from "@nestjs/common"
import { MessagePattern } from "@nestjs/microservices"
import { createSignalRouterDecorator, SignalRouterOptions } from "../../signal-router.utils"
import { parseWithBigInt, stringifyWithBigInt } from "../../common"
import { getKafkaModule } from "../optional-deps"

export function KafkaSignalRouter(serviceType: Type<any> | Type<any>[], options?: SignalRouterOptions) {
  return createSignalRouterDecorator(
    serviceType,
    options,
    (data) => {
      let messageData = data
      if (data && data.value && typeof data.value === "string") {
        try {
          messageData = parseWithBigInt(data.value)
        } catch (e) {
          console.error("Failed to parse JSON from message:", e)
        }
      }

      return {
        method: messageData.method,
        params: messageData.params,
        uuid: messageData.uuid,
        meta: messageData.meta
      }
    },
    (target, eventPattern, handlerName) => {
      const originalMethod = target.prototype[handlerName]
      target.prototype.producer = null
      const originalOnModuleInit = target.prototype.onModuleInit || function () {}
      target.prototype.onModuleInit = async function () {
        await originalOnModuleInit.call(this)

        const kafkaHost = process.env["KAFKA_HOST"] || "localhost"
        const kafkaPort = process.env["KAFKA_PORT"] || "9092"

        const { Kafka } = getKafkaModule()
        const kafka = new Kafka({
          clientId: `${eventPattern}-producer`,
          brokers: [`${kafkaHost}:${kafkaPort}`]
        })

        this.producer = kafka.producer()
        await this.producer.connect()

        if (options?.debug) {
          console.log(`[${eventPattern}] Kafka Producer connected for reply topics`)
        }
      }

      const originalOnModuleDestroy = target.prototype.onModuleDestroy || function () {}
      target.prototype.onModuleDestroy = async function () {
        await originalOnModuleDestroy.call(this)

        if (this.producer) {
          await this.producer.disconnect()
          if (options?.debug) {
            console.log(`[${eventPattern}] Kafka Producer disconnected`)
          }
        }
      }

      target.prototype[handlerName] = async function (data: any) {
        const result = await originalMethod.call(this, data)

        if (result && this.producer) {
          try {
            const replyTopic = `${eventPattern}.reply`

            await this.producer.send({
              topic: replyTopic,
              messages: [
                {
                  key: result.uuid || "",
                  value: stringifyWithBigInt(result)
                }
              ]
            })

            if (options?.debug) {
              console.log(`[${eventPattern}] Sent response to ${replyTopic}:`, stringifyWithBigInt(result))
            }
          } catch (error) {
            console.error(`[${eventPattern}] Failed to send to reply topic:`, error)
          }
        }

        return result
      }

      MessagePattern(eventPattern)(target.prototype, handlerName, Object.getOwnPropertyDescriptor(target.prototype, handlerName)!)
    }
  )
}
