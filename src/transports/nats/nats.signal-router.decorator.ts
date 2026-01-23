import { Type } from "@nestjs/common"
import { connect, NatsConnection, StringCodec, Subscription } from "nats"
import { createSignalRouterDecorator, SignalRouterOptions } from "../../signal-router.utils"
import { parseWithBigInt, stringifyWithBigInt } from "../../common"

export interface NatsSignalRouterOptions extends SignalRouterOptions {
  servers?: string[]
}

export function NatsSignalRouter(serviceType: Type<any> | Type<any>[], options?: NatsSignalRouterOptions) {
  return createSignalRouterDecorator(
    serviceType,
    options,
    (data) => {
      const messageData = typeof data === "string" ? parseWithBigInt(data) : data
      return {
        method: messageData.method,
        params: messageData.params,
        uuid: messageData.uuid,
        meta: messageData.meta
      }
    },
    (target, eventPattern, handlerName) => {
      const codec = StringCodec()
      target.prototype.natsConnection = null
      target.prototype.natsSubscription = null

      const originalOnModuleInit = target.prototype.onModuleInit || function () {}
      target.prototype.onModuleInit = async function () {
        await originalOnModuleInit.call(this)

        const servers = options?.servers && options.servers.length > 0 ? options.servers : ["nats://127.0.0.1:4222"]
        const nc: NatsConnection = await connect({ servers })
        this.natsConnection = nc

        const sub: Subscription = nc.subscribe(eventPattern)
        this.natsSubscription = sub
        ;(async () => {
          for await (const msg of sub) {
            const payload = codec.decode(msg.data)
            const result = await this[handlerName](payload)
            if (msg.reply && result) {
              nc.publish(msg.reply, codec.encode(stringifyWithBigInt(result)))
            }
          }
        })()
      }

      const originalOnModuleDestroy = target.prototype.onModuleDestroy || function () {}
      target.prototype.onModuleDestroy = async function () {
        await originalOnModuleDestroy.call(this)
        if (this.natsSubscription) {
          this.natsSubscription.unsubscribe()
        }
        if (this.natsConnection) {
          await this.natsConnection.close()
        }
      }
    }
  )
}
