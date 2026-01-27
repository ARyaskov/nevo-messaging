import { Type } from "@nestjs/common"
import type { NatsConnection, Subscription } from "nats"
import { createSignalRouterDecorator, SignalRouterOptions } from "../../signal-router.utils"
import { parseWithBigInt, stringifyWithBigInt } from "../../common"
import { getNatsModule } from "../optional-deps"

export interface NatsSignalRouterOptions extends SignalRouterOptions {
  servers?: string[]
  reconnect?: {
    enabled?: boolean
    maxAttempts?: number
    timeWaitMs?: number
    jitterMs?: number
    jitterTlsMs?: number
    waitOnFirstConnect?: boolean
    lazyConnect?: boolean
  }
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
      const { connect, StringCodec } = getNatsModule()
      const codec = StringCodec()
      target.prototype.natsConnection = null
      target.prototype.natsSubscription = null

      const originalOnModuleInit = target.prototype.onModuleInit || function () {}
      target.prototype.onModuleInit = async function () {
        await originalOnModuleInit.call(this)

        const servers = options?.servers && options.servers.length > 0 ? options.servers : ["nats://127.0.0.1:4222"]
        const reconnectEnabled = options?.reconnect?.enabled !== false
        const maxAttempts = options?.reconnect?.maxAttempts ?? -1
        const timeWaitMs = options?.reconnect?.timeWaitMs ?? 5000
        const jitterMs = options?.reconnect?.jitterMs
        const jitterTlsMs = options?.reconnect?.jitterTlsMs
        const lazyConnect = options?.reconnect?.lazyConnect === true
        const waitOnFirstConnect = options?.reconnect?.waitOnFirstConnect ?? !lazyConnect
        const nc: NatsConnection = await connect({
          servers,
          maxReconnectAttempts: reconnectEnabled ? maxAttempts : 0,
          reconnectTimeWait: timeWaitMs,
          reconnectJitter: jitterMs,
          reconnectJitterTLS: jitterTlsMs,
          waitOnFirstConnect
        })
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
