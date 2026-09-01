import { Type } from "@nestjs/common"
import { createServer, Server as HttpServer } from "node:http"
import { createSignalRouterDecorator, SignalRouterOptions, getRouterRuntime } from "../../signal-router.utils"
import { Codec, getCodec, getDefaultCodec, formatMethod, parseMethod, DEFAULT_METHOD_VERSION } from "../../common"
import { getWsModule } from "./optional-ws"

export interface WsSignalRouterOptions extends SignalRouterOptions {
  port?: number
  host?: string
  path?: string
  codec?: Codec | string
  perMessageDeflate?:
    | boolean
    | {
        threshold?: number
        serverMaxWindowBits?: number
        clientMaxWindowBits?: number
        zlibDeflateOptions?: { level?: number; memLevel?: number }
      }
  maxPayload?: number
}

export function WsSignalRouter(serviceType: Type<any> | Type<any>[], options?: WsSignalRouterOptions) {
  const codec: Codec = typeof options?.codec === "string" ? getCodec(options.codec) : (options?.codec as Codec) || getDefaultCodec()

  return createSignalRouterDecorator(
    serviceType,
    options ?? {},
    (data) => {
      const messageData: any = data || {}
      return {
        method: messageData.method,
        params: messageData.params,
        uuid: messageData.uuid,
        meta: messageData.meta
      }
    },
    (target, eventPattern, handlerName) => {
      target.prototype.wsServer = null
      target.prototype.wsHttpServer = null
      target.prototype.wsSubscribers = null

      const originalOnModuleInit = target.prototype.onModuleInit || function () {}
      target.prototype.onModuleInit = async function () {
        await originalOnModuleInit.call(this)

        const runtime = getRouterRuntime(this.constructor)
        const logger = runtime.logger
        const dlq = runtime.dlq

        const port = options?.port ?? 3200
        const host = options?.host || "0.0.0.0"
        const path = options?.path || "/"
        const httpServer: HttpServer = createServer()
        const { WebSocketServer } = getWsModule()
        const wssOpts: any = { server: httpServer, path }
        if (options?.perMessageDeflate !== undefined) wssOpts.perMessageDeflate = options.perMessageDeflate
        if (options?.maxPayload) wssOpts.maxPayload = options.maxPayload
        const wss = new WebSocketServer(wssOpts)
        this.wsServer = wss
        this.wsHttpServer = httpServer
        this.wsSubscribers = new Set()
        const subscribers = this.wsSubscribers as Set<any>

        wss.on("connection", (socket: any) => {
          subscribers.add(socket)
          socket.on("close", () => subscribers.delete(socket))
          socket.on("message", async (raw: Buffer) => {
            let envelope: any
            try {
              envelope = codec.decode(new Uint8Array(raw))
            } catch (err) {
              logger.warn({ event: "ws.decode_error", err: (err as Error)?.message })
              return
            }

            const parsed = typeof envelope?.method === "string" ? parseMethod(envelope.method) : null

            if (parsed?.name === "__subscribe") {
              ;(socket as any).__subscriptions ??= new Set()
              const key = `${envelope.params?.serviceName?.toLowerCase?.()}:${envelope.params?.method}`
              ;(socket as any).__subscriptions.add(key)
              return
            }

            const metaType = envelope?.meta?.type
            if (metaType === "sub" || metaType === "broadcast") {
              for (const subscriber of subscribers) {
                if (subscriber.readyState !== 1) continue
                const subscriptions: Set<string> | undefined = (subscriber as any).__subscriptions
                if (!subscriptions || subscriptions.size === 0) continue
                let matched = false
                for (const subKey of subscriptions) {
                  const subMethod = subKey.slice(subKey.indexOf(":") + 1)
                  if (
                    subMethod === envelope.method ||
                    subMethod === parsed?.name ||
                    formatMethod(subMethod, DEFAULT_METHOD_VERSION) === envelope.method
                  ) {
                    matched = true
                    break
                  }
                }
                if (!matched) continue
                try {
                  subscriber.send(raw)
                } catch {}
              }
              return
            }

            try {
              const response = await this[handlerName](envelope)
              if (response) {
                socket.send(codec.encode(response))
              }
            } catch (err) {
              logger.error({ event: "ws.handler_error", err: (err as Error)?.message })
              await dlq.route({
                topic: eventPattern,
                reason: "handler-error",
                error: { message: (err as Error)?.message ?? String(err) },
                rawPayload: envelope,
                ts: Date.now()
              })
            }
          })
        })

        httpServer.listen(port, host)
        logger.info({ event: "ws.router.listen", port, host, path })
      }

      const originalOnModuleDestroy = target.prototype.onModuleDestroy || function () {}
      target.prototype.onModuleDestroy = async function () {
        await originalOnModuleDestroy.call(this)
        if (this.wsServer) {
          try {
            this.wsServer.close()
          } catch {}
        }
        if (this.wsHttpServer) {
          try {
            this.wsHttpServer.close()
          } catch {}
        }
      }
    }
  )
}
