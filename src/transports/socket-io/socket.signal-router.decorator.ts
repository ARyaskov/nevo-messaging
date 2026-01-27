import { Type } from "@nestjs/common"
import { createServer, Server as HttpServer } from "node:http"
import { createSignalRouterDecorator, SignalRouterOptions } from "../../signal-router.utils"
import { DEFAULT_DISCOVERY_TOPIC, DEFAULT_SUBSCRIPTION_SUFFIX, stringifyWithBigInt } from "../../common"
import { getSocketIoModule } from "../optional-deps"

export interface SocketSignalRouterOptions extends SignalRouterOptions {
  port?: number
  path?: string
  cors?: any
  serviceName?: string
  discovery?: {
    enabled?: boolean
    heartbeatIntervalMs?: number
  }
}

export function SocketSignalRouter(serviceType: Type<any> | Type<any>[], options?: SocketSignalRouterOptions) {
  return createSignalRouterDecorator(
    serviceType,
    options,
    (data) => {
      const messageData = data || {}
      return {
        method: messageData.method,
        params: messageData.params,
        uuid: messageData.uuid,
        meta: messageData.meta
      }
    },
    (target, eventPattern, handlerName) => {
      target.prototype.socketServer = null
      target.prototype.socketHttpServer = null
      target.prototype.socketDiscoveryTimer = null

      const originalOnModuleInit = target.prototype.onModuleInit || function () {}
      target.prototype.onModuleInit = async function () {
        await originalOnModuleInit.call(this)

        const port = options?.port || 3100
        const path = options?.path || "/socket.io"
        const httpServer: HttpServer = createServer()
        const { Server: SocketServer } = getSocketIoModule()
        const io = new SocketServer(httpServer, {
          path,
          cors: options?.cors || { origin: "*" }
        })

        this.socketServer = io
        this.socketHttpServer = httpServer

        io.on("connection", (socket) => {
          socket.on("nevo:query", async (payload, ack) => {
            const response = await this[handlerName](payload)
            if (ack) {
              ack(response)
            }
          })

          socket.on("nevo:emit", async (payload) => {
            await this[handlerName](payload)
          })

          socket.on("nevo:subscribe", (data) => {
            const serviceName = data?.serviceName?.toLowerCase()
            const method = data?.method
            if (!serviceName) {
              return
            }
            socket.join(`${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`)
            if (method) {
              socket.join(`${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}:${method}`)
            }
          })

          socket.on("nevo:unsubscribe", (data) => {
            const serviceName = data?.serviceName?.toLowerCase()
            const method = data?.method
            if (!serviceName) {
              return
            }
            socket.leave(`${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`)
            if (method) {
              socket.leave(`${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}:${method}`)
            }
          })

          socket.on("nevo:publish", (payload) => {
            const serviceName = options?.serviceName || eventPattern.replace("-events", "")
            const baseRoom = `${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`
            const methodRoom = payload?.method ? `${baseRoom}:${payload.method}` : null
            io.to(baseRoom).emit("nevo:sub", payload)
            if (methodRoom) {
              io.to(methodRoom).emit("nevo:sub", payload)
            }
          })

          socket.on("nevo:broadcast", (payload) => {
            io.emit("nevo:broadcast", payload)
          })
        })

        httpServer.listen(port)

        if (options?.debug) {
          console.log(`[${eventPattern}] Socket.IO server listening on port ${port}`)
        }

        const discoveryEnabled = options?.discovery?.enabled === true
        if (discoveryEnabled) {
          const interval = options?.discovery?.heartbeatIntervalMs || 5000
          this.socketDiscoveryTimer = setInterval(() => {
            const announcement = {
              serviceName: options?.serviceName || eventPattern.replace("-events", ""),
              transport: "socket.io",
              ts: Date.now()
            }
            io.emit(DEFAULT_DISCOVERY_TOPIC, stringifyWithBigInt(announcement))
          }, interval)
        }
      }

      const originalOnModuleDestroy = target.prototype.onModuleDestroy || function () {}
      target.prototype.onModuleDestroy = async function () {
        await originalOnModuleDestroy.call(this)

        if (this.socketDiscoveryTimer) {
          clearInterval(this.socketDiscoveryTimer)
        }
        if (this.socketServer) {
          await this.socketServer.close()
        }
        if (this.socketHttpServer) {
          this.socketHttpServer.close()
        }
      }
    }
  )
}
