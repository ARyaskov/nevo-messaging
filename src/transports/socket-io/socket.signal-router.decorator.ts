import { Type } from "@nestjs/common"
import { createServer, Server as HttpServer } from "node:http"
import { createSignalRouterDecorator, SignalRouterOptions } from "../../signal-router.utils"
import { DEFAULT_DISCOVERY_TOPIC, DEFAULT_SUBSCRIPTION_SUFFIX, stringifyWithBigInt, getDefaultLogger, DlqRouter, matchesFilter } from "../../common"
import { getSocketIoModule } from "../optional-deps"

export interface SocketSignalRouterOptions extends SignalRouterOptions {
  port?: number
  path?: string
  cors?: any
  discovery?: {
    enabled?: boolean
    heartbeatIntervalMs?: number
  }
}

export function SocketSignalRouter(serviceType: Type<any> | Type<any>[], options?: SocketSignalRouterOptions) {
  const logger = options?.logger || getDefaultLogger().child({ component: "socket-router" })
  const dlq = options?.dlq instanceof DlqRouter ? options.dlq : new DlqRouter({ enabled: (options?.dlq as any)?.enabled === true })

  return createSignalRouterDecorator(
    serviceType,
    { ...options, logger, dlq },
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

        io.on("connection", (socket: any) => {
          socket.on("nevo:query", async (payload: any, ack: any) => {
            try {
              const response = await this[handlerName](payload)
              if (ack) ack(response)
            } catch (err) {
              logger.error({ event: "socket.query.error", err: (err as Error)?.message })
              if (ack) ack({ uuid: payload?.uuid, method: payload?.method, params: { result: "error", error: { code: 0, message: (err as Error)?.message } } })
            }
          })

          socket.on("nevo:emit", async (payload: any) => {
            try { await this[handlerName](payload) } catch (err) {
              logger.error({ event: "socket.emit.error", err: (err as Error)?.message })
              await dlq.route({ topic: eventPattern, reason: "emit-error", error: { message: (err as Error)?.message }, rawPayload: payload, ts: Date.now() })
            }
          })

          socket.on("nevo:identify", (data: any) => {
            const userId = data?.userId ?? data?.subjectId
            if (!userId) return
            ;(socket as any).__nevoUserId = String(userId)
            socket.join(`user:${userId}`)
          })

          socket.on("nevo:subscribe", (data: any) => {
            const serviceName = data?.serviceName?.toLowerCase?.()
            const method = data?.method
            const explicit = data?.room
            const stickyTo = data?.stickyUserId
            if (!serviceName) return
            socket.join(explicit ?? `${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`)
            if (method && !explicit) socket.join(`${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}:${method}`)
            if (stickyTo) socket.join(`user:${stickyTo}`)
          })

          socket.on("nevo:unsubscribe", (data: any) => {
            const serviceName = data?.serviceName?.toLowerCase?.()
            const method = data?.method
            const explicit = data?.room
            if (!serviceName) return
            socket.leave(explicit ?? `${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`)
            if (method && !explicit) socket.leave(`${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}:${method}`)
          })

          socket.on("nevo:publish", (payload: any) => {
            const serviceName = (options as any)?.serviceName || eventPattern.replace("-events", "")
            const baseRoom = `${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`
            const methodRoom = payload?.method ? `${baseRoom}:${payload.method}` : null
            const targetUserId = payload?.meta?.headers?.["nevo-target-user"]
            if (targetUserId) {
              io.to(`user:${targetUserId}`).emit("nevo:sub", payload)
              return
            }
            io.to(baseRoom).emit("nevo:sub", payload)
            if (methodRoom) io.to(methodRoom).emit("nevo:sub", payload)
          })

          socket.on("nevo:broadcast", (payload: any) => {
            io.emit("nevo:broadcast", payload)
          })
        })

        httpServer.listen(port)
        logger.info({ event: "socket.router.listen", port })

        const discoveryEnabled = options?.discovery?.enabled === true
        if (discoveryEnabled) {
          const interval = options?.discovery?.heartbeatIntervalMs || 10000
          this.socketDiscoveryTimer = setInterval(() => {
            const announcement = {
              serviceName: (options as any)?.serviceName || eventPattern.replace("-events", ""),
              transport: "socket.io",
              ts: Date.now()
            }
            io.emit(DEFAULT_DISCOVERY_TOPIC, stringifyWithBigInt(announcement))
          }, interval)
          if (typeof this.socketDiscoveryTimer.unref === "function") this.socketDiscoveryTimer.unref()
        }
      }

      const originalOnModuleDestroy = target.prototype.onModuleDestroy || function () {}
      target.prototype.onModuleDestroy = async function () {
        await originalOnModuleDestroy.call(this)
        if (this.socketDiscoveryTimer) clearInterval(this.socketDiscoveryTimer)
        if (this.socketServer) {
          try { await this.socketServer.close() } catch {}
        }
        if (this.socketHttpServer) {
          try { this.socketHttpServer.close() } catch {}
        }
      }
    }
  )
}
