import { Type } from "@nestjs/common"
import { createServer, Server as HttpServer } from "node:http"
import { createSignalRouterDecorator, SignalRouterOptions, getRouterRuntime } from "../../signal-router.utils"
import { DEFAULT_DISCOVERY_TOPIC, DEFAULT_SUBSCRIPTION_SUFFIX, stringifyWithBigInt, formatMethod, DEFAULT_METHOD_VERSION } from "../../common"
import { getSocketIoModule } from "../optional-deps"

export interface SocketSignalRouterOptions extends SignalRouterOptions {
  port?: number
  path?: string
  cors?: any
  verifyIdentity?: (socket: any, userId: string, authToken?: string) => boolean | Promise<boolean>
  allowClientPublish?: boolean
  allowClientBroadcast?: boolean
  discovery?: {
    enabled?: boolean
    heartbeatIntervalMs?: number
  }
}

function toVersionedMethod(method: unknown): string | null {
  if (typeof method !== "string" || method.length === 0) return null
  return method.includes("@") ? method : formatMethod(method, DEFAULT_METHOD_VERSION)
}

export function SocketSignalRouter(serviceType: Type<any> | Type<any>[], options?: SocketSignalRouterOptions) {
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
      target.prototype.socketServer = null
      target.prototype.socketHttpServer = null
      target.prototype.socketDiscoveryTimer = null

      const originalOnModuleInit = target.prototype.onModuleInit || function () {}
      target.prototype.onModuleInit = async function () {
        await originalOnModuleInit.call(this)

        const runtime = getRouterRuntime(this.constructor)
        const logger = runtime.logger
        const dlq = runtime.dlq

        const port = options?.port || 3100
        const path = options?.path || "/socket.io"
        const httpServer: HttpServer = createServer()
        const { Server: SocketServer } = getSocketIoModule()
        const serverOptions: any = { path }
        if (options?.cors !== undefined) serverOptions.cors = options.cors
        const io = new SocketServer(httpServer, serverOptions)

        this.socketServer = io
        this.socketHttpServer = httpServer

        let identifyWarned = false
        let publishWarned = false
        let broadcastWarned = false
        const verifyUserId = async (socket: any, userId: string, authToken?: string): Promise<boolean> => {
          if (!options?.verifyIdentity) {
            if (!identifyWarned) {
              identifyWarned = true
              logger.warn({ event: "socket.identify.rejected", reason: "no verifyIdentity callback configured" })
            }
            return false
          }
          try {
            return (await options.verifyIdentity(socket, userId, authToken)) === true
          } catch (err) {
            logger.warn({ event: "socket.identify.verify_error", err: (err as Error)?.message })
            return false
          }
        }

        io.on("connection", (socket: any) => {
          socket.on("nevo:query", async (payload: any, ack: any) => {
            try {
              const response = await this[handlerName](payload)
              if (ack) ack(response)
            } catch (err) {
              logger.error({ event: "socket.query.error", err: (err as Error)?.message })
              if (ack)
                ack({
                  uuid: payload?.uuid,
                  method: payload?.method,
                  params: { result: "error", error: { code: 0, message: (err as Error)?.message } }
                })
            }
          })

          socket.on("nevo:emit", async (payload: any) => {
            try {
              await this[handlerName](payload)
            } catch (err) {
              logger.error({ event: "socket.emit.error", err: (err as Error)?.message })
              await dlq.route({
                topic: eventPattern,
                reason: "emit-error",
                error: { message: (err as Error)?.message },
                rawPayload: payload,
                ts: Date.now()
              })
            }
          })

          socket.on("nevo:identify", async (data: any) => {
            const userId = data?.userId ?? data?.subjectId
            if (!userId) return
            if (!(await verifyUserId(socket, String(userId), data?.authToken ?? data?.meta?.auth?.token))) return
            ;(socket as any).__nevoUserId = String(userId)
            socket.join(`user:${userId}`)
          })

          socket.on("nevo:subscribe", async (data: any) => {
            const serviceName = data?.serviceName?.toLowerCase?.()
            const method = toVersionedMethod(data?.method)
            const explicit = data?.room
            const stickyTo = data?.stickyUserId
            if (!serviceName) return
            if (explicit) socket.join(explicit)
            else if (method) socket.join(`${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}:${method}`)
            else socket.join(`${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`)
            if (stickyTo && (await verifyUserId(socket, String(stickyTo), data?.authToken ?? data?.meta?.auth?.token))) {
              socket.join(`user:${stickyTo}`)
            }
          })

          socket.on("nevo:unsubscribe", (data: any) => {
            const serviceName = data?.serviceName?.toLowerCase?.()
            const method = toVersionedMethod(data?.method)
            const explicit = data?.room
            if (!serviceName) return
            if (explicit) socket.leave(explicit)
            else if (method) socket.leave(`${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}:${method}`)
            else socket.leave(`${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`)
          })

          socket.on("nevo:publish", (payload: any) => {
            if (options?.allowClientPublish !== true) {
              if (!publishWarned) {
                publishWarned = true
                logger.warn({ event: "socket.publish.rejected", reason: "allowClientPublish is disabled" })
              }
              return
            }
            const serviceName = String((options as any)?.serviceName || eventPattern.replace("-events", "")).toLowerCase()
            const baseRoom = `${serviceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`
            const method = toVersionedMethod(payload?.method)
            const methodRoom = method ? `${baseRoom}:${method}` : null
            const targetUserId = payload?.meta?.headers?.["nevo-target-user"]
            if (targetUserId) {
              io.to(`user:${targetUserId}`).emit("nevo:sub", payload)
              return
            }
            io.to(baseRoom).emit("nevo:sub", payload)
            if (methodRoom) io.to(methodRoom).emit("nevo:sub", payload)
          })

          socket.on("nevo:broadcast", (payload: any) => {
            if (options?.allowClientBroadcast !== true) {
              if (!broadcastWarned) {
                broadcastWarned = true
                logger.warn({ event: "socket.broadcast.rejected", reason: "allowClientBroadcast is disabled" })
              }
              return
            }
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
          try {
            await this.socketServer.close()
          } catch {}
        }
        if (this.socketHttpServer) {
          try {
            this.socketHttpServer.close()
          } catch {}
        }
      }
    }
  )
}
