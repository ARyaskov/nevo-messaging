import { Module } from "@nestjs/common"
import {
  HealthRegistry,
  HttpTransportController,
  NevoModule,
  createHttpTransportOptionsProvider,
  createNevoHttpClient,
  eventLoopLagPing,
  httpPing,
  memoryUsagePing
} from "@riaskov/nevo-messaging"
import { UserController } from "./user.controller"
import { UserService } from "./user.service"

const COORDINATOR_URL = process.env.HTTP_COORDINATOR ?? "http://127.0.0.1:8091"

@Module({
  imports: [
    // Request-processing options for every @*SignalRouter controller in this app.
    // The factory can inject anything, so stores and registries stay real providers.
    NevoModule.forRootAsync({
      inject: [HealthRegistry],
      useFactory: (health: HealthRegistry) => ({
        serviceName: "user",
        serviceVersion: "2.0.0",
        health,
        accessControl: {
          rules: [
            { topic: "user-events", method: "*", allow: ["frontend", "coordinator"] },
            { topic: "user-events", method: "user.delete", allow: ["coordinator"] }
          ],
          logDenied: true,
          allowAllByDefault: false
        }
      })
    })
  ],
  controllers: [UserController, HttpTransportController],
  providers: [
    UserService,

    // HttpTransportController's endpoints let a caller inject messages into any
    // service's pub/sub channel, so they fail closed unless configured. This demo
    // runs on a local network and opts out; a real deployment passes
    // `authorize: (req) => ...` instead.
    createHttpTransportOptionsProvider({ insecure: true }),

    {
      provide: HealthRegistry,
      useFactory: () => {
        const reg = new HealthRegistry({ serviceName: "user", version: "2.0.0" })
        reg.register("eventLoop", eventLoopLagPing(100), { kind: "liveness" })
        reg.register("memory", memoryUsagePing(1024), { kind: "liveness" })
        // Readiness depends on the coordinator being reachable.
        reg.register("coordinator", httpPing(`${COORDINATOR_URL}/healthz`, { timeoutMs: 2_000 }), { kind: "readiness", timeoutMs: 3_000 })
        return reg
      }
    },
    createNevoHttpClient(
      { coordinator: COORDINATOR_URL },
      {
        clientIdPrefix: "user",
        timeoutMs: 10_000,

        // Agent tuning.
        keepAlive: true,
        maxSockets: 64,
        maxFreeSockets: 16,
        tcpNoDelay: true,
        socketKeepAliveMs: 30_000,
        recvBufferSize: 256 * 1024,

        // DNS cache — peer-optional (`cacheable-lookup`); falls back silently if not installed.
        cacheableDns: { ttl: 60_000, maxTtl: 600_000 },

        // Compression on the wire.
        compression: { enabled: true, algorithm: "gzip", threshold: 1024 }
      }
    )
  ]
})
export class UserModule {}
