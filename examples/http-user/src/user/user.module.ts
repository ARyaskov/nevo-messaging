import { Module } from "@nestjs/common"
import {
  HealthRegistry,
  HttpTransportController,
  createNevoHttpClient,
  eventLoopLagPing,
  httpPing,
  memoryUsagePing
} from "@riaskov/nevo-messaging"
import { UserController } from "./user.controller"
import { UserService } from "./user.service"

const COORDINATOR_URL = process.env.HTTP_COORDINATOR ?? "http://127.0.0.1:8091"

@Module({
  controllers: [UserController, HttpTransportController],
  providers: [
    UserService,
    {
      provide: HealthRegistry,
      useFactory: () => {
        const reg = new HealthRegistry({ serviceName: "user", version: "2.0.0" })
        reg.register("eventLoop", eventLoopLagPing(100), { kind: "liveness" })
        reg.register("memory", memoryUsagePing(1024), { kind: "liveness" })
        // Readiness depends on the coordinator being reachable.
        reg.register(
          "coordinator",
          httpPing(`${COORDINATOR_URL}/healthz`, { timeoutMs: 2_000 }),
          { kind: "readiness", timeoutMs: 3_000 }
        )
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
