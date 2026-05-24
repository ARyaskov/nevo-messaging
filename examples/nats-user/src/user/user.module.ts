import { Module } from "@nestjs/common"
import { ErrorCode, HealthRegistry, createNevoNatsClient, eventLoopLagPing, memoryUsagePing } from "@riaskov/nevo-messaging"
import { UserController } from "./user.controller"
import { UserService } from "./user.service"

@Module({
  controllers: [UserController],
  providers: [
    UserService,
    {
      provide: HealthRegistry,
      useFactory: () => {
        const reg = new HealthRegistry({ serviceName: "user", version: "2.0.0" })
        reg.register("eventLoop", eventLoopLagPing(100), { kind: "liveness" })
        reg.register("memory", memoryUsagePing(1024), { kind: "liveness" })
        // Add readiness checks (DB / Redis / NATS / Kafka pings) as the service grows.
        return reg
      }
    },
    createNevoNatsClient(["COORDINATOR", "USER"], {
      clientIdPrefix: "user",
      servers: (process.env.NATS_SERVERS ?? "nats://127.0.0.1:4222").split(","),

      // Reconnect with infinite attempts and lazy first connection (useful in tests).
      reconnect: {
        enabled: true,
        timeWaitMs: 5_000,
        maxAttempts: -1,
        jitterMs: 100,
        lazyConnect: false,
        waitOnFirstConnect: true
      },

      // Retry only transient transport-level errors.
      retry: {
        enabled: true,
        maxAttempts: 3,
        baseMs: 100,
        maxMs: 2_000,
        jitter: true,
        retryOnCodes: [ErrorCode.TIMEOUT, ErrorCode.CONNECTION_LOST, ErrorCode.SERVICE_UNAVAILABLE]
      },

      // Surface slow consumers as warnings before the channel buffer overflows.
      subscribeMaxPending: 2048,
      subscribeOnSlow: ({ subject, pending }) => {
        console.warn(`[nats] slow consumer on '${subject}' (${pending} pending)`)
      },

      // Compress payloads above 1 KB.
      compression: {
        enabled: true,
        algorithm: "gzip",
        threshold: 1024,
        level: 6
      }
    })
  ]
})
export class UserModule {}
