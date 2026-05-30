import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import {
  DlqRouter,
  ErrorCode,
  HealthRegistry,
  InMemoryDlqStore,
  createNevoKafkaClient,
  eventLoopLagPing,
  memoryUsagePing
} from "@riaskov/nevo-messaging"
import { UserController } from "./user.controller"
import { UserService } from "./user.service"
import { AuthService } from "./auth/auth.service"

@Module({
  imports: [ConfigModule],
  controllers: [UserController],
  providers: [
    UserService,
    AuthService,

    // Health probes — Kafka readiness can be added with kafkaAdminPing once an admin handle is available.
    {
      provide: HealthRegistry,
      useFactory: () => {
        const reg = new HealthRegistry({ serviceName: "user", version: "2.0.0" })
        reg.register("eventLoop", eventLoopLagPing(100), { kind: "liveness" })
        reg.register("memory", memoryUsagePing(1024), { kind: "liveness" })
        return reg
      }
    },

    // DLQ — capture envelopes that exhaust retries.
    {
      provide: DlqRouter,
      useFactory: () => {
        const store = new InMemoryDlqStore()
        const dlq = new DlqRouter({ enabled: true, store, redactPaths: ["*.password", "*.token"] })
        dlq.addSink(async (entry) => {
          console.warn(`[dlq] ${entry.topic}/${entry.reason}`, entry.error)
        })
        return dlq
      }
    },

    createNevoKafkaClient(["COORDINATOR", "WALLET"], {
      clientIdPrefix: "user",
      groupIdPrefix: process.env.NODE_ENV === "production" ? "prod-user" : "dev-user",
      // Both fields are env-var *names* the framework reads at startup (not literal values).
      kafkaHost: "KAFKA_HOST",
      kafkaPort: "KAFKA_PORT",

      // Resilience — retry only transient errors, then move to DLQ.
      retry: {
        enabled: true,
        maxAttempts: 4,
        baseMs: 200,
        maxMs: 5_000,
        jitter: true,
        retryOnCodes: [ErrorCode.TIMEOUT, ErrorCode.SERVICE_UNAVAILABLE, ErrorCode.CONNECTION_LOST]
      },

      // Cost-based circuit breaker keeps short-tail spikes from cascading.
      circuitBreaker: {
        enabled: true,
        failureThreshold: 10,
        resetTimeoutMs: 30_000,
        halfOpenSuccessThreshold: 2
      },

      // Move exhausted retries to <topic>.dlq automatically.
      dlq: { enabled: true },

      discovery: {
        enabled: true,
        heartbeatIntervalMs: 5_000,
        ttlMs: 15_000
      },

      compression: { enabled: true, algorithm: "gzip", threshold: 2048 }
    })
  ]
})
export class UserModule {}
