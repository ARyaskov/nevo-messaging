import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import {
  ErrorCode,
  HealthRegistry,
  InMemoryOutboxStore,
  Outbox,
  type OutboxPublisher,
  createNevoKafkaClient,
  eventLoopLagPing,
  memoryUsagePing
} from "@riaskov/nevo-messaging"
import { CoordinatorController } from "./coordinator.controller"
import { CoordinatorService } from "./coordinator.service"

@Module({
  imports: [ConfigModule],
  controllers: [CoordinatorController],
  providers: [
    CoordinatorService,

    {
      provide: HealthRegistry,
      useFactory: () => {
        const reg = new HealthRegistry({ serviceName: "coordinator", version: "2.0.0" })
        reg.register("eventLoop", eventLoopLagPing(100), { kind: "liveness" })
        reg.register("memory", memoryUsagePing(1024), { kind: "liveness" })
        return reg
      }
    },

    // Outbox — at-least-once event publish after a local write.
    // The publisher must come from the Kafka client base; we inject the client into a small adapter.
    {
      provide: Outbox,
      inject: ["NEVO_KAFKA_CLIENT"],
      useFactory: (client: any) => {
        const store = new InMemoryOutboxStore()
        const publisher: OutboxPublisher = {
          emit: (service: string, method: string, params: unknown) => client.emit(service, method, params)
        }
        const outbox = new Outbox(store, publisher, {
          intervalMs: 200,
          batch: 100,
          maxAttempts: 8
        })
        outbox.start()
        return outbox
      }
    },

    createNevoKafkaClient(["USER", "WALLET"], {
      clientIdPrefix: "coordinator",
      groupIdPrefix: process.env.NODE_ENV === "production" ? "prod-coordinator" : "dev-coordinator",
      // Both fields are env-var *names* the framework reads at startup (not literal values).
      kafkaHost: "KAFKA_HOST",
      kafkaPort: "KAFKA_PORT",

      retry: {
        enabled: true,
        maxAttempts: 4,
        baseMs: 200,
        maxMs: 5_000,
        jitter: true,
        retryOnCodes: [ErrorCode.TIMEOUT, ErrorCode.SERVICE_UNAVAILABLE, ErrorCode.CONNECTION_LOST]
      },

      circuitBreaker: {
        enabled: true,
        failureThreshold: 8,
        resetTimeoutMs: 30_000,
        halfOpenSuccessThreshold: 2
      },

      discovery: {
        enabled: true,
        heartbeatIntervalMs: 5_000,
        ttlMs: 15_000
      },

      compression: { enabled: true, algorithm: "gzip", threshold: 2048 }
    })
  ]
})
export class CoordinatorModule {}
