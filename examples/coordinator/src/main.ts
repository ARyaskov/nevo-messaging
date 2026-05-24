import {
  GracefulShutdown,
  HealthRegistry,
  Outbox,
  createKafkaMicroservice,
  createLogger,
  setDefaultLogger
} from "@riaskov/nevo-messaging"
import { AppModule } from "./app.module"

async function bootstrap() {
  setDefaultLogger(createLogger({ name: "coordinator", level: (process.env.LOG_LEVEL as any) ?? "info" }))

  const app = await createKafkaMicroservice({
    microserviceName: "coordinator",
    module: AppModule,
    port: 8085
  })

  const shutdown = new GracefulShutdown()
  const health = app.get(HealthRegistry)
  health.register(
    "not-draining",
    () => ({ status: shutdown.isShuttingDown() ? "down" : "ok" }),
    { kind: "readiness" }
  )

  // Flush the outbox before tearing down the Kafka client.
  const outbox = app.get(Outbox)
  shutdown.register("stop outbox", async () => {
    outbox.stop()
    await outbox.flushOnce()
  })
  shutdown.register("close nest app", () => app.close())

  for (const sig of ["SIGTERM", "SIGINT"] as const) {
    process.on(sig, async () => {
      if (shutdown.isShuttingDown()) return
      await shutdown.shutdown(30_000)
      process.exit(0)
    })
  }
}

bootstrap().catch((err) => {
  console.error(err)
  process.exit(1)
})
