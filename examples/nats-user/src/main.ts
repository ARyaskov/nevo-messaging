import {
  GracefulShutdown,
  HealthRegistry,
  createNatsMicroservice,
  createLogger,
  setDefaultLogger,
  setupNevoTracing
} from "@riaskov/nevo-messaging"
import { AppModule } from "./app.module"

async function bootstrap() {
  // Logger first — everything that follows is logged.
  const logger = createLogger({
    name: "user",
    level: (process.env.LOG_LEVEL as any) ?? "info"
  })
  setDefaultLogger(logger)

  // OTel tracing — falls back gracefully if the exporter isn't installed.
  if (process.env.OTEL_EXPORTER === "otlp") {
    await setupNevoTracing({
      serviceName: "user",
      serviceVersion: "2.0.0",
      exporter: "otlp",
      endpoint: process.env.OTEL_EXPORTER_OTLP_ENDPOINT,
      sampleRate: 0.1
    })
  }

  const app = await createNatsMicroservice({
    microserviceName: "user",
    module: AppModule,
    port: 8087,
    host: "0.0.0.0"
  })

  // Wire graceful shutdown: drain in-flight handlers, then run hooks in order.
  const shutdown = new GracefulShutdown()

  // Health probe shows "down" once shutdown begins → orchestrator drains us first.
  const health = app.get(HealthRegistry)
  health.register(
    "not-draining",
    () => ({
      status: shutdown.isShuttingDown() ? "down" : "ok",
      message: shutdown.isShuttingDown() ? "draining" : "ready"
    }),
    { kind: "readiness" }
  )

  shutdown.register("close nest app", async () => {
    await app.close()
  })

  for (const sig of ["SIGTERM", "SIGINT"] as const) {
    process.on(sig, async () => {
      if (shutdown.isShuttingDown()) return
      logger.info({ sig }, "shutting down")
      await shutdown.shutdown(30_000)
      process.exit(0)
    })
  }
}

bootstrap().catch((error) => {
  console.error(error)
  process.exit(1)
})
