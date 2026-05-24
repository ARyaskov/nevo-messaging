import {
  GracefulShutdown,
  HealthRegistry,
  createKafkaMicroservice,
  createLogger,
  getDefaultMetrics,
  setDefaultLogger,
  setupNevoTracing
} from "@riaskov/nevo-messaging"
import { AppModule } from "./app.module"

async function bootstrap() {
  setDefaultLogger(createLogger({ name: "user", level: (process.env.LOG_LEVEL as any) ?? "info" }))

  if (process.env.OTEL_EXPORTER === "otlp") {
    await setupNevoTracing({
      serviceName: "user",
      serviceVersion: "2.0.0",
      exporter: "otlp",
      endpoint: process.env.OTEL_EXPORTER_OTLP_ENDPOINT,
      sampleRate: 0.1
    })
  }

  const app = await createKafkaMicroservice({
    microserviceName: "user",
    module: AppModule,
    port: 8086
  })

  // Optional: expose Prometheus metrics on a side port.
  const metricsPort = process.env.METRICS_PORT ? Number(process.env.METRICS_PORT) : 0
  if (metricsPort > 0) {
    const http = require("node:http")
    http
      .createServer(async (_req: any, res: any) => {
        res.setHeader("content-type", "text/plain; version=0.0.4")
        res.end(await getDefaultMetrics().expose())
      })
      .listen(metricsPort)
  }

  const shutdown = new GracefulShutdown()
  const health = app.get(HealthRegistry)
  health.register(
    "not-draining",
    () => ({ status: shutdown.isShuttingDown() ? "down" : "ok" }),
    { kind: "readiness" }
  )
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
