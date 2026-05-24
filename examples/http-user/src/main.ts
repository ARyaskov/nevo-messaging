import {
  GracefulShutdown,
  HealthRegistry,
  createHttpMicroservice,
  createLogger,
  setDefaultLogger
} from "@riaskov/nevo-messaging"
import { AppModule } from "./app.module"

async function bootstrap() {
  setDefaultLogger(createLogger({ name: "user", level: (process.env.LOG_LEVEL as any) ?? "info" }))

  const app = await createHttpMicroservice({
    microserviceName: "user",
    module: AppModule,
    port: 8090,
    host: "0.0.0.0"
  })

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
