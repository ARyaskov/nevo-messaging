import { NestFactory } from "@nestjs/core"
import { FastifyAdapter, NestFastifyApplication } from "@nestjs/platform-fastify"
import { AppModule } from "./app.module"

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(AppModule, new FastifyAdapter())
  await app.listen(8087, "0.0.0.0")
  console.log("NATS user service listening on http://localhost:8087")
}

bootstrap().catch((error) => {
  console.error(error)
})
