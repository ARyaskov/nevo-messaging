import { INestApplication } from "@nestjs/common"
import { NestFactory } from "@nestjs/core"
import { FastifyAdapter, NestFastifyApplication } from "@nestjs/platform-fastify"
import { NestApplicationOptions } from "../microservice.options"

export async function createHttpMicroservice(options: NestApplicationOptions): Promise<INestApplication> {
  // @ts-ignore
  const { microserviceName, module, port = 3000, host = "0.0.0.0", debug = process.env["NODE_ENV"] !== "production", onInit } = options

  const app = await NestFactory.create<NestFastifyApplication>(module, new FastifyAdapter())

  if (onInit) {
    await onInit(app)
  }

  await app.listen(port, host)
  console.log(`Service started on http://${host === "0.0.0.0" ? "localhost" : host}:${port}`)

  return app
}
