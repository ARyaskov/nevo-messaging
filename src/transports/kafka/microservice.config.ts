import { INestApplication } from "@nestjs/common"
import { NestFactory } from "@nestjs/core"
import { FastifyAdapter, NestFastifyApplication } from "@nestjs/platform-fastify"
import { MicroserviceOptions, Transport } from "@nestjs/microservices"
import { NestApplicationOptions } from "../microservice.options"

export async function createKafkaMicroservice(options: NestApplicationOptions): Promise<INestApplication> {
  const { microserviceName, module, port = 3000, host = "0.0.0.0", onInit } = options

  const app = await NestFactory.create<NestFastifyApplication>(module, new FastifyAdapter())

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: `${microserviceName}-microservice`,
        brokers: [process.env["KAFKA_HOST"] ? `${process.env["KAFKA_HOST"]}:${process.env["KAFKA_PORT"] || 9092}` : "127.0.0.1:9092"],
        connectionTimeout: 3000,
        requestTimeout: 15000,
        retry: {
          retries: 3
        }
      },
      consumer: {
        groupId: `${microserviceName}-events`,
        allowAutoTopicCreation: true,
        sessionTimeout: 15000,
        maxWaitTimeInMs: 1000,
        heartbeatInterval: 1000
      },
      subscribe: {
        fromBeginning: true
      },
      producer: {
        idempotent: true,
        maxInFlightRequests: 1
      }
    }
  })

  await app.startAllMicroservices()

  if (onInit) {
    await onInit(app)
  }

  await app.listen(port, host)
  console.log(`Service started on http://${host === "0.0.0.0" ? "localhost" : host}:${port}`)

  return app
}
