import { INestApplication, Type } from "@nestjs/common"

export interface NestApplicationOptions {
  microserviceName: string
  module: Type<any>
  port?: number
  host?: string
  debug?: boolean
  onInit?: (app: INestApplication) => Promise<void>
}
