import { randomUUID } from "node:crypto"
import { ErrorCode } from "../common"
import { MessagingError } from "./errors"
import { MessagePayload, MessageRequest, MicroserviceConfig, TransportClientOptions } from "./types"

export abstract class BaseMessagingClient {
  protected readonly options: TransportClientOptions
  protected readonly microservices: Map<string, string> = new Map()

  protected constructor(options?: TransportClientOptions) {
    this.options = {
      timeout: 20000,
      debug: false,
      ...options
    }
  }

  protected registerMicroservices(configs: MicroserviceConfig[]): void {
    for (const config of configs) {
      this.microservices.set(config.serviceName, config.clientName)
    }
  }

  protected async query<T = any>(serviceName: string, method: string, params: any): Promise<T> {
    const clientName = this.microservices.get(serviceName)
    if (!clientName) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Microservice ${serviceName} is not registered`,
        serviceName
      })
    }
    return this._queryMicroservice(clientName, method, params)
  }

  protected async emit(serviceName: string, method: string, params: any): Promise<void> {
    const clientName = this.microservices.get(serviceName)
    if (!clientName) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Microservice ${serviceName} is not registered`,
        serviceName
      })
    }
    return this._emitToMicroservice(clientName, method, params)
  }

  protected createMessagePayload(method: string, params: any): MessagePayload {
    const uuid = randomUUID()
    const request: MessageRequest = { uuid, method, params }

    return {
      key: uuid,
      value: JSON.stringify(request)
    }
  }

  protected abstract _queryMicroservice<T>(clientName: string, method: string, params: any): Promise<T>
  protected abstract _emitToMicroservice(clientName: string, method: string, params: any): Promise<void>
}
