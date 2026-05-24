import { NevoWsClient } from "./nevo-ws.client"
import type { ClientCallOptions } from "../nats/nats.client-base"

export abstract class WsClientBase {
  protected readonly universalClient: NevoWsClient

  protected constructor(universalClient: NevoWsClient) {
    this.universalClient = universalClient
  }

  protected async query<T = any>(serviceName: string, method: string, params: any, opts?: ClientCallOptions): Promise<T> {
    return this.universalClient.query<T>(serviceName, method, params, opts)
  }

  protected async emit(serviceName: string, method: string, params: any, opts?: ClientCallOptions): Promise<void> {
    return this.universalClient.emit(serviceName, method, params, opts)
  }

  protected async publish(serviceName: string, method: string, params: any, opts?: ClientCallOptions): Promise<void> {
    return this.universalClient.publish(serviceName, method, params, opts)
  }

  protected async broadcast(method: string, params: any, opts?: ClientCallOptions): Promise<void> {
    return this.universalClient.broadcast(method, params, opts)
  }

  protected async subscribe<T = any>(
    serviceName: string,
    method: string,
    options: Parameters<NevoWsClient["subscribe"]>[2],
    handler: Parameters<NevoWsClient["subscribe"]>[3]
  ) {
    return this.universalClient.subscribe<T>(serviceName, method, options, handler)
  }

  protected getAvailableServices(): string[] {
    return this.universalClient.getAvailableServices()
  }
}
