import { NevoSocketClient } from "./nevo-socket.client"
import type { ClientCallOptions } from "../nats/nats.client-base"

export abstract class SocketClientBase {
  protected readonly universalClient: NevoSocketClient

  protected constructor(universalClient: NevoSocketClient) {
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
    options: Parameters<NevoSocketClient["subscribe"]>[2],
    handler: Parameters<NevoSocketClient["subscribe"]>[3]
  ) {
    return this.universalClient.subscribe<T>(serviceName, method, options, handler)
  }

  protected getAvailableServices(): string[] {
    return this.universalClient.getAvailableServices()
  }

  protected getDiscoveredServices() {
    return this.universalClient.getDiscoveredServices()
  }

  protected isServiceAvailable(serviceName: string): boolean {
    return this.universalClient.isServiceAvailable(serviceName)
  }
}
