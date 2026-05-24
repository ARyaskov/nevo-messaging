import { NevoHttpClient } from "./nevo-http.client"
import type { ClientCallOptions } from "../nats/nats.client-base"

export abstract class HttpClientBase {
  protected readonly universalClient: NevoHttpClient

  protected constructor(universalClient: NevoHttpClient) {
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
    options: Parameters<NevoHttpClient["subscribe"]>[2],
    handler: Parameters<NevoHttpClient["subscribe"]>[3]
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
