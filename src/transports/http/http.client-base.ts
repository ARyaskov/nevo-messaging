import { NevoHttpClient } from "./nevo-http.client"

export abstract class HttpClientBase {
  protected readonly universalClient: NevoHttpClient

  protected constructor(universalClient: NevoHttpClient) {
    this.universalClient = universalClient
  }

  protected async query<T = any>(serviceName: string, method: string, params: any): Promise<T> {
    return this.universalClient.query(serviceName, method, params)
  }

  protected async emit(serviceName: string, method: string, params: any): Promise<void> {
    return this.universalClient.emit(serviceName, method, params)
  }

  protected async publish(serviceName: string, method: string, params: any): Promise<void> {
    return this.universalClient.publish(serviceName, method, params)
  }

  protected async broadcast(method: string, params: any): Promise<void> {
    return this.universalClient.broadcast(method, params)
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
