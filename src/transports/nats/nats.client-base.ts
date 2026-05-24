import { NevoNatsClient } from "./nevo-nats.client"

export interface ClientCallOptions {
  version?: string
  idempotencyKey?: string
  headers?: Record<string, string>
  tenantId?: string
  timeoutMs?: number
}

export abstract class NatsClientBase {
  protected readonly universalClient: NevoNatsClient

  protected constructor(universalClient: NevoNatsClient) {
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
    options: Parameters<NevoNatsClient["subscribe"]>[2],
    handler: Parameters<NevoNatsClient["subscribe"]>[3]
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
