import { NevoKafkaClient } from "./nevo-kafka.client"

export abstract class KafkaClientBase {
  protected readonly universalClient: NevoKafkaClient

  protected constructor(universalClient: NevoKafkaClient) {
    this.universalClient = universalClient
  }

  protected async query<T = any>(serviceName: string, method: string, params: any): Promise<T> {
    return this.universalClient.query(serviceName, method, params)
  }

  protected async emit(serviceName: string, method: string, params: any): Promise<void> {
    return this.universalClient.emit(serviceName, method, params)
  }

  protected getAvailableServices(): string[] {
    return this.universalClient.getAvailableServices()
  }
}
