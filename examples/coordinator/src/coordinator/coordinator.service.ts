import { Injectable, Inject, Post, Body, Headers } from "@nestjs/common"
import { KafkaClientBase, NevoKafkaClient } from "@riaskov/nevo-messaging"

@Injectable()
export class CoordinatorService extends KafkaClientBase {
  constructor(@Inject("NEVO_KAFKA_CLIENT") universalClient: NevoKafkaClient) {
    super(universalClient)
  }

  async handleRequest(body: any) {
    switch (body.type) {
      case "user":
        return await this.query("user", body.method, body.params)
      case "wallet":
        return await this.query("wallet", body.method, body.params)
      default:
        throw new Error(`Unknown service type: ${body.type}`)
    }
  }

  process(_data) {
    console.log("Some processing is here")
  }
}
