import { Injectable, Inject } from "@nestjs/common"
import { KafkaClientBase, NevoKafkaClient } from "@riaskov/nevo-messaging"

@Injectable()
export class AuthService extends KafkaClientBase {
  constructor(@Inject("NEVO_KAFKA_CLIENT") nevoClient: NevoKafkaClient) {
    super(nevoClient)
  }

  async sendMagicLink(email: string, baseUrl?: string) {
    console.log(`Sending magic link email to ${email}`)

    await this.emit("coordinator", "auth.magicLinkSent", { email, timestamp: new Date() })

    return { success: true, email }
  }
}
