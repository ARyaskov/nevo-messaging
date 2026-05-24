import { Injectable, Inject } from "@nestjs/common"
import { KafkaClientBase, NevoKafkaClient, RateLimit, Schema } from "@riaskov/nevo-messaging"
import { z } from "zod"

const SendMagicLinkInput = z.object({
  email: z.string().email(),
  baseUrl: z.string().url().optional()
})

@Injectable()
export class AuthService extends KafkaClientBase {
  constructor(@Inject("NEVO_KAFKA_CLIENT") nevoClient: NevoKafkaClient) {
    super(nevoClient)
  }

  // Magic links should be rate-limited per caller to slow down enumeration.
  // For per-email limits, add a custom RateLimiter with a keyExtractor at the client level.
  @RateLimit({
    capacity: 3,
    refillPerSec: 0.1,
    keyBy: ["callerService", "method"]
  })
  @Schema(SendMagicLinkInput)
  async sendMagicLink(email: string, baseUrl?: string) {
    console.log(`[auth] sending magic link to ${email} (baseUrl=${baseUrl ?? "default"})`)

    await this.emit("coordinator", "auth.magicLinkSent", {
      email,
      timestamp: new Date().toISOString()
    })

    return { success: true, email }
  }
}
