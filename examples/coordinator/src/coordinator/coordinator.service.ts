import { Injectable, Inject } from "@nestjs/common"
import {
  KafkaClientBase,
  NevoKafkaClient,
  Outbox,
  createSaga
} from "@riaskov/nevo-messaging"

interface PlaceOrderCtx {
  userId: bigint
  amount: number
  // Filled by steps:
  reservationId?: string
  chargeId?: string
}

@Injectable()
export class CoordinatorService extends KafkaClientBase {
  constructor(
    @Inject("NEVO_KAFKA_CLIENT") universalClient: NevoKafkaClient,
    @Inject(Outbox) private readonly outbox: Outbox
  ) {
    super(universalClient)
  }

  // Cross-service request routing — unchanged: a generic switch over body.type
  // dispatches to the right upstream service by query.
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

  // Multi-step transaction with compensation — saga orchestration over Nevo.
  // If any step fails, earlier successful steps are compensated in reverse order.
  async placeOrder(userId: bigint, amount: number) {
    const saga = createSaga<PlaceOrderCtx>()
      .step({
        name: "reserveWallet",
        execute: async (ctx) => {
          const r = await this.query<{ reservationId: string }>(
            "wallet", "wallet.reserve", { userId: ctx.userId, amount: ctx.amount }
          )
          ctx.reservationId = r.reservationId
        },
        compensate: async (ctx) => {
          if (ctx.reservationId) {
            await this.emit("wallet", "wallet.release", { reservationId: ctx.reservationId })
          }
        },
        retries: 2,
        backoff: { baseMs: 200, maxMs: 1_000, jitter: true },
        timeoutMs: 5_000
      })
      .step({
        name: "chargeUser",
        execute: async (ctx) => {
          const r = await this.query<{ chargeId: string }>(
            "user", "user.charge", { userId: ctx.userId, amount: ctx.amount }
          )
          ctx.chargeId = r.chargeId
        },
        compensate: async (ctx) => {
          if (ctx.chargeId) {
            await this.emit("user", "user.refund", { chargeId: ctx.chargeId })
          }
        }
      })

    const result = await saga.run({ userId, amount })

    // Audit trail via the outbox — writes synchronously, publishes in background.
    await this.outbox.enqueue("coordinator", "order.attempted", {
      userId,
      amount,
      status: result.status,
      executed: result.executed
    })

    return result
  }

  process(_data: any) {
    console.log("[coordinator] process()")
  }
}
