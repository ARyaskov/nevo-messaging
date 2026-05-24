import { Inject, Injectable } from "@nestjs/common"
import {
  NatsClientBase,
  NevoNatsClient,
  Schema,
  RateLimit,
  Cacheable
} from "@riaskov/nevo-messaging"
import { z } from "zod"

const GetByIdInput = z.object({ id: z.bigint() })
const DeleteInput = z.object({ id: z.bigint() })
const NotifyInput = z.object({ userId: z.bigint() })

@Injectable()
export class UserService extends NatsClientBase {
  constructor(@Inject("NEVO_NATS_CLIENT") natsClient: NevoNatsClient) {
    super(natsClient)
  }

  // Read-mostly path — cache for a minute, partition by id.
  @Cacheable({ ttlMs: 60_000, maxEntries: 1024, keyBy: (params: any) => `u:${params?.id ?? "_"}` })
  @Schema(GetByIdInput)
  getById(id: bigint) {
    return { id, name: "Eddie", email: "mail@example.com" }
  }

  // Mutation — token-bucket limit per caller.
  @RateLimit({ capacity: 20, refillPerSec: 5, keyBy: ["callerService"] })
  @Schema(DeleteInput)
  async delete(id: bigint) {
    await this.emit("coordinator", "user.deleted", { userId: id })
    return { success: true, deletedId: id }
  }

  @Schema(NotifyInput)
  async notifyUpdate(userId: bigint) {
    await this.publish("user", "user.updated", { userId })
    return { ok: true }
  }

  async broadcastStatus() {
    await this.broadcast("system.status", { service: "user", ok: true })
    return { ok: true }
  }
}
