import { Inject, Injectable } from "@nestjs/common"
import {
  Cacheable,
  HttpClientBase,
  NevoHttpClient,
  RateLimit,
  Schema,
  type ServiceContract
} from "@riaskov/nevo-messaging"
import { z } from "zod"

const GetByIdInput = z.object({ id: z.bigint() })
const DeleteInput = z.object({ id: z.bigint() })
const NotifyInput = z.object({ userId: z.bigint() })

@Injectable()
export class UserService extends HttpClientBase {
  constructor(@Inject("NEVO_HTTP_CLIENT") httpClient: NevoHttpClient) {
    super(httpClient)
  }

  @Cacheable({ ttlMs: 30_000, maxEntries: 1024, keyBy: (p: any) => `u:${p?.id}` })
  @Schema(GetByIdInput)
  getById(id: bigint) {
    return { id, name: "Eddie", email: "mail@example.com" }
  }

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

  // Public wrapper that asks the running service for its own contract.
  // The signal "nevo.contract" is auto-registered by the framework.
  async contract(): Promise<ServiceContract> {
    return this.query<ServiceContract>("user", "nevo.contract", {})
  }
}
