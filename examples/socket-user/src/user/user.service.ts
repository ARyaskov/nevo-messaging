import { Inject, Injectable } from "@nestjs/common"
import {
  NevoSocketClient,
  RateLimit,
  Schema,
  SocketClientBase
} from "@riaskov/nevo-messaging"
import { z } from "zod"

const GetByIdInput = z.object({ id: z.bigint() })
const DeleteInput = z.object({ id: z.bigint() })
const NotifyInput = z.object({ userId: z.bigint() })

@Injectable()
export class UserService extends SocketClientBase {
  constructor(@Inject("NEVO_SOCKET_CLIENT") socketClient: NevoSocketClient) {
    super(socketClient)
  }

  // Socket.IO clients are typically untrusted browsers — apply tighter limits than on internal services.
  @RateLimit({ capacity: 30, refillPerSec: 10, keyBy: ["callerService"] })
  @Schema(GetByIdInput)
  getById(id: bigint) {
    return { id, name: "Eddie", email: "mail@example.com" }
  }

  @RateLimit({ capacity: 5, refillPerSec: 1, keyBy: ["callerService"] })
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
