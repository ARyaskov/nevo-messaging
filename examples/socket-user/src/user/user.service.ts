import { Inject, Injectable } from "@nestjs/common"
import { NevoSocketClient, SocketClientBase } from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends SocketClientBase {
  constructor(@Inject("NEVO_SOCKET_CLIENT") socketClient: NevoSocketClient) {
    super(socketClient)
  }

  getById(id: bigint) {
    return { id, name: "Eddie", email: "mail@example.com" }
  }

  async delete(id: bigint) {
    await this.emit("coordinator", "user.deleted", { userId: id })
    return { success: true, deletedId: id }
  }

  async notifyUpdate(userId: bigint) {
    await this.publish("user", "user.updated", { userId })
    return { ok: true }
  }

  async broadcastStatus() {
    await this.broadcast("system.status", { service: "user", ok: true })
    return { ok: true }
  }
}
