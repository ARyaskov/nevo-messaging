import { Inject, Injectable } from "@nestjs/common"
import { HttpClientBase, NevoHttpClient } from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends HttpClientBase {
  constructor(@Inject("NEVO_HTTP_CLIENT") httpClient: NevoHttpClient) {
    super(httpClient)
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
