import { Injectable, Inject } from "@nestjs/common"
import { KafkaClientBase, NevoKafkaClient } from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends KafkaClientBase {
  constructor(@Inject("NEVO_KAFKA_CLIENT") nevoClient: NevoKafkaClient) {
    super(nevoClient)
  }

  getById(id: bigint) {
    return { id, name: "Eddie", email: "mail@example.com" }
  }

  getByEmail(email: string) {
    return { id: 42n, name: "Eddie", email }
  }

  async delete(id: bigint) {
    await this.emit("coordinator", "user.deleted", { userId: id })

    return { success: true, deletedId: id }
  }

  async list(page: number, limit: number, filters: any) {
    return {
      users: [
        { id: 1n, name: "Eddie", email: "eddie@example.com" },
        { id: 2n, name: "Alice", email: "alice@example.com" }
      ],
      total: 2,
      page,
      limit
    }
  }
}
