import { Injectable, Inject } from "@nestjs/common"
import {
  Cacheable,
  KafkaClientBase,
  NevoKafkaClient,
  RateLimit,
  Schema
} from "@riaskov/nevo-messaging"
import { z } from "zod"

const GetByIdInput = z.object({ id: z.bigint() })
const GetByEmailInput = z.object({ email: z.string().email() })
const DeleteInput = z.object({ id: z.bigint() })
const ListInput = z.object({
  page: z.number().int().min(1).default(1),
  limit: z.number().int().min(1).max(100).default(20),
  filters: z.record(z.unknown()).optional()
})

@Injectable()
export class UserService extends KafkaClientBase {
  constructor(@Inject("NEVO_KAFKA_CLIENT") nevoClient: NevoKafkaClient) {
    super(nevoClient)
  }

  // Hot read — cache for 30 s per id.
  @Cacheable({ ttlMs: 30_000, maxEntries: 4096, keyBy: (p: any) => `u:${p?.id ?? "_"}` })
  @Schema(GetByIdInput)
  getById(id: bigint) {
    return { id, name: "Eddie", email: "mail@example.com" }
  }

  @Schema(GetByEmailInput)
  getByEmail(email: string) {
    return { id: 42n, name: "Eddie", email }
  }

  // Mutation — bucket per caller service.
  @RateLimit({ capacity: 10, refillPerSec: 2, keyBy: ["callerService"] })
  @Schema(DeleteInput)
  async delete(id: bigint) {
    await this.emit("coordinator", "user.deleted", { userId: id })
    return { success: true, deletedId: id }
  }

  @Schema(ListInput)
  async list(page: number, limit: number, filters: any) {
    return {
      users: [
        { id: 1n, name: "Eddie", email: "eddie@example.com" },
        { id: 2n, name: "Alice", email: "alice@example.com" }
      ],
      total: 2,
      page,
      limit,
      filters: filters ?? {}
    }
  }
}
