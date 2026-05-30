import { Inject, Injectable } from "@nestjs/common"
import {
  NatsClientBase,
  NevoNatsClient,
  Schema,
  RateLimit,
  Cacheable,
  Hedge,
  CircuitBreaker,
  Adaptive,
  Backpressure
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

  // Read-mostly path:
  //   - LRU cache so identical hits are sub-millisecond
  //   - `@Hedge` fires a duplicate after 40ms to shave off the long tail
  //   - `@CircuitBreaker` opens if 50% of the last 20 calls fail
  //   - `@Adaptive` retunes timeout/retries against observed p99
  @Hedge({ copies: 1, delayMs: 40 })
  @CircuitBreaker({ mode: "sliding", windowMs: 10_000, errorRateThreshold: 0.5, minSampleSize: 20 })
  @Adaptive({ targetP99Ms: 250, minRetries: 1, maxRetries: 4 })
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

  // Subscribe-style projection — `@Backpressure` pauses the JetStream consumer
  // automatically when 160 messages are in flight, resumes below 80, and
  // nacks anything that overflows the hard cap.
  @Backpressure({ maxInflight: 200, highWatermark: 160, lowWatermark: 80, onOverflow: "nack" })
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
