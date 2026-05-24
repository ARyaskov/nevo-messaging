import { Controller, Inject } from "@nestjs/common"
import { NatsSignalRouter, Signal } from "@riaskov/nevo-messaging"
import { UserService } from "./user.service"

@Controller()
@NatsSignalRouter([UserService], {
  // Plain caller-based ACL.
  accessControl: {
    rules: [
      { topic: "user-events", method: "*", allow: ["frontend", "coordinator"] },
      // Stricter rule on the destructive call.
      { topic: "user-events", method: "user.delete", allow: ["coordinator"] }
    ],
    logDenied: true,
    allowAllByDefault: false
  },
  // Audit hook — log every dispatch with redaction handled upstream.
  before: async (ctx) => {
    if (process.env.NEVO_DEBUG_HOOKS === "1") {
      const meta = (ctx.rawData as any)?.meta
      console.log(`[router] ${ctx.method} from ${meta?.callerService ?? "?"}`)
    }
    return ctx.params
  }
})
export class UserController {
  constructor(@Inject(UserService) private readonly userService: UserService) {}

  @Signal("user.getById", "getById", (data: any) => [data.id])
  getUserById() {}

  @Signal("user.delete", "delete", (data: any) => [data.id])
  deleteUser() {}

  @Signal("user.updated.notify", "notifyUpdate", (data: any) => [data.userId])
  notifyUpdate() {}

  @Signal("system.status", "broadcastStatus")
  broadcastStatus() {}
}
