import { Controller, Inject } from "@nestjs/common"
import { NatsSignalRouter, Signal } from "@riaskov/nevo-messaging"
import { UserService } from "./user.service"

@Controller()
@NatsSignalRouter([UserService], {
  accessControl: {
    rules: [{ topic: "user-events", method: "*", allow: ["frontend", "coordinator"] }],
    logDenied: true
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
