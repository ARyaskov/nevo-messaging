import { Controller, Inject } from "@nestjs/common"
import { UserService } from "./user.service"
import { KafkaSignalRouter, Signal } from "@riaskov/nevo-messaging"
import { AuthService } from "./auth/auth.service"

@Controller()
@KafkaSignalRouter([UserService, AuthService], {
  // Multi-service controller — ACL applies to every routed signal.
  accessControl: {
    rules: [
      { topic: "user-events", method: "*", allow: ["frontend", "coordinator"] },
      { topic: "user-events", method: "auth.sendMagicLink", allow: ["frontend"] },
      { topic: "user-events", method: "user.delete", allow: ["coordinator"] }
    ],
    logDenied: true,
    allowAllByDefault: false
  }
})
export class UserController {
  constructor(
    // @ts-ignore
    @Inject(UserService) private readonly userService: UserService,
    // @ts-ignore
    @Inject(AuthService) private readonly authService: AuthService
  ) {}

  @Signal("auth.sendMagicLink", "sendMagicLink", (data: any) => [data.email, data.baseUrl])
  sendMagicLink() {}

  @Signal("user.getByEmail", "getByEmail", (data: any) => [data.email])
  getByEmail() {}

  // Versioned variant — old callers continue to hit "user.getById" (v1).
  @Signal("user.getById", "getById", (data: any) => [data.id])
  getById() {}

  @Signal("user.delete", "delete", (data: any) => [data.id])
  deleteUser() {}

  @Signal("user.list", "list", (data: any) => [data.page ?? 1, data.limit ?? 20, data.filters])
  listUsers() {}
}
