import { Controller, Inject } from "@nestjs/common"
import { UserService } from "./user.service"
import { KafkaSignalRouter, Signal } from "@riaskov/nevo-messaging"
import { AuthService } from "./auth/auth.service"

@Controller()
@KafkaSignalRouter([UserService, AuthService])
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
  actuallyYouCanNameItAnyhow_itExistsJustToSatisfyTSCompiler() {}

  @Signal("user.delete", "delete", (data: any) => [data.id])
  deleteUser() {}

  @Signal("user.list", "list", (data: any) => [data.page, data.limit, data.filters])
  listUsers() {}
}
