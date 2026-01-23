import { Module } from "@nestjs/common"
import { createNevoSocketClient } from "@riaskov/nevo-messaging"
import { UserController } from "./user.controller"
import { UserService } from "./user.service"

@Module({
  controllers: [UserController],
  providers: [
    UserService,
    createNevoSocketClient(
      {
        coordinator: "http://127.0.0.1:8094"
      },
      {
        clientIdPrefix: "user"
      }
    )
  ]
})
export class UserModule {}
