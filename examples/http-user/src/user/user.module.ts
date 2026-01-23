import { Module } from "@nestjs/common"
import { createNevoHttpClient, HttpTransportController } from "@riaskov/nevo-messaging"
import { UserController } from "./user.controller"
import { UserService } from "./user.service"

@Module({
  controllers: [UserController, HttpTransportController],
  providers: [
    UserService,
    createNevoHttpClient(
      {
        coordinator: "http://127.0.0.1:8091"
      },
      {
        clientIdPrefix: "user"
      }
    )
  ]
})
export class UserModule {}
