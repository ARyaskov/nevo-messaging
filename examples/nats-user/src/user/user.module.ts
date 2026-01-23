import { Module } from "@nestjs/common"
import { createNevoNatsClient } from "@riaskov/nevo-messaging"
import { UserController } from "./user.controller"
import { UserService } from "./user.service"

@Module({
  controllers: [UserController],
  providers: [
    UserService,
    createNevoNatsClient(["COORDINATOR", "USER"], {
      clientIdPrefix: "user",
      servers: ["nats://127.0.0.1:4222"]
    })
  ]
})
export class UserModule {}
