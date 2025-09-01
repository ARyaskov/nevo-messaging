import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import { UserController } from "./user.controller"
import { UserService } from "./user.service"
import { AuthService } from "./auth/auth.service"
import { createNevoKafkaClient } from "@riaskov/nevo-messaging"

@Module({
  imports: [ConfigModule],
  controllers: [UserController],
  providers: [
    UserService,
    AuthService,
    createNevoKafkaClient(["COORDINATOR", "WALLET"], {
      clientIdPrefix: "user"
    })
  ]
})
export class UserModule {}
