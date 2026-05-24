import { Module } from "@nestjs/common"
import {
  HealthRegistry,
  createNevoSocketClient,
  eventLoopLagPing,
  memoryUsagePing
} from "@riaskov/nevo-messaging"
import { UserController } from "./user.controller"
import { UserService } from "./user.service"

@Module({
  controllers: [UserController],
  providers: [
    UserService,
    {
      provide: HealthRegistry,
      useFactory: () => {
        const reg = new HealthRegistry({ serviceName: "user", version: "2.0.0" })
        reg.register("eventLoop", eventLoopLagPing(100), { kind: "liveness" })
        reg.register("memory", memoryUsagePing(1024), { kind: "liveness" })
        return reg
      }
    },
    createNevoSocketClient(
      { coordinator: process.env.SOCKET_COORDINATOR ?? "http://127.0.0.1:8094" },
      { clientIdPrefix: "user" }
    )
  ]
})
export class UserModule {}
