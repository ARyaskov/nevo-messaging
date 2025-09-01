import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import { createNevoKafkaClient } from "@riaskov/nevo-messaging"
import { CoordinatorController } from "./coordinator.controller"
import { CoordinatorService } from "./coordinator.service"

@Module({
  imports: [ConfigModule],
  controllers: [CoordinatorController],
  providers: [
    CoordinatorService,
    createNevoKafkaClient(["USER", "WALLET"], {
      clientIdPrefix: "coordinator"
    })
  ]
})
export class CoordinatorModule {}
