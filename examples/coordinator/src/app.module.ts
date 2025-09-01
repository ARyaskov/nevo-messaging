import { Module } from "@nestjs/common"
import { CoordinatorModule } from "./coordinator/coordinator.module"

@Module({
  imports: [CoordinatorModule]
})
export class AppModule {}
