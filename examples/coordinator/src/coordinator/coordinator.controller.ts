import { Controller, Inject, Post, Body } from "@nestjs/common"
import { KafkaSignalRouter, Signal } from "@riaskov/nevo-messaging"
import { CoordinatorService } from "./coordinator.service"

@Controller()
@KafkaSignalRouter([CoordinatorService])
export class CoordinatorController {
  constructor(@Inject(CoordinatorService) private readonly _coordinatorService: CoordinatorService) {}

  @Signal("process", (data: any) => [data])
  process() {}

  @Post()
  async handleRequest(@Body() body) {
    return await this._coordinatorService.handleRequest(body)
  }
}
