import { Controller, Inject, Post, Body, Get } from "@nestjs/common"
import { HealthRegistry, KafkaSignalRouter, Signal } from "@riaskov/nevo-messaging"
import { CoordinatorService } from "./coordinator.service"

@Controller()
@KafkaSignalRouter([CoordinatorService], {
  // Coordinator accepts work from frontend or any internal service.
  accessControl: {
    rules: [{ topic: "coordinator-events", method: "*", allow: ["frontend", "user", "wallet"] }],
    logDenied: true
  }
})
export class CoordinatorController {
  constructor(
    @Inject(CoordinatorService) private readonly coordinator: CoordinatorService,
    @Inject(HealthRegistry) private readonly health: HealthRegistry
  ) {}

  @Signal("process", "process", (data: any) => [data])
  process() {}

  // Bridge HTTP → Kafka query.
  @Post()
  async handleRequest(@Body() body: any) {
    return await this.coordinator.handleRequest(body)
  }

  // Cross-service saga endpoint.
  @Post("/place-order")
  async placeOrder(@Body() body: { userId: string; amount: number }) {
    return await this.coordinator.placeOrder(BigInt(body.userId), body.amount)
  }

  // Probes for Kubernetes.
  @Get("/healthz") liveness()  { return this.health.liveness() }
  @Get("/readyz")  readiness() { return this.health.readiness() }
}
