import { Controller, Get, Inject } from "@nestjs/common"
import { HealthRegistry, HttpSignalRouter, Signal, contractToOpenApi } from "@riaskov/nevo-messaging"
import { UserService } from "./user.service"

@Controller()
@HttpSignalRouter([UserService], {
  accessControl: {
    rules: [
      { topic: "user-events", method: "*",           allow: ["frontend", "coordinator"] },
      { topic: "user-events", method: "user.delete", allow: ["coordinator"] }
    ],
    logDenied: true,
    allowAllByDefault: false
  }
})
export class UserController {
  constructor(
    @Inject(UserService) private readonly userService: UserService,
    @Inject(HealthRegistry) private readonly health: HealthRegistry
  ) {}

  @Signal("user.getById", "getById", (data: any) => [data.id])
  getUserById() {}

  @Signal("user.delete", "delete", (data: any) => [data.id])
  deleteUser() {}

  @Signal("user.updated.notify", "notifyUpdate", (data: any) => [data.userId])
  notifyUpdate() {}

  @Signal("system.status", "broadcastStatus")
  broadcastStatus() {}

  // Standard probe paths for Kubernetes.
  @Get("/healthz")
  liveness() {
    return this.health.liveness()
  }

  @Get("/readyz")
  readiness() {
    return this.health.readiness()
  }

  // Live OpenAPI rendered from the reflected contract.
  // The contract is built from @Signal + @Schema declarations on this controller.
  @Get("/openapi.json")
  async openapi() {
    const contract = await this.userService.contract()
    return contractToOpenApi(contract, {
      title: "User Service",
      version: "2.0.0",
      baseUrl: "http://127.0.0.1:8090"
    })
  }
}
