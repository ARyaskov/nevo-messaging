import { Controller, Inject } from "@nestjs/common"
import { Signal, SocketSignalRouter, createJwksVerifier } from "@riaskov/nevo-messaging"
import { UserService } from "./user.service"

// Optional JWT verification — only enabled if AUTH_JWKS_URI is set.
const jwtVerifier = process.env.AUTH_JWKS_URI
  ? createJwksVerifier({
      jwksUri: process.env.AUTH_JWKS_URI,
      issuer: process.env.AUTH_ISSUER,
      audience: process.env.AUTH_AUDIENCE,
      cacheTtlMs: 600_000,
      clockSkewSec: 30
    })
  : undefined

@Controller()
@SocketSignalRouter([UserService], {
  serviceName: "user",
  port: 8093,
  cors: { origin: process.env.CORS_ORIGIN ?? "*" },
  discovery: { enabled: true, heartbeatIntervalMs: 5_000 },

  accessControl: {
    rules: [
      { topic: "user-events", method: "*",           allow: ["frontend"] },
      { topic: "user-events", method: "user.delete", allow: ["coordinator"] }
    ],
    logDenied: true,
    allowAllByDefault: false,
    jwtVerifier
  },

  // Hook every dispatch — useful as an audit trail for browser-facing services.
  before: async (ctx) => {
    if (process.env.NEVO_DEBUG_HOOKS === "1") {
      const actor = (ctx.rawData as any)?.meta?.callerService ?? "anon"
      console.log(`[socket] ${ctx.method} from ${actor}`)
    }
    return ctx.params
  }
})
export class UserController {
  constructor(@Inject(UserService) private readonly userService: UserService) {}

  @Signal("user.getById", "getById", (data: any) => [data.id])
  getUserById() {}

  @Signal("user.delete", "delete", (data: any) => [data.id])
  deleteUser() {}

  @Signal("user.updated.notify", "notifyUpdate", (data: any) => [data.userId])
  notifyUpdate() {}

  @Signal("system.status", "broadcastStatus")
  broadcastStatus() {}
}
