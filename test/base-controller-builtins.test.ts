import { test } from "node:test"
import assert from "node:assert/strict"
import { BaseMessageController } from "../src/common/base.controller"
import { NEVO_CONTRACT_METHOD } from "../src/common/contract"
import { ErrorCode } from "../src/common/error-code"
import { RateLimiter } from "../src/common/rate-limit"
import type { AccessControlConfig, MessageResponse, ServiceMethodMapping } from "../src/common/types"

class BuiltinController extends BaseMessageController {
  constructor(
    opts: {
      accessControl?: AccessControlConfig
      disableBuiltinHandlers?: boolean
      rateLimit?: RateLimiter
    } = {}
  ) {
    const handlers: ServiceMethodMapping = {
      echo: { serviceMethod: "echo" }
    }
    super("svc", [{ echo: (value: unknown) => value }], handlers, {
      tracing: { enabled: false },
      devtools: false,
      ...opts
    })
  }
  protected extractMessageData(data: any) {
    return data
  }
  handleMessage(data: any): Promise<MessageResponse> {
    return this.processMessage(data)
  }
}

test("built-in contract method is evaluated after ACL", async () => {
  const controller = new BuiltinController({
    accessControl: {
      allowAllByDefault: false,
      rules: [{ method: NEVO_CONTRACT_METHOD, deny: ["caller"] }]
    }
  })
  const response = await controller.handleMessage({
    method: NEVO_CONTRACT_METHOD,
    uuid: "u1",
    params: {},
    meta: { service: "caller" }
  })
  assert.equal((response.params as any).error.code, ErrorCode.UNAUTHORIZED)
})

test("disableBuiltinHandlers makes the contract method unavailable", async () => {
  const controller = new BuiltinController({ disableBuiltinHandlers: true })
  const response = await controller.handleMessage({
    method: NEVO_CONTRACT_METHOD,
    uuid: "u2",
    params: {}
  })
  assert.equal((response.params as any).error.code, ErrorCode.METHOD_NOT_FOUND)
})

test("BaseMessageController closes an internally owned rate limiter", async () => {
  const controller = new BuiltinController()
  let stopped = 0
  ;(controller as any).rateLimiter.stop = () => {
    stopped++
  }
  await controller.close()
  assert.equal(stopped, 1)
})

test("BaseMessageController does not stop a caller-owned rate limiter", async () => {
  const limiter = new RateLimiter({ enabled: true })
  let stopped = 0
  limiter.stop = () => {
    stopped++
  }
  const controller = new BuiltinController({ rateLimit: limiter })
  await controller.close()
  assert.equal(stopped, 0)
})
