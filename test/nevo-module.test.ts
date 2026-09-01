import "reflect-metadata"
import { test } from "node:test"
import assert from "node:assert/strict"
import { Controller, Injectable, Module } from "@nestjs/common"
import { NestFactory } from "@nestjs/core"
import { NevoModule } from "../src/nevo.module"
import { HttpSignalRouter } from "../src/transports/http/http.signal-router.decorator"
import { addSignalMetadata } from "../src/signal.decorator"
import { resetNevoRouters, tryGetRouterRuntime } from "../src/router-runtime"
import { RateLimiter } from "../src/common/rate-limit"
import type { MessageResponse } from "../src/common/types"

const SILENT: any = {
  trace() {},
  debug() {},
  info() {},
  warn() {},
  error() {},
  fatal() {},
  child() {
    return SILENT
  },
  isLevelEnabled() {
    return false
  }
}

const QUIET = { logger: SILENT, devtools: false as const, tracing: { enabled: false } }

@Injectable()
class EchoService {
  calls = 0
  echo(params: any) {
    this.calls++
    return { seen: params }
  }
}

// The service is a plain property rather than a constructor dependency: the test
// runner transpiles with esbuild, which emits neither `design:paramtypes` nor
// legacy parameter decorators, so Nest could not inject it here.
function makeController(signal = "echo.run") {
  @Controller()
  class EchoController {
    svc = new EchoService()
  }
  addSignalMetadata(EchoController, signal, "echo")
  HttpSignalRouter(EchoService, { serviceName: "echo" })(EchoController)
  return EchoController
}

async function bootstrap(moduleClass: any) {
  const app = await NestFactory.createApplicationContext(moduleClass, { logger: false, abortOnError: false })
  await app.init()
  return app
}

const call = (ctrl: any, uuid: string, params: unknown = {}, meta: Record<string, unknown> = {}) =>
  ctrl.handleSignalMessage({ method: "echo.run", uuid, params, meta }) as Promise<MessageResponse>

test("NevoModule.forRoot wires a decorated controller through discovery", async () => {
  await resetNevoRouters()
  const EchoController = makeController()

  @Module({ imports: [NevoModule.forRoot(QUIET)], controllers: [EchoController], providers: [EchoService] })
  class AppModule {}

  const app = await bootstrap(AppModule)
  try {
    const ctrl = app.get(EchoController) as any
    assert.ok(tryGetRouterRuntime(EchoController), "the controller must be bound at onModuleInit")

    const res = await call(ctrl, "u1", { a: 1 })
    assert.deepEqual(res.params.result, { seen: { a: 1 } })
    assert.equal(ctrl.svc.calls, 1)
  } finally {
    await app.close()
  }
})

test("runtime options come from injected providers via forRootAsync", async () => {
  await resetNevoRouters()
  const EchoController = makeController()

  @Injectable()
  class Settings {
    readonly serviceName = "from-config"
    readonly limiter = new RateLimiter({ enabled: true, capacity: 1, refillPerSec: 0 })
  }

  @Module({
    providers: [Settings],
    exports: [Settings]
  })
  class SettingsModule {}

  @Module({
    imports: [
      SettingsModule,
      NevoModule.forRootAsync({
        imports: [SettingsModule],
        inject: [Settings],
        useFactory: (settings: Settings) => ({ ...QUIET, serviceName: settings.serviceName, rateLimit: settings.limiter })
      })
    ],
    controllers: [EchoController],
    providers: [EchoService]
  })
  class AppModule {}

  const app = await bootstrap(AppModule)
  try {
    const ctrl = app.get(EchoController)

    // The injected limiter has a single token and no refill.
    const first = await call(ctrl, "u1")
    assert.notEqual(first.params.result, "error")
    const second = await call(ctrl, "u2")
    assert.equal(second.params.result, "error", "the DI-provided rate limiter must be the one in force")
    assert.match(String(second.params.error?.message), /rate limit/i)
  } finally {
    await app.close()
  }
})

test("forFeature refines the root options for one module's controllers", async () => {
  await resetNevoRouters()
  const EchoController = makeController()

  @Module({
    imports: [NevoModule.forRoot({ ...QUIET, serviceName: "root-name", defaultVersion: "v1" })],
    controllers: [EchoController],
    providers: [EchoService, NevoModule.forFeature({ rateLimit: new RateLimiter({ enabled: true, capacity: 1, refillPerSec: 0 }) })]
  })
  class AppModule {}

  const app = await bootstrap(AppModule)
  try {
    const ctrl = app.get(EchoController)
    const first = await call(ctrl, "u1")
    assert.notEqual(first.params.result, "error")
    const second = await call(ctrl, "u2")
    assert.equal(second.params.result, "error", "the feature-level override must win over the root default")
  } finally {
    await app.close()
  }
})

test("a router used without NevoModule fails closed with an actionable error", async () => {
  await resetNevoRouters()
  const EchoController = makeController()

  @Module({ controllers: [EchoController], providers: [EchoService] })
  class AppModule {}

  const app = await bootstrap(AppModule)
  try {
    const ctrl = app.get(EchoController)
    await assert.rejects(() => call(ctrl, "u1"), /not initialised.*NevoModule\.forRoot/s)
  } finally {
    await app.close()
  }
})

test("controllers without router metadata are left alone", async () => {
  await resetNevoRouters()

  @Controller()
  class PlainController {
    ping() {
      return "pong"
    }
  }

  @Module({ imports: [NevoModule.forRoot(QUIET)], controllers: [PlainController] })
  class AppModule {}

  const app = await bootstrap(AppModule)
  try {
    assert.equal(tryGetRouterRuntime(PlainController), undefined)
    assert.equal(app.get(PlainController).ping(), "pong")
  } finally {
    await app.close()
  }
})

test("shutdown releases the router runtime", async () => {
  await resetNevoRouters()
  const EchoController = makeController()

  @Module({ imports: [NevoModule.forRoot(QUIET)], controllers: [EchoController], providers: [EchoService] })
  class AppModule {}

  const app = await bootstrap(AppModule)
  app.enableShutdownHooks()
  assert.ok(tryGetRouterRuntime(EchoController))
  await app.close()
  assert.equal(tryGetRouterRuntime(EchoController), undefined, "the runtime must be disposed and forgotten on shutdown")
})

test("two controllers in one app each get their own runtime", async () => {
  await resetNevoRouters()
  const A = makeController()
  const B = makeController()

  @Module({ imports: [NevoModule.forRoot(QUIET)], controllers: [A, B], providers: [EchoService] })
  class AppModule {}

  const app = await bootstrap(AppModule)
  try {
    const runtimeA = tryGetRouterRuntime(A)
    const runtimeB = tryGetRouterRuntime(B)
    assert.ok(runtimeA)
    assert.ok(runtimeB)
    assert.notEqual(runtimeA, runtimeB)
  } finally {
    await app.close()
  }
})
