// Boots a minimal Nest application against the built package (dist/) and makes one
// signal call. Shared by the CommonJS and ESM consumer entry points.
const assert = require("node:assert/strict")
const fs = require("node:fs")
const path = require("node:path")

const SILENT = {
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

module.exports = async function runApp({ common, core, nevo, label }) {
  const { Controller, Injectable, Module } = common
  const { NestFactory } = core

  class EchoService {
    echo(params) {
      return { seen: params }
    }
  }
  Injectable()(EchoService)

  class EchoController {
    constructor() {
      this.svc = new EchoService()
    }
  }
  Controller()(EchoController)
  nevo.addSignalMetadata(EchoController, "echo.run", "echo")
  nevo.HttpSignalRouter(EchoService, { serviceName: "echo" })(EchoController)

  class AppModule {}
  Module({
    imports: [nevo.NevoModule.forRoot({ logger: SILENT, devtools: false, tracing: { enabled: false } })],
    controllers: [EchoController],
    providers: [EchoService]
  })(AppModule)

  const app = await NestFactory.createApplicationContext(AppModule, { logger: false, abortOnError: false })
  try {
    await app.init()
    assert.ok(nevo.tryGetRouterRuntime(EchoController), "NevoModule must bind the decorated controller")
    const res = await app.get(EchoController).handleSignalMessage({ method: "echo.run", uuid: "u1", params: { a: 1 }, meta: {} })
    assert.deepEqual(res.params.result, { seen: { a: 1 } })
  } finally {
    await app.close()
    await nevo.resetNevoRouters()
  }
  // Nest 12 does not export ./package.json, so read it from disk.
  const corePkg = path.join(path.dirname(require.resolve("@nestjs/core")), "package.json")
  const { version } = JSON.parse(fs.readFileSync(corePkg, "utf8"))
  console.log(`[smoke] ${label} consumer OK with @nestjs/core ${version}`)
}
