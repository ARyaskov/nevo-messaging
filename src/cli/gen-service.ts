#!/usr/bin/env node
import { promises as fs } from "node:fs"
import * as path from "node:path"

export type GenServiceType = "service" | "consumer" | "worker" | "saga" | "workflow"

interface GenOptions {
  name: string
  outDir?: string
  transport?: "nats" | "kafka" | "http" | "socket"
  port?: number
  force?: boolean
  help?: boolean
  /**
   * Kind of scaffold to emit:
   *   - `service`  (default) — full request/response microservice
   *   - `consumer` — subscribe-only handler skeleton with @Backpressure
   *   - `worker`   — background polling worker on top of the outbox/scheduler
   *   - `saga`     — saga orchestrator skeleton with compensation
   *   - `workflow` — durable workflow on top of the saga engine + EventStore
   */
  type?: GenServiceType
}

function parseArgs(argv: string[]): GenOptions {
  const out: Partial<GenOptions> = { transport: "nats", port: 8086, type: "service" }
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i]
    const next = () => argv[++i]
    switch (a) {
      case "-h": case "--help": out.help = true; break
      case "-n": case "--name": out.name = next(); break
      case "-o": case "--out": out.outDir = next(); break
      case "-t": case "--transport": out.transport = next() as any; break
      case "-p": case "--port": out.port = Number(next()); break
      case "-f": case "--force": out.force = true; break
      case "--type": out.type = next() as GenServiceType; break
      default:
        if (!out.name && !a.startsWith("-")) out.name = a
    }
  }
  return out as GenOptions
}

/**
 * Convert a service name (e.g. "legal-entity", "user_profile") into a class
 * identifier suitable for TypeScript: words capitalised, separators dropped.
 *
 *   toClassName("legal-entity")  → "LegalEntity"
 *   toClassName("user")          → "User"
 *   toClassName("user_profile")  → "UserProfile"
 */
function toClassName(s: string): string {
  return s.replaceAll(/[^A-Za-z0-9]+/g, " ").split(" ").filter(Boolean).map((w) => w[0].toUpperCase() + w.slice(1)).join("")
}

function printHelp(): void {
  process.stdout.write(`nevo gen — scaffold a new nevo module.

USAGE
  nevo-gen <name> [--type service|consumer|worker|saga|workflow]
                  [--transport nats|kafka|http|socket]
                  [--out ./services] [--port 8086]

FLAGS
  -n, --name        Module name (e.g. user, order)
      --type        What to generate:
                      service  (default) — request/response microservice
                      consumer — subscribe-only handler with @Backpressure
                      worker   — periodic background worker
                      saga     — saga orchestrator with compensation
                      workflow — durable workflow on top of saga + EventStore
  -t, --transport   Transport (default: nats)
  -o, --out         Output directory (default: ./<name>)
  -p, --port        Listen port for microservice bootstrap (default: 8086)
  -f, --force       Overwrite existing files
  -h, --help        Show help

EXAMPLES
  nevo-gen user
  nevo-gen audit  --type consumer  --transport kafka
  nevo-gen nightly-report --type worker
  nevo-gen order  --type saga
  nevo-gen onboarding --type workflow
`)
}

function moduleTemplate(name: string, className: string, transport: string): string {
  const factoryByTransport: Record<string, string> = {
    nats: `createNevoNatsClient(["COORDINATOR"], { clientIdPrefix: "${name}" })`,
    kafka: `createNevoKafkaClient(["COORDINATOR"], { clientIdPrefix: "${name}" })`,
    http: `createNevoHttpClient({ coordinator: "http://127.0.0.1:8091" }, { clientIdPrefix: "${name}" })`,
    socket: `createNevoSocketClient({ coordinator: "http://127.0.0.1:8094" }, { clientIdPrefix: "${name}" })`
  }
  const importByTransport: Record<string, string> = {
    nats: `import { createNevoNatsClient } from "@riaskov/nevo-messaging"`,
    kafka: `import { createNevoKafkaClient } from "@riaskov/nevo-messaging"`,
    http: `import { createNevoHttpClient } from "@riaskov/nevo-messaging"`,
    socket: `import { createNevoSocketClient } from "@riaskov/nevo-messaging"`
  }
  return `import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
${importByTransport[transport]}
import { ${className}Controller } from "./${name}.controller"
import { ${className}Service } from "./${name}.service"

@Module({
  imports: [ConfigModule],
  controllers: [${className}Controller],
  providers: [
    ${className}Service,
    ${factoryByTransport[transport]}
  ]
})
export class ${className}Module {}
`
}

function serviceTemplate(name: string, className: string, transport: string): string {
  const baseByTransport: Record<string, string> = {
    nats: "NatsClientBase, NevoNatsClient",
    kafka: "KafkaClientBase, NevoKafkaClient",
    http: "HttpClientBase, NevoHttpClient",
    socket: "SocketClientBase, NevoSocketClient"
  }
  const baseClassByTransport: Record<string, string> = {
    nats: "NatsClientBase",
    kafka: "KafkaClientBase",
    http: "HttpClientBase",
    socket: "SocketClientBase"
  }
  const clientTokenByTransport: Record<string, string> = {
    nats: "NEVO_NATS_CLIENT",
    kafka: "NEVO_KAFKA_CLIENT",
    http: "NEVO_HTTP_CLIENT",
    socket: "NEVO_SOCKET_CLIENT"
  }
  const clientTypeByTransport: Record<string, string> = {
    nats: "NevoNatsClient",
    kafka: "NevoKafkaClient",
    http: "NevoHttpClient",
    socket: "NevoSocketClient"
  }
  return `import { Injectable, Inject } from "@nestjs/common"
import { ${baseByTransport[transport]} } from "@riaskov/nevo-messaging"

@Injectable()
export class ${className}Service extends ${baseClassByTransport[transport]} {
  constructor(@Inject("${clientTokenByTransport[transport]}") client: ${clientTypeByTransport[transport]}) {
    super(client)
  }

  async getById(id: bigint) {
    return { id, name: "Sample ${className}" }
  }

  async create(input: { name: string }) {
    return { id: 1n, ...input }
  }
}
`
}

function controllerTemplate(name: string, className: string, transport: string): string {
  const routerByTransport: Record<string, string> = {
    nats: "NatsSignalRouter",
    kafka: "KafkaSignalRouter",
    http: "HttpSignalRouter",
    socket: "SocketSignalRouter"
  }
  return `import { Controller, Inject } from "@nestjs/common"
import { ${routerByTransport[transport]}, Signal } from "@riaskov/nevo-messaging"
import { ${className}Service } from "./${name}.service"

@Controller()
@${routerByTransport[transport]}([${className}Service])
export class ${className}Controller {
  constructor(@Inject(${className}Service) private readonly ${name}Service: ${className}Service) {}

  @Signal("${name}.getById", "getById", (data: any) => [data.id])
  getById() {}

  @Signal("${name}.create", "create", (data: any) => [data])
  create() {}
}
`
}

function mainTemplate(name: string, className: string, transport: string, port: number): string {
  const factoryByTransport: Record<string, string> = {
    nats: "createNatsMicroservice",
    kafka: "createKafkaMicroservice",
    http: "createHttpMicroservice",
    socket: "createSocketMicroservice"
  }
  return `import { ${factoryByTransport[transport]} } from "@riaskov/nevo-messaging"
import { ${className}Module } from "./${name}/${name}.module"

${factoryByTransport[transport]}({
  microserviceName: "${name}",
  module: ${className}Module,
  port: ${port}
}).then(() => console.log("[${name}] started on :${port}"))
`
}

function appModuleTemplate(className: string, name: string): string {
  return `import { Module } from "@nestjs/common"
import { ${className}Module } from "./${name}/${name}.module"

@Module({ imports: [${className}Module] })
export class AppModule {}
`
}

function packageJsonTemplate(name: string): string {
  return `{
  "name": "${name}-service",
  "version": "0.1.0",
  "private": true,
  "scripts": {
    "build": "tsc",
    "start": "node dist/main.js",
    "dev": "tsc --watch"
  },
  "dependencies": {
    "@nestjs/common": "^11.1.0",
    "@nestjs/core": "^11.1.0",
    "@nestjs/microservices": "^11.1.0",
    "@nestjs/config": "^4.0.0",
    "@nestjs/platform-fastify": "^11.1.0",
    "@riaskov/nevo-messaging": "^2.0.0",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.2"
  },
  "devDependencies": {
    "@types/node": "^22.15.30",
    "typescript": "^6.0.0"
  }
}
`
}

// ---------------------------------------------------------------------------
// Consumer template — subscribe-only handler with @Backpressure
// ---------------------------------------------------------------------------

function consumerServiceTemplate(name: string, className: string, transport: string): string {
  const baseClass: Record<string, string> = {
    nats: "NatsClientBase",
    kafka: "KafkaClientBase",
    http: "HttpClientBase",
    socket: "SocketClientBase"
  }
  const baseImport: Record<string, string> = {
    nats: "NatsClientBase, NevoNatsClient",
    kafka: "KafkaClientBase, NevoKafkaClient",
    http: "HttpClientBase, NevoHttpClient",
    socket: "SocketClientBase, NevoSocketClient"
  }
  const clientToken: Record<string, string> = {
    nats: "NEVO_NATS_CLIENT",
    kafka: "NEVO_KAFKA_CLIENT",
    http: "NEVO_HTTP_CLIENT",
    socket: "NEVO_SOCKET_CLIENT"
  }
  const clientType: Record<string, string> = {
    nats: "NevoNatsClient",
    kafka: "NevoKafkaClient",
    http: "NevoHttpClient",
    socket: "NevoSocketClient"
  }
  return `import { Injectable, Inject, OnModuleInit, OnModuleDestroy } from "@nestjs/common"
import {
  ${baseImport[transport]},
  Backpressure,
  Subscription,
  wrapSubscriptionHandler
} from "@riaskov/nevo-messaging"

/**
 * Subscribe-only consumer. The framework auto-pauses the upstream
 * subscription when in-flight handlers exceed \`highWatermark\` and resumes
 * once it falls below \`lowWatermark\`. Failed messages are nacked so the
 * broker redelivers (use \`onOverflow: "drop"\` for fire-and-forget streams).
 */
@Injectable()
export class ${className}Consumer extends ${baseClass[transport]} implements OnModuleInit, OnModuleDestroy {
  private subscription?: Subscription

  constructor(@Inject("${clientToken[transport]}") client: ${clientType[transport]}) {
    super(client)
  }

  async onModuleInit(): Promise<void> {
    this.subscription = await this.subscribe(
      "${name}",                  // upstream service name to subscribe to
      "${name}.event.*",          // wildcard pattern of events to consume
      { ack: true, durableKey: "${name}-consumer" },
      wrapSubscriptionHandler(
        async (msg, ctx) => { await this.handle(msg as any); await ctx.ack() },
        () => this.subscription!,
        { maxInflight: 200, highWatermark: 160, lowWatermark: 80, onOverflow: "nack" }
      )
    )
  }

  async onModuleDestroy(): Promise<void> {
    await this.subscription?.unsubscribe()
  }

  @Backpressure({ maxInflight: 200, highWatermark: 160, lowWatermark: 80, onOverflow: "nack", keyBy: ["service", "method", "tenantId"] })
  async handle(msg: { id: bigint; type: string }): Promise<void> {
    // TODO: project to your read model / forward downstream / write to DB.
    // Throw to nack and let the broker redeliver.
  }
}
`
}

function consumerControllerTemplate(name: string, className: string): string {
  return `import { Controller, Inject } from "@nestjs/common"
import { ${className}Consumer } from "./${name}.consumer"

@Controller()
export class ${className}Controller {
  constructor(@Inject(${className}Consumer) private readonly consumer: ${className}Consumer) {}
}
`
}

function consumerModuleTemplate(name: string, className: string, transport: string): string {
  const factoryByTransport: Record<string, string> = {
    nats: `createNevoNatsClient(["UPSTREAM"], { clientIdPrefix: "${name}" })`,
    kafka: `createNevoKafkaClient(["UPSTREAM"], { clientIdPrefix: "${name}" })`,
    http: `createNevoHttpClient({ coordinator: "http://127.0.0.1:8091" }, { clientIdPrefix: "${name}" })`,
    socket: `createNevoSocketClient({ coordinator: "http://127.0.0.1:8094" }, { clientIdPrefix: "${name}" })`
  }
  const importByTransport: Record<string, string> = {
    nats: `import { createNevoNatsClient } from "@riaskov/nevo-messaging"`,
    kafka: `import { createNevoKafkaClient } from "@riaskov/nevo-messaging"`,
    http: `import { createNevoHttpClient } from "@riaskov/nevo-messaging"`,
    socket: `import { createNevoSocketClient } from "@riaskov/nevo-messaging"`
  }
  return `import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
${importByTransport[transport]}
import { ${className}Controller } from "./${name}.controller"
import { ${className}Consumer } from "./${name}.consumer"

@Module({
  imports: [ConfigModule],
  controllers: [${className}Controller],
  providers: [
    ${className}Consumer,
    ${factoryByTransport[transport]}
  ]
})
export class ${className}Module {}
`
}

// ---------------------------------------------------------------------------
// Worker template — periodic background worker on top of the outbox
// ---------------------------------------------------------------------------

function workerServiceTemplate(name: string, className: string): string {
  return `import { Injectable, OnModuleInit, OnModuleDestroy, Inject } from "@nestjs/common"
import { Outbox, InMemoryOutboxStore } from "@riaskov/nevo-messaging"

/**
 * Periodic background worker. Drains the outbox every \`flushIntervalMs\`,
 * publishing pending messages and retrying failures with exponential backoff.
 *
 * Plug a persistent store (SqliteOutboxStore, PgOutboxStore) once your data
 * model warrants it — the InMemoryOutboxStore here is for local development.
 */
@Injectable()
export class ${className}Worker implements OnModuleInit, OnModuleDestroy {
  private readonly outbox: Outbox
  private timer?: NodeJS.Timeout
  private stopped = false

  constructor() {
    this.outbox = new Outbox({
      store: new InMemoryOutboxStore(),
      batchSize: 100,
      maxAttempts: 5,
      // Replace with a real publisher — typically your transport client.
      publisher: async (entry) => {
        // await client.publish(entry.topic, entry.method, entry.payload)
      }
    })
  }

  async onModuleInit(): Promise<void> {
    const flushIntervalMs = 1_000
    const tick = async () => {
      if (this.stopped) return
      try {
        await this.outbox.flushOnce()
      } catch (err) {
        // log + continue; the next tick retries
      }
      if (!this.stopped) this.timer = setTimeout(tick, flushIntervalMs)
      if (this.timer && typeof this.timer.unref === "function") this.timer.unref()
    }
    await tick()
  }

  async onModuleDestroy(): Promise<void> {
    this.stopped = true
    if (this.timer) clearTimeout(this.timer)
    await this.outbox.flushOnce().catch(() => undefined)
  }
}
`
}

function workerModuleTemplate(name: string, className: string): string {
  return `import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import { ${className}Worker } from "./${name}.worker"

@Module({
  imports: [ConfigModule],
  providers: [${className}Worker],
  exports: [${className}Worker]
})
export class ${className}Module {}
`
}

// ---------------------------------------------------------------------------
// Saga template — orchestrator with compensation
// ---------------------------------------------------------------------------

function sagaServiceTemplate(name: string, className: string, transport: string): string {
  const baseClass: Record<string, string> = {
    nats: "NatsClientBase",
    kafka: "KafkaClientBase",
    http: "HttpClientBase",
    socket: "SocketClientBase"
  }
  const baseImport: Record<string, string> = {
    nats: "NatsClientBase, NevoNatsClient",
    kafka: "KafkaClientBase, NevoKafkaClient",
    http: "HttpClientBase, NevoHttpClient",
    socket: "SocketClientBase, NevoSocketClient"
  }
  const clientToken: Record<string, string> = {
    nats: "NEVO_NATS_CLIENT",
    kafka: "NEVO_KAFKA_CLIENT",
    http: "NEVO_HTTP_CLIENT",
    socket: "NEVO_SOCKET_CLIENT"
  }
  const clientType: Record<string, string> = {
    nats: "NevoNatsClient",
    kafka: "NevoKafkaClient",
    http: "NevoHttpClient",
    socket: "NevoSocketClient"
  }
  return `import { Injectable, Inject } from "@nestjs/common"
import { ${baseImport[transport]}, createSaga, InMemorySagaStore, type SagaResult } from "@riaskov/nevo-messaging"

interface ${className}Ctx {
  // Carry whatever state your steps need across execute/compensate boundaries.
  inputId: bigint
  reservationId?: string
  chargeId?: string
}

@Injectable()
export class ${className}Saga extends ${baseClass[transport]} {
  // Swap for SqliteSagaStore / PgSagaStore once you outgrow in-memory.
  private readonly store = new InMemorySagaStore()

  constructor(@Inject("${clientToken[transport]}") client: ${clientType[transport]}) {
    super(client)
  }

  async run(input: { id: bigint }): Promise<SagaResult> {
    const saga = createSaga<${className}Ctx>({ store: this.store })
      .step({
        name: "reserve",
        execute: async (ctx) => {
          ctx.reservationId = await this.query("inventory", "item.reserve", { id: ctx.inputId })
        },
        compensate: async (ctx) => {
          if (ctx.reservationId) await this.emit("inventory", "item.release", { id: ctx.reservationId })
        }
      })
      .step({
        name: "charge",
        execute: async (ctx) => {
          ctx.chargeId = await this.query("payment", "card.charge", { id: ctx.inputId })
        },
        compensate: async (ctx) => {
          if (ctx.chargeId) await this.emit("payment", "card.refund", { id: ctx.chargeId })
        }
      })
      .step({
        name: "finalize",
        execute: async (ctx) => {
          await this.emit("audit", "order.created", { id: ctx.inputId, chargeId: ctx.chargeId })
        }
        // last step → no compensation needed
      })

    return saga.run({ inputId: input.id })
  }
}
`
}

function sagaControllerTemplate(name: string, className: string, transport: string): string {
  const router: Record<string, string> = {
    nats: "NatsSignalRouter",
    kafka: "KafkaSignalRouter",
    http: "HttpSignalRouter",
    socket: "SocketSignalRouter"
  }
  return `import { Controller, Inject } from "@nestjs/common"
import { ${router[transport]}, Signal } from "@riaskov/nevo-messaging"
import { ${className}Saga } from "./${name}.saga"

@Controller()
@${router[transport]}([${className}Saga])
export class ${className}Controller {
  constructor(@Inject(${className}Saga) private readonly saga: ${className}Saga) {}

  @Signal("${name}.start", "run", (data: any) => [data])
  start() {}
}
`
}

// ---------------------------------------------------------------------------
// Workflow template — durable on top of saga engine + EventStore
// ---------------------------------------------------------------------------

function workflowServiceTemplate(name: string, className: string, transport: string): string {
  const baseClass: Record<string, string> = {
    nats: "NatsClientBase",
    kafka: "KafkaClientBase",
    http: "HttpClientBase",
    socket: "SocketClientBase"
  }
  const baseImport: Record<string, string> = {
    nats: "NatsClientBase, NevoNatsClient",
    kafka: "KafkaClientBase, NevoKafkaClient",
    http: "HttpClientBase, NevoHttpClient",
    socket: "SocketClientBase, NevoSocketClient"
  }
  const clientToken: Record<string, string> = {
    nats: "NEVO_NATS_CLIENT",
    kafka: "NEVO_KAFKA_CLIENT",
    http: "NEVO_HTTP_CLIENT",
    socket: "NEVO_SOCKET_CLIENT"
  }
  const clientType: Record<string, string> = {
    nats: "NevoNatsClient",
    kafka: "NevoKafkaClient",
    http: "NevoHttpClient",
    socket: "NevoSocketClient"
  }
  return `import { Injectable, Inject } from "@nestjs/common"
import {
  ${baseImport[transport]},
  createSaga, InMemorySagaStore,
  InMemoryEventStore,
  type SagaResult
} from "@riaskov/nevo-messaging"

interface ${className}Ctx {
  workflowId: string
  inputId: bigint
  // Add fields as the workflow grows; the EventStore preserves history.
}

/**
 * Durable workflow skeleton.
 *
 * Combines the saga engine (step orchestration + compensation) with an
 * append-only EventStore for history. On crash, \`resume(workflowId)\`
 * replays the events to rebuild state and continues from the last
 * uncompleted step.
 *
 * For production durability, swap the in-memory stores for SQLite/Postgres
 * implementations once they ship in the framework.
 */
@Injectable()
export class ${className}Workflow extends ${baseClass[transport]} {
  private readonly sagaStore = new InMemorySagaStore()
  private readonly events = new InMemoryEventStore()

  constructor(@Inject("${clientToken[transport]}") client: ${clientType[transport]}) {
    super(client)
  }

  async start(input: { id: bigint }): Promise<SagaResult> {
    const workflowId = \`wf-\${Date.now()}-\${Math.random().toString(36).slice(2, 8)}\`
    await this.events.append({ type: "${name}.started", aggregateId: workflowId, payload: { input } })
    const result = await this.runSteps(workflowId, { workflowId, inputId: input.id })
    await this.events.append({ type: \`${name}.\${result.status}\`, aggregateId: workflowId, payload: result })
    return result
  }

  async resume(workflowId: string): Promise<SagaResult> {
    // Walk history to rebuild context, then re-enter saga.
    const history = await this.events.read({ aggregateId: workflowId })
    const last = history[history.length - 1]
    const inputId = (history[0]?.payload as any)?.input?.id ?? 0n
    if (last?.type === \`${name}.completed\`) return last.payload as SagaResult
    return this.runSteps(workflowId, { workflowId, inputId })
  }

  private async runSteps(workflowId: string, ctx: ${className}Ctx): Promise<SagaResult> {
    const saga = createSaga<${className}Ctx>({ store: this.sagaStore, sagaId: workflowId })
      .step({
        name: "step1",
        execute: async (ctx) => {
          await this.events.append({ type: "${name}.step1.executed", aggregateId: workflowId, payload: { ctx } })
          // call out to a downstream service…
        },
        compensate: async (ctx) => {
          await this.events.append({ type: "${name}.step1.compensated", aggregateId: workflowId, payload: { ctx } })
        }
      })
      .step({
        name: "step2",
        execute: async (ctx) => {
          await this.events.append({ type: "${name}.step2.executed", aggregateId: workflowId, payload: { ctx } })
        }
      })

    return saga.run(ctx)
  }
}
`
}

function workflowControllerTemplate(name: string, className: string, transport: string): string {
  const router: Record<string, string> = {
    nats: "NatsSignalRouter",
    kafka: "KafkaSignalRouter",
    http: "HttpSignalRouter",
    socket: "SocketSignalRouter"
  }
  return `import { Controller, Inject } from "@nestjs/common"
import { ${router[transport]}, Signal } from "@riaskov/nevo-messaging"
import { ${className}Workflow } from "./${name}.workflow"

@Controller()
@${router[transport]}([${className}Workflow])
export class ${className}Controller {
  constructor(@Inject(${className}Workflow) private readonly workflow: ${className}Workflow) {}

  @Signal("${name}.start",  "start",  (data: any) => [data])
  start() {}

  @Signal("${name}.resume", "resume", (data: any) => [data.workflowId])
  resume() {}
}
`
}

function tsconfigTemplate(): string {
  return `{
  "compilerOptions": {
    "target": "ES2024",
    "module": "commonjs",
    "lib": ["ESNext"],
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "experimentalDecorators": true,
    "emitDecoratorMetadata": true,
    "skipLibCheck": true
  },
  "include": ["src/**/*"]
}
`
}

function buildFilesForType(opts: GenOptions, name: string, className: string, outDir: string, srcDir: string): Array<[string, string]> {
  const transport = opts.transport!
  const port = opts.port ?? 8086
  switch (opts.type) {
    case "consumer":
      return [
        [path.join(srcDir, `${name}.module.ts`), consumerModuleTemplate(name, className, transport)],
        [path.join(srcDir, `${name}.consumer.ts`), consumerServiceTemplate(name, className, transport)],
        [path.join(srcDir, `${name}.controller.ts`), consumerControllerTemplate(name, className)],
        [path.join(outDir, "src", "app.module.ts"), appModuleTemplate(className, name)],
        [path.join(outDir, "src", "main.ts"), mainTemplate(name, className, transport, port)],
        [path.join(outDir, "package.json"), packageJsonTemplate(name)],
        [path.join(outDir, "tsconfig.json"), tsconfigTemplate()]
      ]
    case "worker":
      return [
        [path.join(srcDir, `${name}.module.ts`), workerModuleTemplate(name, className)],
        [path.join(srcDir, `${name}.worker.ts`), workerServiceTemplate(name, className)],
        [path.join(outDir, "src", "app.module.ts"), appModuleTemplate(className, name)],
        // workers don't expose RPC by default — main.ts boots Nest but skips microservice
        [path.join(outDir, "src", "main.ts"),
          `import { NestFactory } from "@nestjs/core"
import { GracefulShutdown } from "@riaskov/nevo-messaging"
import { AppModule } from "./app.module"

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule)
  console.log("[${name}] worker started")

  // Drain gracefully on SIGTERM/SIGINT: closing the Nest context fires the worker's
  // OnModuleDestroy, which flushes any pending outbox messages before we exit.
  const shutdown = new GracefulShutdown()
  shutdown.register("close nest app", () => app.close())
  for (const sig of ["SIGTERM", "SIGINT"] as const) {
    process.on(sig, async () => {
      if (shutdown.isShuttingDown()) return
      console.log("[${name}] shutting down")
      await shutdown.shutdown(30_000)
      process.exit(0)
    })
  }
}
bootstrap()
`],
        [path.join(outDir, "package.json"), packageJsonTemplate(name)],
        [path.join(outDir, "tsconfig.json"), tsconfigTemplate()]
      ]
    case "saga":
      return [
        [path.join(srcDir, `${name}.module.ts`), moduleTemplate(name, className, transport).replace(`${className}Service`, `${className}Saga`)],
        [path.join(srcDir, `${name}.saga.ts`), sagaServiceTemplate(name, className, transport)],
        [path.join(srcDir, `${name}.controller.ts`), sagaControllerTemplate(name, className, transport)],
        [path.join(outDir, "src", "app.module.ts"), appModuleTemplate(className, name)],
        [path.join(outDir, "src", "main.ts"), mainTemplate(name, className, transport, port)],
        [path.join(outDir, "package.json"), packageJsonTemplate(name)],
        [path.join(outDir, "tsconfig.json"), tsconfigTemplate()]
      ]
    case "workflow":
      return [
        [path.join(srcDir, `${name}.module.ts`), moduleTemplate(name, className, transport).replace(`${className}Service`, `${className}Workflow`)],
        [path.join(srcDir, `${name}.workflow.ts`), workflowServiceTemplate(name, className, transport)],
        [path.join(srcDir, `${name}.controller.ts`), workflowControllerTemplate(name, className, transport)],
        [path.join(outDir, "src", "app.module.ts"), appModuleTemplate(className, name)],
        [path.join(outDir, "src", "main.ts"), mainTemplate(name, className, transport, port)],
        [path.join(outDir, "package.json"), packageJsonTemplate(name)],
        [path.join(outDir, "tsconfig.json"), tsconfigTemplate()]
      ]
    case "service":
    default:
      return [
        [path.join(srcDir, `${name}.module.ts`), moduleTemplate(name, className, transport)],
        [path.join(srcDir, `${name}.service.ts`), serviceTemplate(name, className, transport)],
        [path.join(srcDir, `${name}.controller.ts`), controllerTemplate(name, className, transport)],
        [path.join(outDir, "src", "app.module.ts"), appModuleTemplate(className, name)],
        [path.join(outDir, "src", "main.ts"), mainTemplate(name, className, transport, port)],
        [path.join(outDir, "package.json"), packageJsonTemplate(name)],
        [path.join(outDir, "tsconfig.json"), tsconfigTemplate()]
      ]
  }
}

export async function runGen(argv: string[]): Promise<number> {
  const opts = parseArgs(argv)
  if (opts.help || !opts.name) { printHelp(); return opts.help ? 0 : 2 }
  const validTypes: GenServiceType[] = ["service", "consumer", "worker", "saga", "workflow"]
  if (opts.type && !validTypes.includes(opts.type)) {
    process.stderr.write(`Unknown --type "${opts.type}". Valid: ${validTypes.join(", ")}\n`)
    return 2
  }

  const name = opts.name.toLowerCase()
  const className = toClassName(name)
  const outDir = path.resolve(opts.outDir ?? `./${name}`)
  const srcDir = path.join(outDir, "src", name)

  await fs.mkdir(srcDir, { recursive: true })
  await fs.mkdir(path.join(outDir, "src"), { recursive: true })

  const files = buildFilesForType(opts, name, className, outDir, srcDir)

  for (const [filePath, content] of files) {
    if (!opts.force) {
      try { await fs.access(filePath); process.stderr.write(`Refusing to overwrite ${filePath} (use --force)\n`); return 1 } catch {}
    }
    await fs.writeFile(filePath, content, "utf8")
  }

  process.stdout.write(
    `Scaffolded ${opts.transport} ${opts.type ?? "service"} "${name}" at ${outDir}\n` +
    `Next steps:\n  cd ${outDir}\n  npm install\n  npm run dev\n`
  )
  return 0
}

if (require.main === module) {
  runGen(process.argv).then((code) => process.exit(code)).catch((err) => {
    process.stderr.write(`Unexpected error: ${err?.message ?? err}\n`)
    process.exit(1)
  })
}
