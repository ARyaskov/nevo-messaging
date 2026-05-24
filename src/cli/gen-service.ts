#!/usr/bin/env node
import { promises as fs } from "node:fs"
import * as path from "node:path"

interface GenOptions {
  name: string
  outDir?: string
  transport?: "nats" | "kafka" | "http" | "socket"
  port?: number
  force?: boolean
  help?: boolean
}

function parseArgs(argv: string[]): GenOptions {
  const out: Partial<GenOptions> = { transport: "nats", port: 8086 }
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
  process.stdout.write(`nevo gen:service — scaffold a new nevo microservice.

USAGE
  nevo-gen <name> [--transport nats|kafka|http|socket] [--out ./services] [--port 8086]

FLAGS
  -n, --name        Service name (e.g. user)
  -t, --transport   Transport (default: nats)
  -o, --out         Output directory (default: ./<name>)
  -p, --port        Listen port for microservice bootstrap (default: 8086)
  -f, --force       Overwrite existing files
  -h, --help        Show help
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

export async function runGen(argv: string[]): Promise<number> {
  const opts = parseArgs(argv)
  if (opts.help || !opts.name) { printHelp(); return opts.help ? 0 : 2 }

  const name = opts.name.toLowerCase()
  const className = toClassName(name)
  const outDir = path.resolve(opts.outDir ?? `./${name}`)
  const srcDir = path.join(outDir, "src", name)

  await fs.mkdir(srcDir, { recursive: true })
  await fs.mkdir(path.join(outDir, "src"), { recursive: true })

  const files: Array<[string, string]> = [
    [path.join(srcDir, `${name}.module.ts`), moduleTemplate(name, className, opts.transport!)],
    [path.join(srcDir, `${name}.service.ts`), serviceTemplate(name, className, opts.transport!)],
    [path.join(srcDir, `${name}.controller.ts`), controllerTemplate(name, className, opts.transport!)],
    [path.join(outDir, "src", "app.module.ts"), appModuleTemplate(className, name)],
    [path.join(outDir, "src", "main.ts"), mainTemplate(name, className, opts.transport!, opts.port ?? 8086)],
    [path.join(outDir, "package.json"), packageJsonTemplate(name)],
    [path.join(outDir, "tsconfig.json"), tsconfigTemplate()]
  ]

  for (const [filePath, content] of files) {
    if (!opts.force) {
      try { await fs.access(filePath); process.stderr.write(`Refusing to overwrite ${filePath} (use --force)\n`); return 1 } catch {}
    }
    await fs.writeFile(filePath, content, "utf8")
  }

  process.stdout.write(`Scaffolded ${opts.transport} service "${name}" at ${outDir}\nNext steps:\n  cd ${outDir}\n  npm install\n  npm run dev\n`)
  return 0
}

if (require.main === module) {
  runGen(process.argv).then((code) => process.exit(code)).catch((err) => {
    process.stderr.write(`Unexpected error: ${err?.message ?? err}\n`)
    process.exit(1)
  })
}
