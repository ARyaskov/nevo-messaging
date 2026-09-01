#!/usr/bin/env node
import { startDevToolsUiServer, DEFAULT_DEVTOOLS_UI_PORT } from "../devtools-ui/server"
import { MissingAssetsError } from "../devtools-ui/static"

interface CliOptions {
  port: number
  host: string
  servers: string[]
  subject?: string
  help?: boolean
}

const DEFAULT_NATS = "nats://127.0.0.1:4222"

function splitServers(raw: string): string[] {
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
}

function parseArgs(argv: string[]): CliOptions {
  const envServers = process.env["NEVO_DEVTOOLS_NATS_SERVERS"] || process.env["NATS_URL"] || DEFAULT_NATS
  const out: CliOptions = {
    port: Number(process.env["NEVO_DEVTOOLS_PORT"]) || DEFAULT_DEVTOOLS_UI_PORT,
    host: process.env["NEVO_DEVTOOLS_HOST"] || "127.0.0.1",
    servers: splitServers(envServers)
  }

  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i]
    const next = () => argv[++i]
    switch (arg) {
      case "-h":
      case "--help":
        out.help = true
        break
      case "-p":
      case "--port":
        out.port = Number(next())
        break
      case "--host":
        out.host = next()
        break
      case "-n":
      case "--nats":
        out.servers = splitServers(next() ?? "")
        break
      case "--subject":
        out.subject = next()
        break
      case "--no-nats":
        // In-process only: useful when the UI is started inside a service.
        out.servers = []
        break
      default:
        if (arg.startsWith("-")) {
          console.error(`Unknown option: ${arg}\n`)
          out.help = true
        }
        break
    }
  }

  return out
}

function printHelp(): void {
  console.log(`nevo-devtools — live dashboard for @riaskov/nevo-messaging

Usage:
  nevo-devtools [options]

Options:
  -p, --port <n>          Port to listen on (default ${DEFAULT_DEVTOOLS_UI_PORT}, env NEVO_DEVTOOLS_PORT)
      --host <addr>       Bind address (default 127.0.0.1, env NEVO_DEVTOOLS_HOST)
  -n, --nats <urls>       Comma-separated NATS servers to ingest events from
                          (default ${DEFAULT_NATS}, env NEVO_DEVTOOLS_NATS_SERVERS or NATS_URL)
      --subject <s>       DevTools subject (default __nevo.devtools)
      --no-nats           Do not bridge to NATS; show only this process's bus
  -h, --help              Show this help

Every service must publish its events. Wire it once per service:

  import { wireDevToolsToNatsByConfig } from "@riaskov/nevo-messaging"
  await wireDevToolsToNatsByConfig({ servers: ["${DEFAULT_NATS}"], bridgeLocalEvents: true })
`)
}

async function main(): Promise<void> {
  const opts = parseArgs(process.argv)

  if (opts.help) {
    printHelp()
    return
  }

  if (!Number.isInteger(opts.port) || opts.port < 0 || opts.port > 65535) {
    console.error(`Invalid --port: ${opts.port}`)
    process.exit(1)
  }

  const handle = await startDevToolsUiServer({
    port: opts.port,
    host: opts.host,
    natsServers: opts.servers,
    ...(opts.subject ? { subject: opts.subject } : {})
  })

  console.log(`nevo-devtools  ${handle.url}`)
  console.log(`NATS           ${opts.servers.length > 0 ? opts.servers.join(", ") : "(disabled — in-process bus only)"}`)
  console.log(
    `\nServices appear as they register, which happens at service startup.\n` +
      `If the Services page is empty, restart a service or send it some traffic.\n`
  )

  let shuttingDown = false
  const shutdown = () => {
    if (shuttingDown) return
    shuttingDown = true
    void handle.close().then(
      () => process.exit(0),
      () => process.exit(1)
    )
  }
  process.on("SIGINT", shutdown)
  process.on("SIGTERM", shutdown)
}

main().catch((err: unknown) => {
  if (err instanceof MissingAssetsError) {
    console.error(err.message)
    process.exit(1)
  }
  const code = (err as { code?: string })?.code
  if (code === "EADDRINUSE") {
    console.error(`Port already in use. Pick another with --port.`)
    process.exit(1)
  }
  console.error(err)
  process.exit(1)
})
