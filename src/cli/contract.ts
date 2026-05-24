#!/usr/bin/env node
import { promises as fs } from "node:fs"
import * as path from "node:path"
import { randomUUID } from "node:crypto"
import { generateContractModule } from "./generate"
import { NEVO_CONTRACT_METHOD } from "../common/contract"
import type { ServiceContract } from "../common/contract"

interface CliOptions {
  transport: "http" | "nats"
  service: string
  url?: string
  servers?: string[]
  out?: string
  timeoutMs?: number
  serviceMapName?: string
  authToken?: string
  print?: boolean
  help?: boolean
}

function parseArgs(argv: string[]): CliOptions {
  const out: Partial<CliOptions> = { transport: "http", timeoutMs: 10000 }
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i]
    const next = () => argv[++i]
    switch (a) {
      case "-h":
      case "--help": out.help = true; break
      case "-t":
      case "--transport": out.transport = next() as any; break
      case "-s":
      case "--service": out.service = next(); break
      case "-u":
      case "--url": out.url = next(); break
      case "--servers": out.servers = next().split(","); break
      case "-o":
      case "--out": out.out = next(); break
      case "--timeout": out.timeoutMs = Number(next()); break
      case "--name": out.serviceMapName = next(); break
      case "--auth-token": out.authToken = next(); break
      case "--print": out.print = true; break
      default:
        if (a.startsWith("--")) {
          process.stderr.write(`Unknown flag: ${a}\n`)
          process.exit(2)
        }
    }
  }
  return out as CliOptions
}

function printHelp(): void {
  const help = `nevo-contract — generate a type-safe contract for a remote nevo service.

USAGE
  nevo-contract --transport http  --url http://host:port --service user --out src/clients/user.contract.ts
  nevo-contract --transport nats  --servers nats://127.0.0.1:4222 --service user --out src/clients/user.contract.ts

FLAGS
  -t, --transport       "http" | "nats" (default: http)
  -s, --service         Service name to query for nevo.contract  (required)
  -u, --url             Base URL of the service (http transport)
      --servers         Comma-separated list of NATS servers (nats transport)
  -o, --out             Output .ts path. If omitted, writes to stdout (unless --print)
      --timeout         Request timeout in ms (default 10000)
      --name            Override TypeScript contract interface name
      --auth-token      Bearer / nevo auth token to attach to the discovery request
      --print           Print result to stdout instead of writing a file
  -h, --help            Show help
`
  process.stdout.write(help)
}

async function fetchContractHttp(opts: CliOptions): Promise<ServiceContract> {
  if (!opts.url) throw new Error("--url is required for http transport")
  const endpoint = `${opts.url.replace(/\/+$/, "")}/${opts.service.toLowerCase()}-events`
  const uuid = randomUUID()
  const body = {
    uuid,
    method: NEVO_CONTRACT_METHOD,
    params: {},
    meta: {
      type: "query",
      service: "nevo-cli",
      instanceId: randomUUID(),
      ts: Date.now(),
      auth: opts.authToken ? { token: opts.authToken } : undefined
    }
  }
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), opts.timeoutMs ?? 10000)
  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal
    })
    if (!res.ok) throw new Error(`HTTP ${res.status} from ${endpoint}`)
    const data: any = await res.json().catch(() => null)
    if (!data) throw new Error("Empty contract response")
    if (data?.params?.error) throw new Error(`Service returned error: ${data.params.error.message}`)
    return data?.params?.result as ServiceContract
  } finally {
    clearTimeout(timer)
  }
}

async function fetchContractNats(opts: CliOptions): Promise<ServiceContract> {
  let connect: any
  try {
    // nats.js v3 split the legacy `nats` package: `connect` lives in transport-node.
    connect = require("@nats-io/transport-node").connect
  } catch {
    throw new Error("Missing optional dependency '@nats-io/transport-node'. Install with: npm i @nats-io/transport-node")
  }
  const enc = new TextEncoder()
  const dec = new TextDecoder()
  const nc = await connect({ servers: opts.servers && opts.servers.length > 0 ? opts.servers : ["nats://127.0.0.1:4222"] })
  try {
    const subject = `${opts.service.toLowerCase()}-events`
    const uuid = randomUUID()
    const payload = JSON.stringify({
      uuid,
      method: NEVO_CONTRACT_METHOD,
      params: {},
      meta: {
        type: "query",
        service: "nevo-cli",
        instanceId: randomUUID(),
        ts: Date.now(),
        codec: "json",
        auth: opts.authToken ? { token: opts.authToken } : undefined
      }
    })
    const msg = await nc.request(subject, enc.encode(payload), { timeout: opts.timeoutMs ?? 10000 })
    const raw = dec.decode(msg.data)
    let parsed: any
    try { parsed = JSON.parse(raw) } catch {
      throw new Error(`Failed to parse contract response (got ${raw.length} bytes — server may use MessagePack; switch to HTTP transport or expose nevo.contract over JSON)`)
    }
    if (parsed?.params?.error) throw new Error(`Service returned error: ${parsed.params.error.message}`)
    return parsed?.params?.result as ServiceContract
  } finally {
    try { await nc.drain() } catch {}
  }
}

export async function runCli(argv: string[]): Promise<number> {
  const opts = parseArgs(argv)
  if (opts.help) { printHelp(); return 0 }
  if (!opts.service) { process.stderr.write("Missing --service\n"); printHelp(); return 2 }

  let contract: ServiceContract
  try {
    contract = opts.transport === "nats" ? await fetchContractNats(opts) : await fetchContractHttp(opts)
  } catch (err: any) {
    process.stderr.write(`Failed to fetch contract: ${err?.message ?? err}\n`)
    return 1
  }
  if (!contract || !Array.isArray(contract.methods)) {
    process.stderr.write(`Invalid contract response (no methods)\n`)
    return 1
  }

  const ts = generateContractModule(contract, { serviceMapName: opts.serviceMapName, serviceName: opts.service })

  if (opts.print || !opts.out) {
    process.stdout.write(ts)
    return 0
  }
  const outAbs = path.resolve(opts.out)
  await fs.mkdir(path.dirname(outAbs), { recursive: true })
  await fs.writeFile(outAbs, ts, "utf8")
  process.stdout.write(`Wrote contract to ${outAbs} (${contract.methods.length} methods)\n`)
  return 0
}

if (require.main === module) {
  runCli(process.argv).then((code) => process.exit(code)).catch((err) => {
    process.stderr.write(`Unexpected error: ${err?.message ?? err}\n`)
    process.exit(1)
  })
}
