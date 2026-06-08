import { NEVO_CONTRACT_METHOD, ServiceContract, SchemaDescriptor } from "./contract"

export const NEVO_CONTRACT_CHANGED_METHOD = "nevo.contract.changed"

export interface ContractPollerOptions {
  intervalMs?: number
  onChange?: (next: ServiceContract, prev: ServiceContract | null) => void
}

export interface ContractFetcher {
  fetch(serviceName: string): Promise<ServiceContract>
}

export class ContractPoller {
  private timer?: NodeJS.Timeout
  private readonly contracts = new Map<string, ServiceContract>()
  private readonly serviceNames: string[]
  private readonly fetcher: ContractFetcher
  private readonly intervalMs: number
  private readonly onChange?: ContractPollerOptions["onChange"]
  private stopped = false

  constructor(serviceNames: string[], fetcher: ContractFetcher, opts: ContractPollerOptions = {}) {
    this.serviceNames = serviceNames
    this.fetcher = fetcher
    this.intervalMs = opts.intervalMs ?? 30_000
    this.onChange = opts.onChange
  }

  start(): void {
    this.stopped = false
    // Self-scheduling loop so a poll that outlasts the interval can't overlap the next.
    const loop = async () => {
      if (this.stopped) return
      const startedAt = performance.now()
      try {
        await this.tick()
      } catch {
        // swallow: a failed tick must not stop the loop
      }
      if (this.stopped) return
      const elapsed = performance.now() - startedAt
      const delay = Math.max(0, this.intervalMs - elapsed)
      this.timer = setTimeout(loop, delay)
      if (typeof this.timer.unref === "function") this.timer.unref()
    }
    void loop()
  }

  async pollOnce(): Promise<void> { await this.tick() }

  stop(): void {
    this.stopped = true
    if (this.timer) clearTimeout(this.timer)
    this.timer = undefined
  }

  getContract(serviceName: string): ServiceContract | undefined {
    return this.contracts.get(serviceName)
  }

  private async tick(): Promise<void> {
    if (this.stopped) return
    for (const svc of this.serviceNames) {
      try {
        const next = await this.fetcher.fetch(svc)
        const prev = this.contracts.get(svc) ?? null
        if (!prev || !contractsEqual(prev, next)) {
          this.contracts.set(svc, next)
          this.onChange?.(next, prev)
        }
      } catch {
        // ignore
      }
    }
  }
}

export function contractsEqual(a: ServiceContract, b: ServiceContract): boolean {
  if (a.serviceVersion !== b.serviceVersion) return false
  if (a.methods.length !== b.methods.length) return false
  for (let i = 0; i < a.methods.length; i++) {
    const am = a.methods[i]
    const bm = b.methods[i]
    if (am.signalName !== bm.signalName) return false
    if (am.version !== bm.version) return false
    // Compare schema shape, not just version, so an unversioned schema change still registers as drift.
    if (schemaKey(am.paramsSchema) !== schemaKey(bm.paramsSchema)) return false
    if (schemaKey(am.resultSchema) !== schemaKey(bm.resultSchema)) return false
  }
  return true
}

function schemaKey(schema: SchemaDescriptor | null | undefined): string {
  return schema ? stableStringify(schema) : "null"
}

// Order-insensitive canonical serialization (keys sorted) for structural comparison.
function stableStringify(value: unknown): string {
  if (value === null || value === undefined) return "null"
  if (typeof value === "bigint") return `${value}n`
  if (typeof value !== "object") {
    const s = JSON.stringify(value)
    return s === undefined ? "null" : s
  }
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`
  const obj = value as Record<string, unknown>
  const keys = Object.keys(obj).sort()
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(",")}}`
}

export async function broadcastContractChanged(
  client: { broadcast: (method: string, params: unknown) => Promise<void> },
  serviceName: string,
  contractVersion: string
): Promise<void> {
  try {
    await client.broadcast(NEVO_CONTRACT_CHANGED_METHOD, { serviceName, contractVersion, ts: Date.now() })
  } catch {
    // best effort
  }
}

export function createContractFetcherForClient(client: { query: (svc: string, method: string, params: unknown) => Promise<unknown> }): ContractFetcher {
  return {
    fetch: async (serviceName: string) => {
      const result = await client.query(serviceName, NEVO_CONTRACT_METHOD, {})
      return result as ServiceContract
    }
  }
}
