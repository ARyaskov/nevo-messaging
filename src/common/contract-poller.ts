import { setTimeout as sleep } from "node:timers/promises"
import { NEVO_CONTRACT_METHOD, ServiceContract } from "./contract"

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
    void this.tick()
    this.timer = setInterval(() => void this.tick(), this.intervalMs)
    if (typeof this.timer.unref === "function") this.timer.unref()
  }

  async pollOnce(): Promise<void> { await this.tick() }

  stop(): void {
    this.stopped = true
    if (this.timer) clearInterval(this.timer)
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

function contractsEqual(a: ServiceContract, b: ServiceContract): boolean {
  if (a.serviceVersion !== b.serviceVersion) return false
  if (a.methods.length !== b.methods.length) return false
  for (let i = 0; i < a.methods.length; i++) {
    if (a.methods[i].signalName !== b.methods[i].signalName) return false
    if (a.methods[i].version !== b.methods[i].version) return false
  }
  return true
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
