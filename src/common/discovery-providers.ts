import { promises as dnsPromises, type LookupAddress, type SrvRecord, lookup as dnsLookup } from "node:dns"
import { promisify } from "node:util"
import { DiscoveryRegistry } from "./discovery"
import type { DiscoveryAnnouncement } from "./types"
import { getDefaultLogger, type NevoLogger } from "./logger"

const dnsLookupAsync = promisify(dnsLookup)

// External DiscoveryProvider plumbing — pushes entries into a DiscoveryRegistry
// from off-broker registries (Consul, K8s DNS, Etcd, Eureka, Cloud Map, Nomad).

export interface DiscoveryProvider {
  readonly id: string
  start(sink: DiscoverySink): Promise<void> | void
  stop(): Promise<void> | void
}

export interface DiscoverySink {
  /** Replace all entries for a service with this batch. */
  replace(serviceName: string, entries: DiscoveryAnnouncement[]): void
  /** Upsert a single entry (streaming/watch providers). */
  upsert(entry: DiscoveryAnnouncement): void
}

export class RegistryDiscoverySink implements DiscoverySink {
  private readonly registry: DiscoveryRegistry
  private readonly knownPerService = new Map<string, Set<string>>()

  constructor(registry: DiscoveryRegistry) {
    this.registry = registry
  }

  upsert(entry: DiscoveryAnnouncement): void {
    const stamped: DiscoveryAnnouncement = { ...entry, ts: entry.ts || Date.now() }
    this.registry.update(stamped)
    let bag = this.knownPerService.get(stamped.serviceName)
    if (!bag) {
      bag = new Set<string>()
      this.knownPerService.set(stamped.serviceName, bag)
    }
    bag.add(`${stamped.serviceName}::${stamped.instanceId}`)
  }

  replace(serviceName: string, entries: DiscoveryAnnouncement[]): void {
    const before = new Set(this.registry.listInstanceIdsFor(serviceName))
    const seen = new Set<string>()
    for (const e of entries) {
      const instanceId = e.instanceId || e.clientId || serviceName
      this.upsert({ ...e, instanceId })
      seen.add(instanceId)
    }
    for (const id of before) {
      if (!seen.has(id)) this.registry.removeInstance(serviceName, id)
    }
    this.knownPerService.set(serviceName, new Set(Array.from(seen).map((id) => `${serviceName}::${id}`)))
  }
}

// Consul ─────────────────────────────────────────────────────────────────────

export interface ConsulDiscoveryProviderOptions {
  url: string
  /** Service names to track. If omitted, polls `/v1/catalog/services`. */
  serviceNames?: string[]
  datacenter?: string
  token?: string
  pollIntervalMs?: number
  /** Use blocking queries (HTTP long-poll) when set: `?wait=<ms>ms`. */
  waitMs?: number
  fetcher?: typeof fetch
  transport?: (node: ConsulNode) => string
  logger?: NevoLogger
  failFast?: boolean
}

export interface ConsulNode {
  Node: string
  ServiceID: string
  ServiceName: string
  ServiceAddress?: string
  Address?: string
  ServicePort?: number
  ServiceTags?: string[]
  ServiceMeta?: Record<string, string>
}

/** Consul `/v1/health/service/:name` poller. No `consul` npm dep. */
export class ConsulDiscoveryProvider implements DiscoveryProvider {
  readonly id = "consul"
  private timer?: NodeJS.Timeout
  private stopped = false
  private indices = new Map<string, string>()
  private readonly logger: NevoLogger
  private readonly fetcher: typeof fetch
  private sink?: DiscoverySink

  constructor(private readonly opts: ConsulDiscoveryProviderOptions) {
    if (!opts.url) throw new Error("ConsulDiscoveryProvider: `url` is required")
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "discovery.consul" })
    this.fetcher = opts.fetcher ?? fetch
  }

  async start(sink: DiscoverySink): Promise<void> {
    this.sink = sink
    this.stopped = false
    if (this.opts.waitMs && this.opts.waitMs > 0) {
      // Blocking-query mode: kick off one in-flight long-poll per service.
      const names = await this.resolveServiceNames()
      for (const name of names) void this.blockingLoop(name)
      return
    }
    // Polling mode.
    const interval = Math.max(500, this.opts.pollIntervalMs ?? 5000)
    const tick = async () => {
      if (this.stopped) return
      try {
        await this.refreshOnce()
      } catch (err) {
        this.logger.warn({ event: "consul.poll.failed", err: (err as Error)?.message })
        if (this.opts.failFast) this.stopped = true
      } finally {
        if (!this.stopped) this.timer = setTimeout(tick, interval)
        if (this.timer && typeof this.timer.unref === "function") this.timer.unref()
      }
    }
    await tick()
  }

  async stop(): Promise<void> {
    this.stopped = true
    if (this.timer) clearTimeout(this.timer)
  }

  private async resolveServiceNames(): Promise<string[]> {
    if (this.opts.serviceNames?.length) return this.opts.serviceNames
    const url = this.buildUrl("/v1/catalog/services")
    const res = await this.fetchJson<Record<string, string[]>>(url)
    return Object.keys(res ?? {})
  }

  private async refreshOnce(): Promise<void> {
    if (!this.sink) return
    const names = await this.resolveServiceNames()
    await Promise.all(names.map((n) => this.refreshService(n).catch(() => undefined)))
  }

  private async blockingLoop(name: string): Promise<void> {
    while (!this.stopped) {
      try {
        await this.refreshService(name, true)
      } catch (err) {
        this.logger.warn({ event: "consul.watch.failed", service: name, err: (err as Error)?.message })
        await new Promise((r) => setTimeout(r, Math.max(500, this.opts.pollIntervalMs ?? 1000)))
      }
    }
  }

  private async refreshService(serviceName: string, blocking = false): Promise<void> {
    if (!this.sink) return
    const params = new URLSearchParams()
    params.set("passing", "true")
    if (this.opts.datacenter) params.set("dc", this.opts.datacenter)
    if (blocking) {
      params.set("wait", `${Math.max(1, this.opts.waitMs ?? 30000)}ms`)
      const last = this.indices.get(serviceName)
      if (last) params.set("index", last)
    }
    const url = this.buildUrl(`/v1/health/service/${encodeURIComponent(serviceName)}?${params.toString()}`)
    const res = await this.fetchRaw(url)
    if (!res.ok) throw new Error(`consul ${res.status}: ${await res.text().catch(() => "")}`)
    const idx = res.headers.get("x-consul-index")
    if (idx) this.indices.set(serviceName, idx)
    const body = (await res.json()) as Array<{ Service?: Partial<ConsulNode>; Node?: { Address?: string } }>
    const announcements: DiscoveryAnnouncement[] = body
      .map((entry) => {
        const svc = entry.Service ?? {}
        if (!svc.ServiceID || !svc.ServiceName) return null
        const announcement: DiscoveryAnnouncement = {
          serviceName: svc.ServiceName,
          instanceId: svc.ServiceID,
          clientId: svc.ServiceID,
          transport: this.opts.transport
            ? this.opts.transport({
                Node: "",
                ServiceID: svc.ServiceID,
                ServiceName: svc.ServiceName,
                ServiceAddress: svc.ServiceAddress,
                Address: entry.Node?.Address,
                ServicePort: svc.ServicePort,
                ServiceTags: svc.ServiceTags,
                ServiceMeta: svc.ServiceMeta
              })
            : "http",
          ts: Date.now(),
          host: svc.ServiceAddress ?? entry.Node?.Address,
          port: svc.ServicePort,
          capabilities: svc.ServiceTags,
          meta: { source: "consul", tags: svc.ServiceTags, ...(svc.ServiceMeta ?? {}) }
        }
        return announcement
      })
      .filter(Boolean) as DiscoveryAnnouncement[]
    this.sink.replace(serviceName, announcements)
  }

  private buildUrl(path: string): string {
    const base = this.opts.url.replace(/\/$/, "")
    return `${base}${path}`
  }

  private async fetchRaw(url: string): Promise<Response> {
    const headers: Record<string, string> = { Accept: "application/json" }
    if (this.opts.token) headers["X-Consul-Token"] = this.opts.token
    return this.fetcher(url, { headers })
  }

  private async fetchJson<T>(url: string): Promise<T> {
    const res = await this.fetchRaw(url)
    if (!res.ok) throw new Error(`consul ${res.status}: ${await res.text().catch(() => "")}`)
    return (await res.json()) as T
  }
}

// Kubernetes DNS ─────────────────────────────────────────────────────────────
// Headless service (clusterIP: None) → A record per pod, or SRV per port.

export interface KubernetesDnsDiscoveryProviderOptions {
  services: Array<
    | string
    | { name: string; namespace?: string; port?: number; portName?: string; transport?: string }
  >
  /** Default `svc.cluster.local`. */
  clusterDomain?: string
  defaultNamespace?: string
  pollIntervalMs?: number
  resolver?: {
    lookup?: (hostname: string) => Promise<LookupAddress[]>
    resolveSrv?: (hostname: string) => Promise<SrvRecord[]>
  }
  logger?: NevoLogger
}

export class KubernetesDnsDiscoveryProvider implements DiscoveryProvider {
  readonly id = "k8s-dns"
  private timer?: NodeJS.Timeout
  private stopped = false
  private sink?: DiscoverySink
  private readonly logger: NevoLogger
  private readonly resolveSrv: (hostname: string) => Promise<SrvRecord[]>
  private readonly lookup: (hostname: string) => Promise<LookupAddress[]>

  constructor(private readonly opts: KubernetesDnsDiscoveryProviderOptions) {
    if (!opts.services?.length) throw new Error("KubernetesDnsDiscoveryProvider: `services` must not be empty")
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "discovery.k8s-dns" })
    this.resolveSrv = opts.resolver?.resolveSrv ?? dnsPromises.resolveSrv.bind(dnsPromises)
    this.lookup =
      opts.resolver?.lookup ??
      (async (hostname) => {
        // dns.lookup with `all: true` returns all addresses; SRV-less services
        // typically map to a single A record per pod via headless service.
        const all = (await dnsLookupAsync(hostname, { all: true, family: 0 })) as unknown as LookupAddress[]
        return Array.isArray(all) ? all : [all as unknown as LookupAddress]
      })
  }

  async start(sink: DiscoverySink): Promise<void> {
    this.sink = sink
    this.stopped = false
    const interval = Math.max(500, this.opts.pollIntervalMs ?? 10_000)
    const tick = async () => {
      if (this.stopped) return
      try {
        await this.refreshOnce()
      } catch (err) {
        this.logger.warn({ event: "k8s-dns.refresh.failed", err: (err as Error)?.message })
      } finally {
        if (!this.stopped) this.timer = setTimeout(tick, interval)
        if (this.timer && typeof this.timer.unref === "function") this.timer.unref()
      }
    }
    await tick()
  }

  async stop(): Promise<void> {
    this.stopped = true
    if (this.timer) clearTimeout(this.timer)
  }

  private async refreshOnce(): Promise<void> {
    if (!this.sink) return
    for (const raw of this.opts.services) {
      const cfg = typeof raw === "string" ? { name: raw } : raw
      const namespace = cfg.namespace ?? this.opts.defaultNamespace ?? "default"
      const cluster = this.opts.clusterDomain ?? "svc.cluster.local"
      const host = cfg.name.includes(".") ? cfg.name : `${cfg.name}.${namespace}.${cluster}`
      try {
        const announcements = cfg.portName
          ? await this.resolveSrvHost(cfg.name, host, cfg.portName, cfg.transport)
          : await this.resolveAHost(cfg.name, host, cfg.port, cfg.transport)
        this.sink.replace(cfg.name, announcements)
      } catch (err) {
        this.logger.warn({
          event: "k8s-dns.lookup.failed",
          service: cfg.name,
          host,
          err: (err as Error)?.message
        })
        this.sink.replace(cfg.name, [])
      }
    }
  }

  private async resolveSrvHost(
    name: string,
    host: string,
    portName: string,
    transport?: string
  ): Promise<DiscoveryAnnouncement[]> {
    const srvName = `_${portName}._tcp.${host}`
    const records = await this.resolveSrv(srvName)
    const now = Date.now()
    return records.map((r) => ({
      serviceName: name,
      instanceId: `${r.name}:${r.port}`,
      transport: transport ?? "http",
      ts: now,
      host: r.name,
      port: r.port,
      meta: { source: "k8s-dns", priority: r.priority, weight: r.weight }
    }))
  }

  private async resolveAHost(
    name: string,
    host: string,
    port: number | undefined,
    transport?: string
  ): Promise<DiscoveryAnnouncement[]> {
    const addrs = await this.lookup(host)
    const now = Date.now()
    return addrs.map((a) => ({
      serviceName: name,
      instanceId: a.address,
      transport: transport ?? "http",
      ts: now,
      host: a.address,
      port,
      meta: { source: "k8s-dns", family: a.family }
    }))
  }
}

// ---------------------------------------------------------------------------
// Helper that wires a provider into a registry in one call.
// ---------------------------------------------------------------------------

export async function attachDiscoveryProvider(
  registry: DiscoveryRegistry,
  provider: DiscoveryProvider
): Promise<() => Promise<void>> {
  const sink = new RegistryDiscoverySink(registry)
  await provider.start(sink)
  return async () => {
    await provider.stop()
  }
}
