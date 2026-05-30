import type { DiscoveryProvider, DiscoverySink } from "./discovery-providers"
import type { DiscoveryAnnouncement } from "./types"
import { getDefaultLogger, type NevoLogger } from "./logger"

/** Etcd / Eureka / AWS Cloud Map / Nomad discovery providers. */

// ---------------------------------------------------------------------------
// Etcd v3
// ---------------------------------------------------------------------------

/** Minimal etcd v3 client shape: getPrefix and optional watchPrefix. */
export interface EtcdClientLike {
  getPrefix(prefix: string): Promise<Record<string, string>>
  watchPrefix?(prefix: string, onChange: () => void): { close(): Promise<void> | void }
}

export interface EtcdDiscoveryProviderOptions {
  client: EtcdClientLike
  prefix?: string
  pollIntervalMs?: number
  parseEntry?: (key: string, value: string) => DiscoveryAnnouncement | null
  logger?: NevoLogger
}

export class EtcdDiscoveryProvider implements DiscoveryProvider {
  readonly id = "etcd"
  private stopped = false
  private timer?: NodeJS.Timeout
  private watcher?: { close(): Promise<void> | void }
  private readonly logger: NevoLogger
  private sink?: DiscoverySink

  constructor(private readonly opts: EtcdDiscoveryProviderOptions) {
    if (!opts.client) throw new Error("EtcdDiscoveryProvider: `client` is required")
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "discovery.etcd" })
  }

  async start(sink: DiscoverySink): Promise<void> {
    this.sink = sink
    this.stopped = false
    if (this.opts.client.watchPrefix) {
      this.watcher = this.opts.client.watchPrefix(this.prefix(), () => {
        void this.refresh().catch(() => undefined)
      })
    }
    await this.refresh()
    if (!this.opts.client.watchPrefix) this.schedule()
  }

  async stop(): Promise<void> {
    this.stopped = true
    if (this.timer) clearTimeout(this.timer)
    if (this.watcher) await this.watcher.close()
  }

  private prefix(): string { return this.opts.prefix ?? "/services/" }

  private schedule(): void {
    if (this.stopped) return
    const interval = Math.max(500, this.opts.pollIntervalMs ?? 5000)
    this.timer = setTimeout(async () => {
      try { await this.refresh() } catch (err) {
        this.logger.warn({ event: "etcd.poll.failed", err: (err as Error)?.message })
      }
      this.schedule()
    }, interval)
    if (this.timer && typeof this.timer.unref === "function") this.timer.unref()
  }

  private async refresh(): Promise<void> {
    if (!this.sink) return
    const kvs = await this.opts.client.getPrefix(this.prefix())
    const grouped = new Map<string, DiscoveryAnnouncement[]>()
    for (const [key, value] of Object.entries(kvs)) {
      const parsed = this.parseEntry(key, value)
      if (!parsed) continue
      const bag = grouped.get(parsed.serviceName) ?? []
      bag.push(parsed)
      grouped.set(parsed.serviceName, bag)
    }
    for (const [name, entries] of grouped.entries()) this.sink.replace(name, entries)
  }

  private parseEntry(key: string, value: string): DiscoveryAnnouncement | null {
    if (this.opts.parseEntry) return this.opts.parseEntry(key, value)
    try {
      const parsed = JSON.parse(value) as Partial<DiscoveryAnnouncement>
      if (!parsed.serviceName) return null
      const suffix = key.slice(this.prefix().length)
      return {
        serviceName: parsed.serviceName,
        instanceId: parsed.instanceId ?? suffix,
        transport: parsed.transport ?? "http",
        ts: Date.now(),
        host: parsed.host,
        port: parsed.port,
        capabilities: parsed.capabilities,
        meta: { source: "etcd", key, ...(parsed.meta ?? {}) }
      }
    } catch {
      return null
    }
  }
}

// ---------------------------------------------------------------------------
// Eureka (Spring Cloud)
// ---------------------------------------------------------------------------

export interface EurekaDiscoveryProviderOptions {
  /** Base URL, e.g. `http://eureka.internal:8761/eureka` */
  url: string
  /** App names to track. If omitted, polls `/apps` for everything. */
  appNames?: string[]
  pollIntervalMs?: number
  fetcher?: typeof fetch
  logger?: NevoLogger
}

interface EurekaInstance {
  instanceId?: string
  hostName?: string
  ipAddr?: string
  port?: { $?: number | string; "@enabled"?: string }
  securePort?: { $?: number | string; "@enabled"?: string }
  status?: string
  metadata?: Record<string, string>
}

export class EurekaDiscoveryProvider implements DiscoveryProvider {
  readonly id = "eureka"
  private timer?: NodeJS.Timeout
  private stopped = false
  private sink?: DiscoverySink
  private readonly fetcher: typeof fetch
  private readonly logger: NevoLogger

  constructor(private readonly opts: EurekaDiscoveryProviderOptions) {
    if (!opts.url) throw new Error("EurekaDiscoveryProvider: `url` is required")
    this.fetcher = opts.fetcher ?? fetch
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "discovery.eureka" })
  }

  async start(sink: DiscoverySink): Promise<void> {
    this.sink = sink
    this.stopped = false
    const interval = Math.max(500, this.opts.pollIntervalMs ?? 10_000)
    const tick = async () => {
      if (this.stopped) return
      try { await this.refresh() } catch (err) {
        this.logger.warn({ event: "eureka.poll.failed", err: (err as Error)?.message })
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

  private async refresh(): Promise<void> {
    if (!this.sink) return
    const names = this.opts.appNames ?? (await this.resolveAppNames())
    await Promise.all(names.map((n) => this.refreshApp(n).catch(() => undefined)))
  }

  private async resolveAppNames(): Promise<string[]> {
    const url = `${this.opts.url.replace(/\/$/, "")}/apps`
    const res = await this.fetcher(url, { headers: { Accept: "application/json" } })
    if (!res.ok) throw new Error(`eureka /apps ${res.status}`)
    const body = (await res.json()) as { applications?: { application?: Array<{ name?: string }> } }
    return (body.applications?.application ?? []).map((a) => a.name).filter(Boolean) as string[]
  }

  private async refreshApp(name: string): Promise<void> {
    if (!this.sink) return
    const url = `${this.opts.url.replace(/\/$/, "")}/apps/${encodeURIComponent(name)}`
    const res = await this.fetcher(url, { headers: { Accept: "application/json" } })
    if (!res.ok) {
      this.sink.replace(name.toLowerCase(), [])
      return
    }
    const body = (await res.json()) as { application?: { instance?: EurekaInstance[] } }
    const instances = body.application?.instance ?? []
    const announcements: DiscoveryAnnouncement[] = instances
      .filter((i) => i.status === "UP")
      .map((i) => {
        const portRaw = i.securePort?.["@enabled"] === "true" ? i.securePort?.$ : i.port?.$
        return {
          serviceName: name.toLowerCase(),
          instanceId: i.instanceId ?? `${i.hostName}:${portRaw}`,
          transport: "http",
          ts: Date.now(),
          host: i.ipAddr ?? i.hostName,
          port: portRaw === undefined ? undefined : Number(portRaw),
          meta: { source: "eureka", metadata: i.metadata ?? {} }
        }
      })
    this.sink.replace(name.toLowerCase(), announcements)
  }
}

// ---------------------------------------------------------------------------
// AWS Cloud Map
// ---------------------------------------------------------------------------

/** Minimal AWS Cloud Map (Service Discovery) client shape. */
export interface CloudMapClientLike {
  discoverInstances(input: {
    NamespaceName: string
    ServiceName: string
    HealthStatus?: "HEALTHY" | "UNHEALTHY" | "ALL" | "HEALTHY_OR_ELSE_ALL"
    QueryParameters?: Record<string, string>
  }): Promise<{
    Instances?: Array<{
      InstanceId?: string
      Attributes?: Record<string, string>
      HealthStatus?: string
    }>
  }>
}

export interface CloudMapDiscoveryProviderOptions {
  client: CloudMapClientLike
  services: Array<{ namespace: string; name: string; transport?: string }>
  pollIntervalMs?: number
  logger?: NevoLogger
}

export class CloudMapDiscoveryProvider implements DiscoveryProvider {
  readonly id = "aws-cloud-map"
  private timer?: NodeJS.Timeout
  private stopped = false
  private sink?: DiscoverySink
  private readonly logger: NevoLogger

  constructor(private readonly opts: CloudMapDiscoveryProviderOptions) {
    if (!opts.client) throw new Error("CloudMapDiscoveryProvider: `client` is required")
    if (!opts.services?.length) throw new Error("CloudMapDiscoveryProvider: `services` must not be empty")
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "discovery.aws-cloud-map" })
  }

  async start(sink: DiscoverySink): Promise<void> {
    this.sink = sink
    this.stopped = false
    const interval = Math.max(500, this.opts.pollIntervalMs ?? 10_000)
    const tick = async () => {
      if (this.stopped) return
      try { await this.refresh() } catch (err) {
        this.logger.warn({ event: "cloud-map.poll.failed", err: (err as Error)?.message })
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

  private async refresh(): Promise<void> {
    if (!this.sink) return
    for (const svc of this.opts.services) {
      try {
        const res = await this.opts.client.discoverInstances({
          NamespaceName: svc.namespace,
          ServiceName: svc.name,
          HealthStatus: "HEALTHY"
        })
        const announcements: DiscoveryAnnouncement[] = (res.Instances ?? []).map((i) => {
          const attrs = i.Attributes ?? {}
          const host = attrs.AWS_INSTANCE_IPV4 ?? attrs.AWS_INSTANCE_IPV6 ?? attrs.AWS_INSTANCE_CNAME
          const port = attrs.AWS_INSTANCE_PORT ? Number(attrs.AWS_INSTANCE_PORT) : undefined
          return {
            serviceName: svc.name,
            instanceId: i.InstanceId ?? `${host}:${port ?? 0}`,
            transport: svc.transport ?? "http",
            ts: Date.now(),
            host,
            port,
            meta: { source: "aws-cloud-map", namespace: svc.namespace, attributes: attrs }
          }
        })
        this.sink.replace(svc.name, announcements)
      } catch (err) {
        this.logger.warn({
          event: "cloud-map.refresh.failed",
          service: svc.name,
          err: (err as Error)?.message
        })
      }
    }
  }
}

// ---------------------------------------------------------------------------
// HashiCorp Nomad
// ---------------------------------------------------------------------------

export interface NomadDiscoveryProviderOptions {
  /** Base URL, e.g. `http://nomad.service.consul:4646` */
  url: string
  serviceNames?: string[]
  namespace?: string
  pollIntervalMs?: number
  token?: string
  fetcher?: typeof fetch
  logger?: NevoLogger
}

interface NomadServiceEntry {
  ID?: string
  ServiceName?: string
  Address?: string
  Port?: number
  Tags?: string[]
  Namespace?: string
  AllocID?: string
}

/** Reads `/v1/service/<name>` from the Nomad HTTP API. */
export class NomadDiscoveryProvider implements DiscoveryProvider {
  readonly id = "nomad"
  private timer?: NodeJS.Timeout
  private stopped = false
  private sink?: DiscoverySink
  private readonly fetcher: typeof fetch
  private readonly logger: NevoLogger

  constructor(private readonly opts: NomadDiscoveryProviderOptions) {
    if (!opts.url) throw new Error("NomadDiscoveryProvider: `url` is required")
    this.fetcher = opts.fetcher ?? fetch
    this.logger = (opts.logger ?? getDefaultLogger()).child({ component: "discovery.nomad" })
  }

  async start(sink: DiscoverySink): Promise<void> {
    this.sink = sink
    this.stopped = false
    const interval = Math.max(500, this.opts.pollIntervalMs ?? 5_000)
    const tick = async () => {
      if (this.stopped) return
      try { await this.refresh() } catch (err) {
        this.logger.warn({ event: "nomad.poll.failed", err: (err as Error)?.message })
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

  private async refresh(): Promise<void> {
    if (!this.sink) return
    const names = this.opts.serviceNames ?? (await this.resolveServiceNames())
    await Promise.all(names.map((n) => this.refreshService(n).catch(() => undefined)))
  }

  private async resolveServiceNames(): Promise<string[]> {
    const res = await this.req("/v1/services")
    const body = (await res.json()) as Array<{ Namespace: string; Services?: Array<{ ServiceName: string }> }>
    const out = new Set<string>()
    for (const ns of body) for (const s of ns.Services ?? []) out.add(s.ServiceName)
    return [...out]
  }

  private async refreshService(name: string): Promise<void> {
    if (!this.sink) return
    const res = await this.req(`/v1/service/${encodeURIComponent(name)}`)
    if (!res.ok) {
      this.sink.replace(name, [])
      return
    }
    const body = (await res.json()) as NomadServiceEntry[]
    const announcements: DiscoveryAnnouncement[] = body.map((entry) => ({
      serviceName: name,
      instanceId: entry.ID ?? entry.AllocID ?? `${entry.Address}:${entry.Port}`,
      transport: "http",
      ts: Date.now(),
      host: entry.Address,
      port: entry.Port,
      capabilities: entry.Tags,
      meta: { source: "nomad", namespace: entry.Namespace, allocId: entry.AllocID }
    }))
    this.sink.replace(name, announcements)
  }

  private async req(path: string): Promise<Response> {
    const params = new URLSearchParams()
    if (this.opts.namespace) params.set("namespace", this.opts.namespace)
    const sep = path.includes("?") ? "&" : "?"
    const url = `${this.opts.url.replace(/\/$/, "")}${path}${params.toString() ? sep + params.toString() : ""}`
    const headers: Record<string, string> = { Accept: "application/json" }
    if (this.opts.token) headers["X-Nomad-Token"] = this.opts.token
    return this.fetcher(url, { headers })
  }
}
