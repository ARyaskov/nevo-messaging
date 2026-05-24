import type { MetricsOptions } from "./types"

export interface MetricsRegistry {
  incCounter(name: string, labels: Record<string, string>, value?: number): void
  observeHistogram(name: string, labels: Record<string, string>, value: number): void
  setGauge(name: string, labels: Record<string, string>, value: number): void
  expose(): string | Promise<string>
}

interface CounterStore {
  values: Map<string, number>
}

interface HistogramStore {
  buckets: number[]
  counts: Map<string, number[]>
  sums: Map<string, number>
  totals: Map<string, number>
}

interface GaugeStore {
  values: Map<string, number>
}

function labelsKey(labels: Record<string, string>): string {
  const keys = Object.keys(labels).sort()
  return keys.map((k) => `${k}=${labels[k]}`).join(",")
}

const DEFAULT_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]

export class InMemoryMetrics implements MetricsRegistry {
  private readonly counters = new Map<string, CounterStore>()
  private readonly histograms = new Map<string, HistogramStore>()
  private readonly gauges = new Map<string, GaugeStore>()
  private readonly enabled: boolean

  constructor(opts?: MetricsOptions) {
    this.enabled = opts?.enabled !== false
  }

  isEnabled(): boolean { return this.enabled }

  incCounter(name: string, labels: Record<string, string>, value = 1): void {
    if (!this.enabled) return
    let store = this.counters.get(name)
    if (!store) {
      store = { values: new Map() }
      this.counters.set(name, store)
    }
    const key = labelsKey(labels)
    store.values.set(key, (store.values.get(key) ?? 0) + value)
  }

  observeHistogram(name: string, labels: Record<string, string>, value: number, buckets: number[] = DEFAULT_BUCKETS): void {
    if (!this.enabled) return
    let store = this.histograms.get(name)
    if (!store) {
      store = { buckets, counts: new Map(), sums: new Map(), totals: new Map() }
      this.histograms.set(name, store)
    }
    const key = labelsKey(labels)
    let arr = store.counts.get(key)
    if (!arr) {
      arr = new Array(store.buckets.length).fill(0)
      store.counts.set(key, arr)
    }
    const b = store.buckets
    let lo = 0
    let hi = b.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (b[mid] < value) lo = mid + 1
      else hi = mid
    }
    for (let i = lo; i < b.length; i++) arr[i]++
    store.sums.set(key, (store.sums.get(key) ?? 0) + value)
    store.totals.set(key, (store.totals.get(key) ?? 0) + 1)
  }

  setGauge(name: string, labels: Record<string, string>, value: number): void {
    if (!this.enabled) return
    let store = this.gauges.get(name)
    if (!store) {
      store = { values: new Map() }
      this.gauges.set(name, store)
    }
    store.values.set(labelsKey(labels), value)
  }

  expose(): string {
    const lines: string[] = []
    for (const [name, store] of this.counters.entries()) {
      lines.push(`# TYPE ${name} counter`)
      for (const [labels, val] of store.values.entries()) {
        lines.push(`${name}{${labels}} ${val}`)
      }
    }
    for (const [name, store] of this.gauges.entries()) {
      lines.push(`# TYPE ${name} gauge`)
      for (const [labels, val] of store.values.entries()) {
        lines.push(`${name}{${labels}} ${val}`)
      }
    }
    for (const [name, store] of this.histograms.entries()) {
      lines.push(`# TYPE ${name} histogram`)
      for (const [labels, counts] of store.counts.entries()) {
        const sum = store.sums.get(labels) ?? 0
        const total = store.totals.get(labels) ?? 0
        for (let i = 0; i < store.buckets.length; i++) {
          const lab = labels ? `${labels},le="${store.buckets[i]}"` : `le="${store.buckets[i]}"`
          lines.push(`${name}_bucket{${lab}} ${counts[i]}`)
        }
        const labInf = labels ? `${labels},le="+Inf"` : `le="+Inf"`
        lines.push(`${name}_bucket{${labInf}} ${total}`)
        lines.push(`${name}_sum{${labels}} ${sum}`)
        lines.push(`${name}_count{${labels}} ${total}`)
      }
    }
    return lines.join("\n") + "\n"
  }
}

let defaultMetrics: MetricsRegistry | null = null
export function getDefaultMetrics(): MetricsRegistry {
  if (!defaultMetrics) defaultMetrics = new InMemoryMetrics()
  return defaultMetrics
}
export function setDefaultMetrics(metrics: MetricsRegistry): void {
  defaultMetrics = metrics
}

export const NEVO_METRIC_NAMES = {
  requestsTotal: "nevo_messaging_requests_total",
  requestErrors: "nevo_messaging_request_errors_total",
  requestDuration: "nevo_messaging_request_duration_seconds",
  inflight: "nevo_messaging_inflight",
  retries: "nevo_messaging_retries_total",
  circuitState: "nevo_messaging_circuit_state",
  payloadBytes: "nevo_messaging_payload_bytes"
}
