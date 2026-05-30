import type { MetricsOptions } from "./types"
import { parseMethod } from "./version"
import { getDefaultLogger } from "./logger"

export interface MetricsRegistry {
  incCounter(name: string, labels: Record<string, string>, value?: number): void
  observeHistogram(name: string, labels: Record<string, string>, value: number, buckets?: number[]): void
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

// Sentinel for any method that is not registered (or is attacker-supplied on a
// METHOD_NOT_FOUND). Bucketing here keeps random method names from minting new
// time series. See `methodLabel`.
export const UNKNOWN_METHOD_LABEL = "<unknown>"

// Sentinel label-set every metric collapses into once it exceeds the series
// cap, so a cardinality attack cannot grow the backing Maps without bound.
const OVERFLOW_LABEL_VALUE = "<other>"

// Default request-latency buckets (seconds). Extended past the old 10s ceiling
// to 30s/60s so slow requests stay visible instead of collapsing into +Inf.
const DEFAULT_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60]

const DEFAULT_MAX_SERIES_PER_METRIC = 2000

/**
 * Normalise a raw (possibly version-suffixed, possibly attacker-supplied) method
 * string into a safe, low-cardinality metric label. The `@version` suffix is
 * stripped, and — when an `isKnown` predicate is supplied — any method the
 * server did not register collapses to {@link UNKNOWN_METHOD_LABEL}. Callers
 * without a registry (clients) pass no predicate and just get the version
 * stripped.
 */
export function methodLabel(method: string | undefined | null, isKnown?: (name: string) => boolean): string {
  if (!method) return UNKNOWN_METHOD_LABEL
  const { name } = parseMethod(method)
  if (!name) return UNKNOWN_METHOD_LABEL
  if (isKnown && !isKnown(name)) return UNKNOWN_METHOD_LABEL
  return name
}

// Prometheus text-format label value: wrapped in double quotes with `\`, `"`
// and newline escaped. Other control characters are dropped so a crafted label
// (e.g. a forged method name on METHOD_NOT_FOUND) cannot inject exposition
// lines or otherwise corrupt the output.
function escapeLabelValue(value: string): string {
  let out = ""
  for (const ch of value) {
    if (ch === "\\") out += "\\\\"
    else if (ch === "\"") out += "\\\""
    else if (ch === "\n") out += "\\n"
    else {
      const code = ch.charCodeAt(0)
      if (code < 0x20 || code === 0x7f) continue
      out += ch
    }
  }
  return out
}

// Produces the fully-rendered, escaped, quoted label portion (e.g.
// `method="ping",service="user"`). This doubles as the dedup key for a series
// and as the exact text written by `expose()` — escaping happens exactly once,
// here, so the two can never drift out of sync. Label *names* are framework
// constants (always valid identifiers) so only values need escaping.
function labelsKey(labels: Record<string, string>): string {
  const keys = Object.keys(labels).sort()
  return keys.map((k) => `${k}="${escapeLabelValue(labels[k])}"`).join(",")
}

const OVERFLOW_KEY = labelsKey({ label: OVERFLOW_LABEL_VALUE })

// Bucket boundaries must be ascending and unique for the bucket search to be
// correct; normalise defensively in case a caller passes an unsorted set.
function normalizeBuckets(buckets: number[]): number[] {
  return Array.from(new Set(buckets.filter((b) => Number.isFinite(b)))).sort((a, b) => a - b)
}

function sameBuckets(a: number[], b: number[]): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false
  return true
}

export class InMemoryMetrics implements MetricsRegistry {
  private readonly counters = new Map<string, CounterStore>()
  private readonly histograms = new Map<string, HistogramStore>()
  private readonly gauges = new Map<string, GaugeStore>()
  private readonly enabled: boolean
  private readonly maxSeries: number
  private readonly bucketConfig: Record<string, number[]>
  private readonly bucketWarned = new Set<string>()

  constructor(opts?: MetricsOptions) {
    this.enabled = opts?.enabled !== false
    const cap = opts?.maxSeriesPerMetric
    this.maxSeries = typeof cap === "number" && cap > 0 ? Math.floor(cap) : DEFAULT_MAX_SERIES_PER_METRIC
    const cfg: Record<string, number[]> = {}
    if (opts?.buckets) {
      for (const [name, b] of Object.entries(opts.buckets)) {
        if (Array.isArray(b) && b.length > 0) cfg[name] = normalizeBuckets(b)
      }
    }
    this.bucketConfig = cfg
  }

  isEnabled(): boolean { return this.enabled }

  // Resolve the label-set key, funnelling any *new* series past the cap into the
  // single overflow bucket. Existing keys (including the overflow key itself)
  // always pass through, so a metric retains at most `maxSeries` real series
  // plus the one `{label="<other>"}` catch-all.
  private resolveKey(map: ReadonlyMap<string, unknown>, key: string): string {
    if (map.has(key)) return key
    if (map.size < this.maxSeries) return key
    return OVERFLOW_KEY
  }

  incCounter(name: string, labels: Record<string, string>, value = 1): void {
    if (!this.enabled) return
    let store = this.counters.get(name)
    if (!store) {
      store = { values: new Map() }
      this.counters.set(name, store)
    }
    const key = this.resolveKey(store.values, labelsKey(labels))
    store.values.set(key, (store.values.get(key) ?? 0) + value)
  }

  observeHistogram(name: string, labels: Record<string, string>, value: number, buckets?: number[]): void {
    if (!this.enabled) return
    let store = this.histograms.get(name)
    if (!store) {
      // Bucket precedence: explicit per-metric config > buckets passed on the
      // first observation > the extended latency defaults.
      const initial = this.bucketConfig[name] ?? (buckets ? normalizeBuckets(buckets) : DEFAULT_BUCKETS)
      store = { buckets: initial, counts: new Map(), sums: new Map(), totals: new Map() }
      this.histograms.set(name, store)
    } else if (buckets && !sameBuckets(normalizeBuckets(buckets), store.buckets) && !this.bucketWarned.has(name)) {
      // Buckets are fixed at first observation; a later mismatch is surfaced
      // (once per metric) rather than silently ignored.
      this.bucketWarned.add(name)
      getDefaultLogger().warn(
        { event: "metrics.bucket_mismatch", metric: name, requested: buckets, active: store.buckets },
        "Histogram bucket-set mismatch; keeping the buckets from the first observation"
      )
    }
    const key = this.resolveKey(store.totals, labelsKey(labels))
    // The overflow bucket aggregates many distinct label-sets, so a per-`le`
    // distribution would be meaningless — track only its sum/count.
    if (key !== OVERFLOW_KEY) {
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
    }
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
    const key = this.resolveKey(store.values, labelsKey(labels))
    store.values.set(key, value)
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
      // `totals` holds every observed key (incl. the overflow key, which has no
      // per-bucket counts) so iterate it as the canonical series set.
      for (const [labels, total] of store.totals.entries()) {
        const sum = store.sums.get(labels) ?? 0
        if (labels === OVERFLOW_KEY) {
          lines.push(`${name}_sum{${labels}} ${sum}`)
          lines.push(`${name}_count{${labels}} ${total}`)
          continue
        }
        const counts = store.counts.get(labels) ?? new Array(store.buckets.length).fill(0)
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
  payloadBytes: "nevo_messaging_payload_bytes",
  // High-severity: a distributed idempotency / inbox store read failed. Labelled
  // by `{ store, op, policy }`. Spikes here mean dedup may have degraded (and,
  // under fail-open, that handlers may have re-executed).
  storeErrors: "nevo_messaging_store_errors_total",
  // High-severity: a saga compensation step exhausted its retries and threw.
  // Labelled by `{ type, step }`. The saga is left `compensation_failed` (never
  // `compensated`) and routed to the DLQ — a non-zero rate means side effects
  // may not have been rolled back and need manual intervention.
  sagaCompensationFailures: "nevo_messaging_saga_compensation_failures_total"
}
