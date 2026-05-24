# Metrics

Nevo ships a small in-memory metrics registry with a Prometheus text exporter. Counters and histograms cover the request lifecycle and key cross-cutting features.

## API

```ts
interface MetricsRegistry {
  incCounter(name: string, value?: number, labels?: Record<string, string>): void
  observeHistogram(name: string, value: number, labels?: Record<string, string>): void
  setGauge(name: string, value: number, labels?: Record<string, string>): void
  expose(): string | Promise<string>   // Prometheus text format
}

import {
  InMemoryMetrics,
  getDefaultMetrics,
  setDefaultMetrics,
  NEVO_METRIC_NAMES
} from "@riaskov/nevo-messaging"
```

`InMemoryMetrics` is the default implementation. Swap with `setDefaultMetrics(...)` if you want a different backend (e.g. forward to `prom-client`).

## Built-in metric names

```ts
NEVO_METRIC_NAMES = {
  requestsTotal:    "nevo_messaging_requests_total",
  requestErrors:    "nevo_messaging_request_errors_total",
  requestDuration:  "nevo_messaging_request_duration_seconds",
  inflight:         "nevo_messaging_inflight",
  retries:          "nevo_messaging_retries_total",
  circuitState:     "nevo_messaging_circuit_state",
  payloadBytes:     "nevo_messaging_payload_bytes"
}
```

These are the names emitted automatically by the framework. You can emit additional metrics under your own names.

## Exposing on `/metrics`

```ts
import { Controller, Get } from "@nestjs/common"
import { getDefaultMetrics } from "@riaskov/nevo-messaging"

@Controller()
export class MetricsController {
  @Get("/metrics")
  async scrape() {
    return getDefaultMetrics().expose()
  }
}
```

`expose()` returns Prometheus text format directly. No additional headers required for standard scrapers.

## Custom metrics

Use the same registry:

```ts
const reg = getDefaultMetrics()

reg.incCounter("orders_placed_total", 1, { region: "eu-west" })
reg.observeHistogram("checkout_seconds", elapsed, { tier: "paid" })
reg.setGauge("orders_in_queue", queue.length)
```

There is no "registerMetric" call — the registry creates series on first observation.

## Cardinality discipline

Don't use unbounded values as label values. Common mistakes:

- `userId` or `uuid` as a label → millions of series
- `path` with trailing identifiers (`/users/123`) → one series per ID
- Free-form `error.message` → one series per error string

Prefer enumerated labels: `tier: "free" | "paid"`, `outcome: "ok" | "error" | "timeout"`.

## OTel export

The metrics registry is independent of OpenTelemetry. If you prefer OTel metrics, instantiate the OTel SDK separately and emit through it. The framework does not bridge the two automatically.

## What is not provided

- **No latency histogram bucket configuration knob** on the default registry. The default buckets are sensible for service-to-service RPC.
- **No native push-to-OTLP** for metrics. Tracing has [`setupNevoTracing`](./observability.md); metrics are pull-only via `/metrics`.

## See also

- [observability.md](./observability.md) — OTel tracing setup
- [devtools.md](./devtools.md) — live dashboard reading the same data
- [error-codes.md](./error-codes.md) — `outcome` label values
