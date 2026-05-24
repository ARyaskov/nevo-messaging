# OpenTelemetry tracing

Nevo can emit OpenTelemetry spans for every `query`, `emit`, `publish`, and subscribe handler. Trace context propagates across transports via the W3C `traceparent` field on the envelope `meta`.

## Setup

```ts
import { setupNevoTracing } from "@riaskov/nevo-messaging"

const tracing = await setupNevoTracing({
  serviceName: "user",
  serviceVersion: "2.0.0",
  exporter: "otlp",                                    // "otlp" | "jaeger" | "zipkin" | "console" | "none"
  endpoint: "http://otel-collector:4318/v1/traces",
  headers: { authorization: "Bearer ..." },
  protocol: "http/protobuf",                            // for OTLP/HTTP
  sampleRate: 0.1                                       // 0..1, default: 1.0
})

// On shutdown:
await tracing.shutdown()
```

Call once during process bootstrap (before module init).

## Exporters

| Exporter | Required peer dep |
|---|---|
| `"otlp"` | `@opentelemetry/sdk-trace-node` + `@opentelemetry/exporter-trace-otlp-http` |
| `"jaeger"` | `@opentelemetry/exporter-jaeger` |
| `"zipkin"` | `@opentelemetry/exporter-zipkin` |
| `"console"` | none (built-in console exporter) |
| `"none"` | disables export but keeps span creation |

The SDK packages are loaded dynamically — install only the ones you use.

## What gets traced automatically

When `setupNevoTracing` returns successfully, the framework starts span creation around:

- Every outbound `query()` / `emit()` / `publish()` / `broadcast()`
- Every inbound dispatch (handler invocation)
- Retry attempts (each becomes a child span event)
- Compression / decompression boundaries

Common span attributes:

- `service`, `method`
- `transport` (`"nats"` / `"kafka"` / `"http"` / ...)
- `client.id` and `instance.id`
- `outcome` (`"ok"` / `"error"`)
- `error.code` when applicable

## Manual spans

Add custom spans via the OTel API:

```ts
import { trace } from "@opentelemetry/api"

async handle(input) {
  const tracer = trace.getTracer("user")
  await tracer.startActiveSpan("compute-pricing", async (span) => {
    try {
      span.setAttribute("input.size", input.length)
      const out = await compute(input)
      span.end()
      return out
    } catch (err) {
      span.recordException(err)
      span.end()
      throw err
    }
  })
}
```

These spans nest correctly under the auto-generated request span when called from inside a handler.

## Propagation

The framework injects `traceparent` (and `tracestate` when present) into envelope `meta` on outbound calls, and seeds an active span on the receiving side. This works:

- Across transports (NATS → Kafka → HTTP)
- Through retry, hedging, outbox replay
- Across language boundaries (peers using the W3C standard)

## Sampling

`sampleRate: 0.1` keeps 10% of traces. Pair with a tail-based sampler in your OTel Collector to keep all errored and slow spans.

For high-volume services, lower the head sample to `0.01` or below and rely on the collector's tail sampler.

## Logs correlation

The framework attaches `trace_id` and `span_id` to every log entry while a trace is active. Configure Loki / Elastic / your TSDB to index those fields — clicking a log line in Grafana opens the corresponding trace.

## What is not provided

- **`setupOtel` does not exist** — the function is `setupNevoTracing`.
- **No automatic metrics export to OTLP.** Metrics are pull-only via [`/metrics`](./metrics.md). For OTel metrics, instantiate the SDK separately.
- **No built-in tail sampler.** Use the OpenTelemetry Collector for that.

## See also

- [metrics.md](./metrics.md) — separate registry
- [logger.md](./logger.md) — trace-id correlation
- [devtools.md](./devtools.md) — in-process trace view
