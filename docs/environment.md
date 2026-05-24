# Environment variables

The framework reads a small set of environment variables at process start. Everything else is set in code.

## What the framework actually reads

| Variable | Used by | Effect |
|---|---|---|
| `NODE_ENV` | env.ts | `"production"` / `"prod"` switches `IS_PROD` to true |
| `MODE` | env.ts | Alternative to `NODE_ENV` |
| `KAFKA_HOST` | Kafka client | Used to build the broker URL when `kafkaHost` not in options |
| `KAFKA_PORT` | Kafka client | Default `9092` |

That's the full list directly consumed by the framework's source.

`NODE_ENV` and `MODE` are evaluated **once at module load** via the `IS_PROD` constant in `src/common/env.ts`. To affect detection, set the variable **before** importing the framework.

```ts
process.env.NODE_ENV = "production"   // must precede the import below
import { createNevoNatsClient } from "@riaskov/nevo-messaging"
```

## Effect of `NODE_ENV=production`

- Logger level defaults to `info` (otherwise `debug`)
- Logger pretty-printing is disabled
- Health probe responses omit some diagnostic fields

## Optional, used by peer SDKs

These are read by libraries the framework loads as peers — they affect behavior if you opt into the related feature:

| Variable | Consumer |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTel exporter (when `setupNevoTracing` exporter is `"otlp"`) |
| `OTEL_EXPORTER_OTLP_HEADERS` | OTel auth headers |
| `OTEL_SERVICE_NAME` | OTel resource attribute |
| `OTEL_RESOURCE_ATTRIBUTES` | OTel resource attributes |

See [observability.md](./observability.md) for the explicit options object.

## Conventions used by examples

Examples in `examples/` honor a few variables for convenience; these are read by the example code, not the framework:

- `NATS_SERVERS` — comma-separated list
- `HTTP_COORDINATOR` — coordinator URL for HTTP transport
- `LOG_LEVEL` — passed to `createLogger`

If you need the framework to pick up an environment variable, read it in your bootstrap code and pass it as an option. The framework deliberately keeps env access narrow so behavior is predictable across deployments.
