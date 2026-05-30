import { createRequire } from "node:module"
import { createTracer, setDefaultTracer, NevoTracer } from "./tracing"

const nodeRequire = createRequire(__filename)

export interface NevoTracingSetupOptions {
  serviceName: string
  serviceVersion?: string
  exporter?: "otlp" | "jaeger" | "zipkin" | "console" | "none"
  endpoint?: string
  headers?: Record<string, string>
  protocol?: "http/protobuf" | "http/json" | "grpc"
  sampleRate?: number
}

export interface NevoTracingHandle {
  tracer: NevoTracer
  shutdown(): Promise<void>
}

export async function setupNevoTracing(opts: NevoTracingSetupOptions): Promise<NevoTracingHandle> {
  if (opts.exporter === "none" || !opts.exporter) {
    const tracer = createTracer({ serviceName: opts.serviceName, enabled: false })
    setDefaultTracer(tracer)
    return { tracer, shutdown: async () => {} }
  }

  let provider: any = null
  let processor: any = null

  try {
    const sdkTrace: any = nodeRequire("@opentelemetry/sdk-trace-node")
    const resources: any = nodeRequire("@opentelemetry/resources")
    const semconv: any = nodeRequire("@opentelemetry/semantic-conventions")
    const tracingApi: any = nodeRequire("@opentelemetry/api")

    const attrs: Record<string, string> = {}
    attrs[semconv.SemanticResourceAttributes?.SERVICE_NAME ?? "service.name"] = opts.serviceName
    if (opts.serviceVersion) attrs[semconv.SemanticResourceAttributes?.SERVICE_VERSION ?? "service.version"] = opts.serviceVersion
    const resource = new resources.Resource(attrs)

    let exporter: any
    if (opts.exporter === "otlp") {
      try {
        const otlp: any = nodeRequire("@opentelemetry/exporter-trace-otlp-http")
        exporter = new otlp.OTLPTraceExporter({
          url: opts.endpoint ?? "http://localhost:4318/v1/traces",
          headers: opts.headers
        })
      } catch (err: any) {
        throw new Error(`OTLP exporter requires @opentelemetry/exporter-trace-otlp-http: ${err?.message}`)
      }
    } else if (opts.exporter === "jaeger") {
      try {
        const jaeger = nodeRequire("@opentelemetry/exporter-jaeger") as any
        exporter = new jaeger.JaegerExporter({ endpoint: opts.endpoint ?? "http://localhost:14268/api/traces" })
      } catch (err: any) {
        throw new Error(`Jaeger exporter requires @opentelemetry/exporter-jaeger: ${err?.message}`)
      }
    } else if (opts.exporter === "zipkin") {
      try {
        const zipkin = nodeRequire("@opentelemetry/exporter-zipkin") as any
        exporter = new zipkin.ZipkinExporter({ url: opts.endpoint ?? "http://localhost:9411/api/v2/spans", serviceName: opts.serviceName })
      } catch (err: any) {
        throw new Error(`Zipkin exporter requires @opentelemetry/exporter-zipkin: ${err?.message}`)
      }
    } else if (opts.exporter === "console") {
      exporter = new sdkTrace.ConsoleSpanExporter()
    }

    provider = new sdkTrace.NodeTracerProvider({
      resource,
      sampler: opts.sampleRate !== undefined
        ? new sdkTrace.TraceIdRatioBasedSampler(Math.max(0, Math.min(1, opts.sampleRate)))
        : new sdkTrace.AlwaysOnSampler()
    })
    processor = new sdkTrace.BatchSpanProcessor(exporter)
    provider.addSpanProcessor(processor)
    provider.register()

    void tracingApi
  } catch (err: any) {
    throw new Error(
      `Failed to initialize OpenTelemetry tracing: ${err?.message ?? err}. ` +
      `Install @opentelemetry/api @opentelemetry/sdk-trace-node @opentelemetry/resources @opentelemetry/semantic-conventions plus your exporter.`
    )
  }

  const tracer = createTracer({ serviceName: opts.serviceName, enabled: true })
  setDefaultTracer(tracer)

  return {
    tracer,
    shutdown: async () => {
      try { await processor?.shutdown() } catch {}
      try { await provider?.shutdown() } catch {}
    }
  }
}
