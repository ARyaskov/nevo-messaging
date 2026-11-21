import { createRequire } from "node:module"
import { createTracer, setDefaultTracer, NevoTracer } from "./tracing"

const nodeRequire = createRequire(__filename)

function resolveLegacyTracingSdk(): string | undefined {
  try {
    return nodeRequire.resolve("@opentelemetry/sdk-trace-node")
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "MODULE_NOT_FOUND") return undefined
    throw err
  }
}

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
  let releaseGlobals = () => {}

  try {
    const legacy = resolveLegacyTracingSdk()
    if (opts.exporter === "jaeger" && !legacy) throw new Error("The Jaeger exporter requires sdk-trace-node")
    const modern = !legacy
    const sdkTrace = nodeRequire(legacy ?? "@opentelemetry/sdk-trace")
    const resources: any = nodeRequire("@opentelemetry/resources")
    const semconv: any = nodeRequire("@opentelemetry/semantic-conventions")
    const tracingApi: any = nodeRequire("@opentelemetry/api")

    const attrs: Record<string, string> = {}
    attrs[semconv.SemanticResourceAttributes?.SERVICE_NAME ?? "service.name"] = opts.serviceName
    if (opts.serviceVersion) attrs[semconv.SemanticResourceAttributes?.SERVICE_VERSION ?? "service.version"] = opts.serviceVersion
    const resource = typeof resources.resourceFromAttributes === "function" ? resources.resourceFromAttributes(attrs) : new resources.Resource(attrs)

    let exporter: any
    if (opts.exporter === "otlp") {
      try {
        const otlp: any = nodeRequire("@opentelemetry/exporter-trace-otlp-http")
        exporter = new otlp.OTLPTraceExporter({
          url: opts.endpoint ?? "http://localhost:4318/v1/traces",
          headers: opts.headers
        })
      } catch (err: any) {
        throw new Error(`OTLP exporter requires @opentelemetry/exporter-trace-otlp-http: ${err?.message}`, { cause: err })
      }
    } else if (opts.exporter === "jaeger") {
      try {
        const jaeger = nodeRequire("@opentelemetry/exporter-jaeger") as any
        exporter = new jaeger.JaegerExporter({ endpoint: opts.endpoint ?? "http://localhost:14268/api/traces" })
      } catch (err: any) {
        throw new Error(`Jaeger exporter requires @opentelemetry/exporter-jaeger: ${err?.message}`, { cause: err })
      }
    } else if (opts.exporter === "zipkin") {
      try {
        const zipkin = nodeRequire("@opentelemetry/exporter-zipkin") as any
        exporter = new zipkin.ZipkinExporter({ url: opts.endpoint ?? "http://localhost:9411/api/v2/spans", serviceName: opts.serviceName })
      } catch (err: any) {
        throw new Error(`Zipkin exporter requires @opentelemetry/exporter-zipkin: ${err?.message}`, { cause: err })
      }
    } else if (opts.exporter === "console") {
      exporter = new sdkTrace.ConsoleSpanExporter()
    }

    const processor = modern ? new sdkTrace.BatchSpanProcessor({ exporter }) : new sdkTrace.BatchSpanProcessor(exporter)
    const providerOpts = {
      resource,
      sampler:
        opts.sampleRate !== undefined
          ? new sdkTrace.TraceIdRatioBasedSampler(Math.max(0, Math.min(1, opts.sampleRate)))
          : new sdkTrace.AlwaysOnSampler()
    }
    if (modern) {
      const { AsyncLocalStorageContextManager } = nodeRequire("@opentelemetry/context-async-hooks")
      const { CompositePropagator, W3CBaggagePropagator, W3CTraceContextPropagator } = nodeRequire("@opentelemetry/core")
      const manager = new AsyncLocalStorageContextManager().enable()
      const propagator = new CompositePropagator({ propagators: [new W3CTraceContextPropagator(), new W3CBaggagePropagator()] })
      provider = new sdkTrace.TracerProvider({ ...providerOpts, spanProcessors: [processor] })
      const ownsTrace = tracingApi.trace.setGlobalTracerProvider(provider)
      const ownsContext = tracingApi.context.setGlobalContextManager(manager)
      const ownsPropagation = tracingApi.propagation.setGlobalPropagator(propagator)
      if (!ownsContext) manager.disable()
      releaseGlobals = () => {
        if (ownsTrace) tracingApi.trace.disable()
        if (ownsContext) tracingApi.context.disable()
        if (ownsPropagation) tracingApi.propagation.disable()
      }
    } else if (typeof sdkTrace.NodeTracerProvider.prototype.addSpanProcessor === "function") {
      provider = new sdkTrace.NodeTracerProvider(providerOpts)
      provider.addSpanProcessor(processor)
      provider.register()
    } else {
      provider = new sdkTrace.NodeTracerProvider({ ...providerOpts, spanProcessors: [processor] })
      provider.register()
    }
  } catch (err: any) {
    const install =
      opts.exporter === "jaeger"
        ? "Install the legacy OpenTelemetry SDK and Jaeger exporter, or use the OTLP exporter for Jaeger."
        : "Install @opentelemetry/api, @opentelemetry/sdk-trace, @opentelemetry/resources, @opentelemetry/context-async-hooks, @opentelemetry/core, @opentelemetry/semantic-conventions and your exporter."
    throw new Error(`Failed to initialize OpenTelemetry tracing: ${err?.message ?? err}. ${install}`, { cause: err })
  }

  const tracer = createTracer({ serviceName: opts.serviceName, enabled: true })
  setDefaultTracer(tracer)
  let shutdown: Promise<void> | undefined

  return {
    tracer,
    shutdown: () =>
      (shutdown ??= Promise.resolve()
        .then(() => provider.shutdown())
        .catch(() => {})
        .finally(releaseGlobals))
  }
}
