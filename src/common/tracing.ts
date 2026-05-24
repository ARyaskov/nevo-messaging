import { randomBytes } from "node:crypto"
import type { MessageMeta, TracingOptions } from "./types"

export interface SpanLike {
  end(): void
  setAttribute(key: string, value: string | number | boolean): void
  recordException(err: unknown): void
  setStatus(status: { code: 0 | 1 | 2; message?: string }): void
}

export interface NevoTracer {
  startSpan(name: string, attributes?: Record<string, string | number | boolean>): SpanLike
  extract(meta?: MessageMeta): unknown
  inject(meta: MessageMeta): MessageMeta
}

class NoopSpan implements SpanLike {
  end() {}
  setAttribute() {}
  recordException() {}
  setStatus() {}
}

class FallbackTracer implements NevoTracer {
  private readonly serviceName: string
  private readonly injectTraceparent: boolean
  constructor(opts: TracingOptions) {
    this.serviceName = opts.serviceName || "nevo"
    this.injectTraceparent = opts.enabled === true
  }
  startSpan(): SpanLike { return new NoopSpan() }
  extract(meta?: MessageMeta) { return meta?.trace }
  inject(meta: MessageMeta): MessageMeta {
    if (!this.injectTraceparent) return meta
    if (!meta.trace) {
      meta.trace = { traceparent: makeTraceparent() }
    }
    return meta
  }
}

function makeTraceparent(): string {
  const traceId = randomBytes(16).toString("hex")
  const spanId = randomBytes(8).toString("hex")
  return `00-${traceId}-${spanId}-01`
}

function tryOtel(opts: TracingOptions): NevoTracer | null {
  try {
    const api = require("@opentelemetry/api")
    const tracer = api.trace.getTracer(opts.serviceName || "nevo")
    return {
      startSpan(name, attributes) {
        const span = tracer.startSpan(name, { attributes })
        return {
          end: () => span.end(),
          setAttribute: (k, v) => span.setAttribute(k, v),
          recordException: (e) => span.recordException(e as Error),
          setStatus: (s) => span.setStatus(s)
        }
      },
      extract(meta) {
        if (!meta?.trace?.traceparent) return undefined
        const carrier = { traceparent: meta.trace.traceparent, tracestate: meta.trace.tracestate }
        return api.propagation.extract(api.context.active(), carrier)
      },
      inject(meta) {
        const carrier: Record<string, string> = {}
        api.propagation.inject(api.context.active(), carrier)
        return {
          ...meta,
          trace: {
            ...meta.trace,
            traceparent: carrier["traceparent"] ?? meta.trace?.traceparent ?? makeTraceparent(),
            tracestate: carrier["tracestate"] ?? meta.trace?.tracestate
          }
        }
      }
    }
  } catch {
    return null
  }
}

let defaultTracer: NevoTracer | null = null

export function createTracer(opts?: TracingOptions): NevoTracer {
  const cfg: TracingOptions = { enabled: opts?.enabled === true, serviceName: "nevo", ...opts }
  if (!cfg.enabled) return new FallbackTracer(cfg)
  return tryOtel(cfg) || new FallbackTracer(cfg)
}

export function getDefaultTracer(): NevoTracer {
  if (!defaultTracer) defaultTracer = createTracer()
  return defaultTracer
}

export function setDefaultTracer(tracer: NevoTracer): void {
  defaultTracer = tracer
}
