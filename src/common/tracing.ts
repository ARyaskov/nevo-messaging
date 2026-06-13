import { randomBytes } from "node:crypto"
import { createRequire } from "node:module"
import type { MessageMeta, TracingOptions } from "./types"

const nodeRequire = createRequire(__filename)

export interface SpanLike {
  end(): void
  setAttribute(key: string, value: string | number | boolean): void
  recordException(err: unknown): void
  setStatus(status: { code: 0 | 1 | 2; message?: string }): void
}

export interface NevoTracer {
  startSpan(name: string, attributes?: Record<string, string | number | boolean>): SpanLike
  extract(meta?: MessageMeta): unknown
  inject(meta: MessageMeta, span?: SpanLike): MessageMeta
  withSpan?<T>(
    name: string,
    attributes: Record<string, string | number | boolean> | undefined,
    parentMeta: MessageMeta | undefined,
    fn: (span: SpanLike) => Promise<T>
  ): Promise<T>
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
  startSpan(): SpanLike {
    return new NoopSpan()
  }
  extract(meta?: MessageMeta) {
    return meta?.trace
  }
  inject(meta: MessageMeta, _span?: SpanLike): MessageMeta {
    if (!this.injectTraceparent) return meta
    if (!meta.trace) {
      meta.trace = { traceparent: makeTraceparent() }
    }
    return meta
  }
  async withSpan<T>(
    _name: string,
    _attributes: Record<string, string | number | boolean> | undefined,
    _parentMeta: MessageMeta | undefined,
    fn: (span: SpanLike) => Promise<T>
  ): Promise<T> {
    return fn(new NoopSpan())
  }
}

function makeTraceparent(): string {
  const traceId = randomBytes(16).toString("hex")
  const spanId = randomBytes(8).toString("hex")
  return `00-${traceId}-${spanId}-01`
}

const OTEL_SPAN = Symbol("nevo.otelSpan")

function tryOtel(opts: TracingOptions): NevoTracer | null {
  try {
    const api = nodeRequire("@opentelemetry/api")
    const tracer = api.trace.getTracer(opts.serviceName || "nevo")
    const wrap = (span: any): SpanLike => {
      let ended = false
      const wrapped: SpanLike = {
        end: () => {
          if (!ended) {
            ended = true
            span.end()
          }
        },
        setAttribute: (k: string, v: string | number | boolean) => span.setAttribute(k, v),
        recordException: (e: unknown) => span.recordException(e as Error),
        setStatus: (s: { code: 0 | 1 | 2; message?: string }) => span.setStatus(s)
      }
      ;(wrapped as any)[OTEL_SPAN] = span
      return wrapped
    }
    const extractContext = (meta?: MessageMeta) => {
      if (!meta?.trace?.traceparent) return undefined
      const carrier = { traceparent: meta.trace.traceparent, tracestate: meta.trace.tracestate }
      return api.propagation.extract(api.context.active(), carrier)
    }
    return {
      startSpan(name, attributes) {
        return wrap(tracer.startSpan(name, { attributes }))
      },
      extract(meta) {
        return extractContext(meta)
      },
      inject(meta, span) {
        const otelSpan = (span as any)?.[OTEL_SPAN]
        const ctx = otelSpan ? api.trace.setSpan(api.context.active(), otelSpan) : api.context.active()
        const carrier: Record<string, string> = {}
        api.propagation.inject(ctx, carrier)
        return {
          ...meta,
          trace: {
            ...meta.trace,
            traceparent: carrier["traceparent"] ?? meta.trace?.traceparent ?? makeTraceparent(),
            tracestate: carrier["tracestate"] ?? meta.trace?.tracestate
          }
        }
      },
      async withSpan<T>(
        name: string,
        attributes: Record<string, string | number | boolean> | undefined,
        parentMeta: MessageMeta | undefined,
        fn: (span: SpanLike) => Promise<T>
      ): Promise<T> {
        const parentCtx = extractContext(parentMeta) ?? api.context.active()
        const span = tracer.startSpan(name, { attributes }, parentCtx)
        const wrapped = wrap(span)
        try {
          return await api.context.with(api.trace.setSpan(parentCtx, span), () => fn(wrapped))
        } finally {
          wrapped.end()
        }
      }
    }
  } catch {
    return null
  }
}

export async function runWithSpan<T>(
  tracer: NevoTracer,
  name: string,
  attributes: Record<string, string | number | boolean> | undefined,
  parentMeta: MessageMeta | undefined,
  fn: (span: SpanLike) => Promise<T>
): Promise<T> {
  if (typeof tracer.withSpan === "function") return tracer.withSpan(name, attributes, parentMeta, fn)
  const span = tracer.startSpan(name, attributes)
  try {
    return await fn(span)
  } finally {
    span.end()
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
