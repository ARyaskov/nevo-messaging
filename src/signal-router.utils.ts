import { Type } from "@nestjs/common"
import {
  BeforeHook,
  AfterHook,
  AccessControlConfig,
  MessageMeta,
  MessageResponse,
  IdempotencyOptions,
  SecurityOptions,
  MetricsOptions,
  TracingOptions
} from "./common"
import { IS_PROD } from "./common/env"
import { ErrorCode } from "./common"
import { getClassSignals, getNevoServiceName, SignalMetadata } from "./signal.decorator"
import { suggestClosestMethod } from "./common/levenshtein"
import { getDefaultLogger, NevoLogger } from "./common/logger"
import { TwoTierIdempotency } from "./common/idempotency-runtime"
import type { IdempotencyStore } from "./common/idempotency-store"
import { AuditLog } from "./common/audit-log"
import { ReplayGuard } from "./common/replay-protection"
import { getSchemaFor, toValidator } from "./common/schema"
import { parseMethod, isVersionCompatible, DEFAULT_METHOD_VERSION } from "./common/version"
import { getDefaultMetrics, methodLabel } from "./common/metrics"
import { getDefaultTracer, runWithSpan, SpanLike } from "./common/tracing"
import { matchesFilter } from "./common/subscription-filters"
import { DlqRouter } from "./common/dlq"
import { MessagingError } from "./common/errors"
import { RateLimiter, resolveRateLimiter, RateLimiterOptions } from "./common/rate-limit"
import { NEVO_CONTRACT_METHOD, ContractMethodDescriptor } from "./common/contract"
import { NEVO_HEALTH_METHOD, NEVO_LIVENESS_METHOD, NEVO_READINESS_METHOD, HealthRegistry } from "./common/health"
import { getDevToolsBus, DevToolsBus } from "./common/devtools"
import { getDevToolsRegistry, describeMethodsFromSignals } from "./common/devtools-registry"
import { getMethodRateLimit, getMethodCacheable, rateLimitToOptions } from "./common/method-decorators"
import { LruIdempotencyCache as LruCache } from "./common/idempotency"
import { runInChain, resolveInboundChainId } from "./common/chain-context"
import { readMethodResilience, applyResilience } from "./common/resilience-runtime"
import { runDispatchPipeline, type DispatchPipelineConfig, type DispatchStrategyResult } from "./common/dispatch-pipeline"

export interface SignalRouterOptions {
  before?: BeforeHook
  after?: AfterHook
  debug?: boolean
  eventPattern?: string
  serviceName?: string
  accessControl?: AccessControlConfig
  idempotency?: IdempotencyOptions
  /** Optional distributed idempotency backend (Redis, …). */
  idempotencyStore?: IdempotencyStore<MessageResponse>
  /** Optional append-only audit log. */
  auditLog?: AuditLog
  security?: SecurityOptions
  metrics?: MetricsOptions
  tracing?: TracingOptions
  logger?: NevoLogger
  dlq?: DlqRouter | { enabled?: boolean }
  defaultVersion?: string
  rateLimit?: RateLimiterOptions | RateLimiter
  health?: HealthRegistry
  serviceVersion?: string
  capabilities?: string[]
  instanceId?: string
  devtools?: DevToolsBus | boolean
  disableBuiltinHandlers?: boolean
}

export interface MessageData {
  method: string
  params: any
  uuid: string
  meta?: MessageMeta
}

export type MessageExtractor = (data: any) => MessageData

export function findPropertyByType(obj: any, type: Type<any>): string | null {
  for (const prop in obj) {
    if (obj[prop] instanceof type) {
      return prop
    }
  }
  return null
}

export function findServiceInstances(instance: any, serviceType: Type<any> | Type<any>[]): any[] {
  const types = Array.isArray(serviceType) ? serviceType : [serviceType]
  const out: any[] = []
  for (const type of types) {
    const propName = findPropertyByType(instance, type)
    if (propName && instance[propName]) {
      out.push(instance[propName])
    }
  }
  return out
}

export function createErrorResponse(message: string, uuid?: string, method?: string, code: number = ErrorCode.UNKNOWN, meta?: MessageMeta) {
  return {
    uuid,
    method,
    params: {
      result: "error",
      error: { message, code }
    },
    meta
  }
}

const DEFAULT_CACHE_KEY_MAX_CHARS = 1024

// Non-cryptographic 64-bit FNV-1a digest (two 32-bit lanes); not for security.
function fnv1a64Hex(s: string): string {
  let h1 = 0x811c9dc5
  let h2 = 0xcbf29ce4
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i)
    h1 ^= c
    h1 = (h1 + ((h1 << 1) + (h1 << 4) + (h1 << 7) + (h1 << 8) + (h1 << 24))) >>> 0
    h2 ^= (c >>> 8) ^ (c & 0xff)
    h2 = (h2 + ((h2 << 1) + (h2 << 4) + (h2 << 7) + (h2 << 8) + (h2 << 24))) >>> 0
  }
  return h1.toString(16).padStart(8, "0") + h2.toString(16).padStart(8, "0")
}

// Builds the default cache key; large params collapse to a length+digest form.
function buildDefaultCacheKey(name: string, processedParams: unknown): string {
  const serialized = JSON.stringify(processedParams ?? {})
  if (serialized.length <= DEFAULT_CACHE_KEY_MAX_CHARS) return `${name}::${serialized}`
  return `${name}::#${serialized.length}:${fnv1a64Hex(serialized)}`
}

function deriveServiceName(target: any, options: SignalRouterOptions | undefined): string {
  if (options?.serviceName) return options.serviceName
  if (options?.eventPattern) return options.eventPattern.replace(/-events$/, "")
  const fromMeta = getNevoServiceName(target)
  if (fromMeta) return fromMeta
  const className = (target?.name as string | undefined) ?? "unknown"
  const lower = className.toLowerCase()
  if (lower.endsWith("controller")) return lower.slice(0, -"controller".length) || lower
  return lower
}

export function createSignalRouterDecorator(
  serviceType: Type<any> | Type<any>[],
  options: SignalRouterOptions = {},
  messageExtractor: MessageExtractor,
  registerHandler: (target: any, eventPattern: string, handlerName: string, context?: any) => void
) {
  const debug = options?.debug || !IS_PROD
  const logger = options?.logger || getDefaultLogger().child({ component: "signal-router" })
  const metrics = getDefaultMetrics()
  const tracer = options?.tracing?.enabled !== false ? getDefaultTracer() : null

  return function (target: any): any {
    const serviceNameFromMeta = deriveServiceName(target, options)
    const eventPattern = options?.eventPattern || `${serviceNameFromMeta}-events`
    const handlerName = "handleSignalMessage"
    const debugEnabled = debug && (logger.isLevelEnabled?.("debug") ?? true)

    const idem = new TwoTierIdempotency<MessageResponse>({
      l1Options: options.idempotency,
      distributed: options.idempotencyStore,
      logger
    })
    const replayGuard = new ReplayGuard({
      enabled: (options.security?.replayWindowMs ?? 0) > 0,
      windowMs: options.security?.replayWindowMs
    })
    const dlq = options.dlq instanceof DlqRouter ? options.dlq : new DlqRouter({ enabled: (options.dlq as any)?.enabled === true })
    const defaultVersion = options.defaultVersion || DEFAULT_METHOD_VERSION
    const rateLimiter = options.rateLimit !== undefined ? resolveRateLimiter(options.rateLimit) : new RateLimiter()
    const ownsRateLimiter = !(options.rateLimit instanceof RateLimiter)
    const devtoolsBus: DevToolsBus | null =
      options.devtools === false ? null : options.devtools instanceof Object ? (options.devtools as DevToolsBus) : getDevToolsBus()
    const methodLimiters = new Map<string, RateLimiter>()
    const methodCaches = new Map<string, LruCache<unknown>>()

    const allSignals = getClassSignals(target) as SignalMetadata[]
    const signalsByName = new Map<string, SignalMetadata[]>()
    const contractDescriptors: ContractMethodDescriptor[] = []
    const signalNames: string[] = []
    for (const s of allSignals) {
      let arr = signalsByName.get(s.signalName)
      if (!arr) {
        arr = []
        signalsByName.set(s.signalName, arr)
      }
      arr.push(s)
      if (!s.signalName.startsWith("nevo.")) {
        contractDescriptors.push({ signalName: s.signalName, version: s.version || DEFAULT_METHOD_VERSION })
        signalNames.push(s.signalName)
      }
    }
    contractDescriptors.sort((a, b) => a.signalName.localeCompare(b.signalName))

    // Known method names; unregistered/forged names bucket to `<unknown>` in metric labels.
    const knownMethodNames = new Set<string>(signalsByName.keys())
    knownMethodNames.add(NEVO_CONTRACT_METHOD)
    if (options.health) {
      knownMethodNames.add(NEVO_HEALTH_METHOD)
      knownMethodNames.add(NEVO_LIVENESS_METHOD)
      knownMethodNames.add(NEVO_READINESS_METHOD)
    }

    try {
      getDevToolsRegistry().registerService({
        serviceName: serviceNameFromMeta,
        instanceId: options.instanceId,
        transport: undefined,
        topic: eventPattern,
        capabilities: options.capabilities,
        methods: describeMethodsFromSignals(allSignals),
        accessControl: options.accessControl
      })
    } catch {}

    const serviceInstancesCache = new WeakMap<object, any[]>()
    const hedgeWarnedMethods = new Set<string>()

    const pipelineCfg: DispatchPipelineConfig = {
      serviceName: serviceNameFromMeta,
      topic: eventPattern,
      logger,
      metrics,
      devtoolsBus,
      auditLog: options.auditLog,
      replayGuard,
      rateLimiter,
      idem,
      dlq,
      accessControl: options.accessControl,
      before: options.before,
      after: options.after,
      disableBuiltinHandlers: options.disableBuiltinHandlers,
      methodLabelFor: (m) => methodLabel(m, (name) => knownMethodNames.has(name)),
      builtin: async (parsedName, method, uuid, meta) => {
        if (parsedName === NEVO_CONTRACT_METHOD) {
          const contract = {
            protocol: "1",
            serviceName: serviceNameFromMeta,
            serviceVersion: options.serviceVersion,
            instanceId: options.instanceId,
            capabilities: options.capabilities,
            generatedAt: Date.now(),
            methods: contractDescriptors
          }
          return { uuid, method, params: { result: contract as any }, meta }
        }
        if (options.health) {
          if (parsedName === NEVO_HEALTH_METHOD) {
            return { uuid, method, params: { result: (await options.health.report()) as any }, meta }
          }
          if (parsedName === NEVO_LIVENESS_METHOD) {
            return { uuid, method, params: { result: (await options.health.liveness()) as any }, meta }
          }
          if (parsedName === NEVO_READINESS_METHOD) {
            return { uuid, method, params: { result: (await options.health.readiness()) as any }, meta }
          }
        }
        return null
      }
    }

    const fail = (uuid: string, method: string, meta: MessageMeta | undefined, code: ErrorCode, message: string): DispatchStrategyResult => ({
      response: { uuid, method, params: { result: "error", error: new MessagingError(code, { message }, serviceNameFromMeta).toJSON() }, meta }
    })

    // The router-specific dispatch strategy: signal lookup, versioning, schema,
    // per-method rate limit/cache, resilience decorators, span-wrapped invoke.
    const dispatchSignal = async (
      controller: any,
      args: { parsed: ReturnType<typeof parseMethod>; processedParams: unknown; uuid: string; method: string; meta: MessageMeta | undefined; callerService: string | undefined }
    ): Promise<DispatchStrategyResult> => {
      const { parsed, uuid, method, meta, callerService } = args
      let processedParams = args.processedParams

      let serviceInstances = serviceInstancesCache.get(controller)
      if (!serviceInstances) {
        serviceInstances = findServiceInstances(controller, serviceType)
        if (serviceInstances.length > 0) serviceInstancesCache.set(controller, serviceInstances)
      }
      if (serviceInstances.length === 0) {
        logger.error({ event: "signal.no_service", topic: eventPattern, serviceType: String(serviceType) }, "No service instances found")
        return fail(uuid, method, meta, ErrorCode.SERVICE_NOT_FOUND, "Service not found")
      }

      const requestedVersion = parsed.version
      const candidates = signalsByName.get(parsed.name)
      if (!candidates || candidates.length === 0) {
        const suggestion = suggestClosestMethod(parsed.name, signalNames)
        const message = suggestion ? `Invalid method name '${parsed.name}', did you mean '${suggestion}'?` : `Method ${parsed.name} not found`
        return fail(uuid, method, meta, ErrorCode.METHOD_NOT_FOUND, message)
      }
      let signalHandler: SignalMetadata | undefined
      if (requestedVersion) {
        signalHandler = candidates.find((c) => (c.version || defaultVersion) === requestedVersion)
      } else {
        signalHandler = candidates.find((c) => (c.version || defaultVersion) === defaultVersion) || candidates[0]
      }
      if (!signalHandler) {
        return fail(uuid, method, meta, ErrorCode.UNSUPPORTED_VERSION, `No handler matching version ${requestedVersion} for ${parsed.name}`)
      }
      if (!isVersionCompatible(requestedVersion, signalHandler.version || defaultVersion)) {
        return fail(
          uuid,
          method,
          meta,
          ErrorCode.UNSUPPORTED_VERSION,
          `Method ${parsed.name} version mismatch (requested ${requestedVersion}, available ${signalHandler.version || defaultVersion})`
        )
      }

      const serviceMethod = signalHandler.methodName
      let serviceInstance: any = null
      for (const s of serviceInstances) {
        if (s && typeof s[serviceMethod] === "function") {
          serviceInstance = s
          break
        }
      }
      if (!serviceInstance) {
        logger.error({ event: "signal.method_not_found", serviceMethod }, "Method not found on any service instance")
        return fail(uuid, method, meta, ErrorCode.METHOD_NOT_FOUND, `Method ${serviceMethod} does not exist`)
      }

      const methodRateLimit = getMethodRateLimit(serviceInstance, serviceMethod)
      if (methodRateLimit) {
        let mLimiter = methodLimiters.get(serviceMethod)
        if (!mLimiter) {
          mLimiter = new RateLimiter(rateLimitToOptions(methodRateLimit))
          methodLimiters.set(serviceMethod, mLimiter)
        }
        try {
          mLimiter.check({ topic: eventPattern, method: parsed.name, callerService, tenantId: meta?.tenantId, meta })
        } catch (err) {
          if (err instanceof MessagingError && err.code === ErrorCode.RATE_LIMITED) {
            return { response: { uuid, method, params: { result: "error", error: err.toJSON() }, meta } }
          }
          throw err
        }
      }

      const cacheable = getMethodCacheable(serviceInstance, serviceMethod)
      let cacheKey: string | null = null
      let methodCache: LruCache<unknown> | undefined
      if (cacheable) {
        methodCache = methodCaches.get(serviceMethod)
        if (!methodCache) {
          methodCache = new LruCache<unknown>({ enabled: true, ttlMs: cacheable.ttlMs ?? 60_000, maxEntries: cacheable.maxEntries ?? 1024 })
          methodCaches.set(serviceMethod, methodCache)
        }
        cacheKey = cacheable.keyBy ? cacheable.keyBy(processedParams) : buildDefaultCacheKey(parsed.name, processedParams)
        if (methodCache.has(cacheKey)) {
          const cached = methodCache.get(cacheKey)
          // The cache holds the already-shaped result — skip the after hook so it isn't reapplied.
          return { response: { uuid, method, params: { result: cached as any }, meta }, result: cached, skipAfterHook: true }
        }
      }

      const schema = signalHandler.options?.schema ?? getSchemaFor(serviceInstance, serviceMethod)
      if (schema) {
        const validator = toValidator(schema)
        if (validator) {
          try {
            processedParams = validator.parse(processedParams)
          } catch (err: any) {
            const errPayload =
              err instanceof MessagingError ? err.toJSON() : { code: ErrorCode.VALIDATION_FAILED, message: err?.message || "validation failed" }
            return { response: { uuid, method, params: { result: "error", error: errPayload }, meta } }
          }
        }
      }

      const invokeArgs = signalHandler.paramTransformer ? signalHandler.paramTransformer(processedParams) : [processedParams]

      if (debugEnabled) {
        logger.debug({ event: "signal.call", topic: eventPattern, serviceMethod })
      }

      // Resilience decorators (@CircuitBreaker/@Adaptive/@Backpressure), if any.
      // Hedging a LOCAL handler just runs it twice (duplicated side effects, no
      // tail-latency win) — hedge belongs on the client call site.
      let resilience = readMethodResilience(serviceInstance, serviceMethod)
      if (resilience?.hedge) {
        if (!hedgeWarnedMethods.has(serviceMethod)) {
          hedgeWarnedMethods.add(serviceMethod)
          logger.warn(
            { event: "resilience.hedge_ignored", serviceMethod },
            "@Hedge on a server-side handler is ignored: it would double-execute the handler. Apply hedging on the calling client instead."
          )
        }
        const { hedge: _hedge, ...rest } = resilience
        resilience = rest.circuit || rest.adaptive || rest.backpressure ? rest : undefined
      }

      const invokeWithSpan = async (span: SpanLike | null): Promise<unknown> => {
        try {
          const invoke = () => serviceInstance[serviceMethod](...invokeArgs)
          const value = resilience
            ? await applyResilience({ config: resilience, ctx: { key: `${serviceNameFromMeta}:${parsed.name}` }, invoke })
            : await invoke()
          span?.setStatus({ code: 1 })
          return value
        } catch (err) {
          span?.recordException(err)
          span?.setStatus({ code: 2, message: (err as Error)?.message })
          throw err
        }
      }

      let result: unknown
      try {
        result = tracer
          ? await runWithSpan(
              tracer,
              `nevo.serve ${eventPattern}.${method}`,
              {
                "nevo.method": method,
                "nevo.service": serviceNameFromMeta,
                "nevo.uuid": uuid ?? "",
                "nevo.caller": callerService ?? ""
              },
              meta,
              invokeWithSpan
            )
          : await invokeWithSpan(null)
      } catch (err) {
        // @Backpressure admission failure (RATE_LIMITED) is load-shedding, not a crash.
        if (err instanceof MessagingError && err.code === ErrorCode.RATE_LIMITED) {
          return { response: { uuid, method, params: { result: "error", error: err.toJSON() }, meta } }
        }
        throw err
      }

      const transformedResult = signalHandler.resultTransformer ? signalHandler.resultTransformer(result) : result
      return {
        response: { uuid, method, params: { result: transformedResult as any }, meta },
        result: transformedResult,
        onCommitted: methodCache && cacheKey ? (resp) => methodCache!.set(cacheKey!, resp.params.result) : undefined
      }
    }

    target.prototype[handlerName] = async function (data: any) {
      // Peek at the envelope to seed the chain context for outbound calls.
      let peeked: MessageData | undefined
      try {
        peeked = messageExtractor(data)
      } catch {}
      const chainId = resolveInboundChainId(peeked?.meta?.nevoChainId)

      return runInChain({ chainId, parentUuid: peeked?.uuid }, async () => {
        const startMs = Date.now()
        if (debugEnabled) {
          logger.debug({ event: "signal.received", topic: eventPattern })
        }
        let messageData: MessageData | undefined = peeked
        if (!messageData) {
          try {
            messageData = messageExtractor(data)
          } catch {
            messageData = undefined
          }
        }

        return runDispatchPipeline(
          pipelineCfg,
          {
            data,
            method: messageData?.method ?? "",
            uuid: messageData?.uuid ?? (data as any)?.uuid ?? "",
            params: messageData?.params,
            meta: messageData?.meta,
            chainId,
            startMs
          },
          (strategyArgs) => dispatchSignal(this, strategyArgs)
        )
      })
    }

    const originalOnModuleDestroy = target.prototype.onModuleDestroy
    target.prototype.onModuleDestroy = async function () {
      if (originalOnModuleDestroy) await originalOnModuleDestroy.call(this)
      if (ownsRateLimiter) rateLimiter.stop()
      for (const limiter of methodLimiters.values()) limiter.stop()
      methodLimiters.clear()
    }

    registerHandler(target, eventPattern, handlerName)

    return target
  }
}

export { matchesFilter, parseMethod, DEFAULT_METHOD_VERSION }
