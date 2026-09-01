import { createHash } from "node:crypto"
import { Type } from "@nestjs/common"
import type {
  AccessControlConfig,
  AfterHook,
  BeforeHook,
  IdempotencyOptions,
  MessageMeta,
  MessageResponse,
  MetricsOptions,
  SecurityOptions,
  TracingOptions
} from "./common/types"
import { ErrorCode } from "./common/error-code"
import { MessagingError } from "./common/errors"
import { IS_PROD } from "./common/env"
import { getClassSignals, getNevoServiceName, SignalMetadata } from "./signal.decorator"
import { suggestClosestMethod } from "./common/levenshtein"
import { getDefaultLogger, NevoLogger } from "./common/logger"
import { TwoTierIdempotency } from "./common/idempotency-runtime"
import type { IdempotencyEnvelope, IdempotencyStore } from "./common/idempotency-store"
import { AuditLog } from "./common/audit-log"
import { ReplayGuard } from "./common/replay-protection"
import { getSchemaFor, toValidator } from "./common/schema"
import { parseMethod, isVersionCompatible, DEFAULT_METHOD_VERSION } from "./common/version"
import { getDefaultMetrics, methodLabel } from "./common/metrics"
import { getDefaultTracer, runWithSpan, SpanLike } from "./common/tracing"
import { DlqRouter } from "./common/dlq"
import { RateLimiter, resolveRateLimiter, RateLimiterOptions } from "./common/rate-limit"
import { NEVO_CONTRACT_METHOD, ContractMethodDescriptor } from "./common/contract"
import { NEVO_HEALTH_METHOD, NEVO_LIVENESS_METHOD, NEVO_READINESS_METHOD, HealthRegistry } from "./common/health"
import { getDevToolsBus, DevToolsBus } from "./common/devtools"
import { getDevToolsRegistry, describeMethodsFromSignals } from "./common/devtools-registry"
import {
  getMethodRateLimit,
  getMethodCacheable,
  rateLimitToOptions,
  DEFAULT_CACHEABLE_SCOPE,
  type CacheableConfig,
  type CacheableKeyContext,
  type CacheableScope
} from "./common/method-decorators"
import { stringifyWithBigInt } from "./common/bigint.utils"
import { LruIdempotencyCache as LruCache } from "./common/idempotency"
import { runInChain, resolveInboundChainId } from "./common/chain-context"
import { readMethodResilience, applyResilience, type CompiledResilience } from "./common/resilience-runtime"
import { runDispatchPipeline, toWireError, type DispatchPipelineConfig, type DispatchStrategyResult } from "./common/dispatch-pipeline"
import { DEFAULT_MAX_PAYLOAD_BYTES } from "./common/payload-limit"

export const NEVO_ROUTER_METADATA = "nevo:router:metadata"

/** Request-processing configuration, resolved through Nest DI by `NevoModule`. */
export interface NevoRuntimeOptions {
  serviceName?: string
  serviceVersion?: string
  instanceId?: string
  capabilities?: string[]
  accessControl?: AccessControlConfig
  idempotency?: IdempotencyOptions
  idempotencyStore?: IdempotencyStore<IdempotencyEnvelope<MessageResponse>>
  auditLog?: AuditLog
  security?: SecurityOptions
  metrics?: MetricsOptions
  tracing?: TracingOptions
  logger?: NevoLogger
  dlq?: DlqRouter | { enabled?: boolean }
  defaultVersion?: string
  rateLimit?: RateLimiterOptions | RateLimiter
  health?: HealthRegistry
  devtools?: DevToolsBus | boolean
  disableBuiltinHandlers?: boolean
}

export interface SignalRouterMetadata {
  eventPattern?: string
  serviceName?: string
  debug?: boolean
  before?: BeforeHook
  after?: AfterHook
}

export interface MessageData {
  method: string
  params: any
  uuid: string
  meta?: MessageMeta
}

export interface ExtractContext {
  logger: NevoLogger
  maxPayloadBytes: number
}

export type MessageExtractor = (data: any, ctx: ExtractContext) => MessageData

interface RouterClassMetadata {
  serviceType: Type<any> | Type<any>[]
  eventPattern: string
  serviceName: string
  routerMeta: SignalRouterMetadata
  extract: MessageExtractor
}

interface MethodDescriptor {
  limiter: RateLimiter | null
  cacheable: CacheableConfig | undefined
  cache: LruCache<unknown> | undefined
  ownSchema: unknown
  resilience: CompiledResilience | undefined
}

export function findPropertyByType(obj: any, type: Type<any>): string | null {
  for (const prop in obj) {
    if (obj[prop] instanceof type) return prop
  }
  return null
}

export function findServiceInstances(instance: any, serviceType: Type<any> | Type<any>[]): any[] {
  const types = Array.isArray(serviceType) ? serviceType : [serviceType]
  const out: any[] = []
  for (const type of types) {
    const propName = findPropertyByType(instance, type)
    if (propName && instance[propName]) out.push(instance[propName])
  }
  return out
}

export function deriveServiceName(target: any, meta: SignalRouterMetadata): string {
  if (meta.serviceName) return meta.serviceName
  if (meta.eventPattern) return meta.eventPattern.replace(/-events$/, "")
  const fromMeta = getNevoServiceName(target)
  if (fromMeta) return fromMeta
  const className = (target?.name as string | undefined) ?? "unknown"
  const lower = className.toLowerCase()
  if (lower.endsWith("controller")) return lower.slice(0, -"controller".length) || lower
  return lower
}

const DEFAULT_CACHE_KEY_MAX_CHARS = 1024

// Must be collision-resistant: params are caller-controlled.
function cacheKeyDigest(s: string): string {
  return createHash("sha256").update(s).digest("base64url").slice(0, 27)
}

/** Returns null when params can't be serialized; the call then bypasses the cache. */
function buildDefaultCacheKey(ctx: CacheableKeyContext, processedParams: unknown, scope: readonly CacheableScope[]): string | null {
  let serialized: string
  try {
    serialized = stringifyWithBigInt(processedParams ?? {}) ?? "{}"
  } catch {
    return null
  }
  const scoped: string[] = []
  for (const dim of scope) {
    if (dim === "tenantId") scoped.push(`t=${ctx.tenantId ?? ""}`)
    else if (dim === "callerService") scoped.push(`c=${ctx.callerService ?? ""}`)
  }
  const head = `${ctx.method}@${ctx.version ?? ""}|${scoped.join("&")}`
  if (serialized.length <= DEFAULT_CACHE_KEY_MAX_CHARS) return `${head}::${serialized}`
  return `${head}::#${serialized.length}:${cacheKeyDigest(serialized)}`
}

/** One per decorated controller class, built by `NevoModule` from DI-resolved options. */
export class RouterRuntime {
  readonly serviceName: string
  readonly eventPattern: string
  readonly logger: NevoLogger
  readonly dlq: DlqRouter
  readonly maxPayloadBytes: number

  private readonly serviceType: Type<any> | Type<any>[]
  private readonly extract: MessageExtractor
  private readonly debugEnabled: boolean
  private readonly defaultVersion: string
  private readonly tracer: ReturnType<typeof getDefaultTracer> | null
  private readonly rateLimiter: RateLimiter
  private readonly ownsRateLimiter: boolean
  private readonly ownedLimiters: RateLimiter[] = []
  private readonly signalsByName = new Map<string, SignalMetadata[]>()
  private readonly signalNames: string[] = []
  private readonly contractDescriptors: ContractMethodDescriptor[] = []
  private readonly serviceInstancesCache = new WeakMap<object, any[]>()
  private readonly descriptors = new WeakMap<object, Map<string, MethodDescriptor>>()
  private readonly pipelineCfg: DispatchPipelineConfig

  constructor(
    private readonly target: any,
    classMeta: RouterClassMetadata,
    private readonly options: NevoRuntimeOptions
  ) {
    this.serviceName = classMeta.serviceName
    this.eventPattern = classMeta.eventPattern
    this.serviceType = classMeta.serviceType
    this.extract = classMeta.extract
    this.logger = options.logger ?? getDefaultLogger().child({ component: "signal-router", service: this.serviceName })
    this.debugEnabled = (classMeta.routerMeta.debug ?? !IS_PROD) && (this.logger.isLevelEnabled?.("debug") ?? true)
    this.defaultVersion = options.defaultVersion || DEFAULT_METHOD_VERSION
    this.tracer = options.tracing?.enabled !== false ? getDefaultTracer() : null
    this.maxPayloadBytes = options.security?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
    this.dlq = options.dlq instanceof DlqRouter ? options.dlq : new DlqRouter({ enabled: (options.dlq as any)?.enabled === true })
    this.rateLimiter = options.rateLimit !== undefined ? resolveRateLimiter(options.rateLimit) : new RateLimiter()
    this.ownsRateLimiter = !(options.rateLimit instanceof RateLimiter)

    const idem = new TwoTierIdempotency<MessageResponse>({
      l1Options: options.idempotency,
      distributed: options.idempotencyStore,
      logger: this.logger
    })
    const replayGuard = new ReplayGuard({
      enabled: (options.security?.replayWindowMs ?? 0) > 0,
      windowMs: options.security?.replayWindowMs
    })

    const allSignals = getClassSignals(target) as SignalMetadata[]
    for (const s of allSignals) {
      let arr = this.signalsByName.get(s.signalName)
      if (!arr) {
        arr = []
        this.signalsByName.set(s.signalName, arr)
      }
      arr.push(s)
      if (!s.signalName.startsWith("nevo.")) {
        this.contractDescriptors.push({ signalName: s.signalName, version: s.version || DEFAULT_METHOD_VERSION })
        this.signalNames.push(s.signalName)
      }
    }
    this.contractDescriptors.sort((a, b) => a.signalName.localeCompare(b.signalName))

    const knownMethodNames = new Set<string>(this.signalsByName.keys())
    knownMethodNames.add(NEVO_CONTRACT_METHOD)
    if (options.health) {
      knownMethodNames.add(NEVO_HEALTH_METHOD)
      knownMethodNames.add(NEVO_LIVENESS_METHOD)
      knownMethodNames.add(NEVO_READINESS_METHOD)
    }

    const devtoolsBus: DevToolsBus | null =
      options.devtools === false ? null : options.devtools instanceof Object ? (options.devtools as DevToolsBus) : getDevToolsBus()

    try {
      getDevToolsRegistry().registerService({
        serviceName: this.serviceName,
        instanceId: options.instanceId,
        transport: undefined,
        topic: this.eventPattern,
        capabilities: options.capabilities,
        methods: describeMethodsFromSignals(allSignals),
        accessControl: options.accessControl
      })
    } catch {}

    this.pipelineCfg = {
      serviceName: this.serviceName,
      topic: this.eventPattern,
      logger: this.logger,
      metrics: getDefaultMetrics(),
      devtoolsBus,
      auditLog: options.auditLog,
      replayGuard,
      rateLimiter: this.rateLimiter,
      idem,
      dlq: this.dlq,
      accessControl: options.accessControl,
      before: classMeta.routerMeta.before,
      after: classMeta.routerMeta.after,
      disableBuiltinHandlers: options.disableBuiltinHandlers,
      methodLabelFor: (m) => methodLabel(m, (name) => knownMethodNames.has(name)),
      builtin: (parsedName, method, uuid, meta) => this.handleBuiltin(parsedName, method, uuid, meta)
    }
  }

  private async handleBuiltin(parsedName: string, method: string, uuid: string, meta?: MessageMeta): Promise<MessageResponse | null> {
    if (parsedName === NEVO_CONTRACT_METHOD) {
      const contract = {
        protocol: "1",
        serviceName: this.serviceName,
        serviceVersion: this.options.serviceVersion,
        instanceId: this.options.instanceId,
        capabilities: this.options.capabilities,
        generatedAt: Date.now(),
        methods: this.contractDescriptors
      }
      return { uuid, method, params: { result: contract as any }, meta }
    }
    const health = this.options.health
    if (health) {
      if (parsedName === NEVO_HEALTH_METHOD) return { uuid, method, params: { result: (await health.report()) as any }, meta }
      if (parsedName === NEVO_LIVENESS_METHOD) return { uuid, method, params: { result: (await health.liveness()) as any }, meta }
      if (parsedName === NEVO_READINESS_METHOD) return { uuid, method, params: { result: (await health.readiness()) as any }, meta }
    }
    return null
  }

  private describeMethod(serviceInstance: any, serviceMethod: string): MethodDescriptor {
    let byMethod = this.descriptors.get(serviceInstance)
    if (!byMethod) {
      byMethod = new Map<string, MethodDescriptor>()
      this.descriptors.set(serviceInstance, byMethod)
    }
    const existing = byMethod.get(serviceMethod)
    if (existing) return existing

    const rateLimit = getMethodRateLimit(serviceInstance, serviceMethod)
    const cacheable = getMethodCacheable(serviceInstance, serviceMethod)
    let limiter: RateLimiter | null = null
    if (rateLimit) {
      limiter = new RateLimiter(rateLimitToOptions(rateLimit))
      this.ownedLimiters.push(limiter)
    }

    let resilience = readMethodResilience(serviceInstance, serviceMethod)
    if (resilience?.hedge) {
      this.logger.warn(
        { event: "resilience.hedge_ignored", serviceMethod },
        "@Hedge on a server-side handler is ignored: it would double-execute the handler. Apply hedging on the calling client instead."
      )
      const { hedge: _hedge, ...rest } = resilience
      resilience = rest.circuit || rest.adaptive || rest.backpressure ? rest : undefined
    }

    const descriptor: MethodDescriptor = {
      limiter,
      cacheable,
      cache: cacheable
        ? new LruCache<unknown>({ enabled: true, ttlMs: cacheable.ttlMs ?? 60_000, maxEntries: cacheable.maxEntries ?? 1024 })
        : undefined,
      ownSchema: getSchemaFor(serviceInstance, serviceMethod),
      resilience
    }
    byMethod.set(serviceMethod, descriptor)
    return descriptor
  }

  private fail(uuid: string, method: string, meta: MessageMeta | undefined, code: ErrorCode, message: string): DispatchStrategyResult {
    return {
      response: { uuid, method, params: { result: "error", error: toWireError(new MessagingError(code, { message }, this.serviceName)) }, meta }
    }
  }

  async handle(controller: any, data: any): Promise<MessageResponse> {
    const extractCtx: ExtractContext = { logger: this.logger, maxPayloadBytes: this.maxPayloadBytes }
    let peeked: MessageData | undefined
    try {
      peeked = this.extract(data, extractCtx)
    } catch {}
    const chainId = resolveInboundChainId(peeked?.meta?.nevoChainId)

    return runInChain({ chainId, parentUuid: peeked?.uuid }, async () => {
      const startMs = Date.now()
      if (this.debugEnabled) this.logger.debug({ event: "signal.received", topic: this.eventPattern })

      let messageData: MessageData | undefined = peeked
      if (!messageData) {
        try {
          messageData = this.extract(data, extractCtx)
        } catch {
          messageData = undefined
        }
      }

      return runDispatchPipeline(
        this.pipelineCfg,
        {
          data,
          method: messageData?.method ?? "",
          uuid: messageData?.uuid ?? (data as any)?.uuid ?? "",
          params: messageData?.params,
          meta: messageData?.meta,
          chainId,
          startMs
        },
        (strategyArgs) => this.dispatchSignal(controller, strategyArgs)
      )
    })
  }

  private async dispatchSignal(
    controller: any,
    args: {
      parsed: ReturnType<typeof parseMethod>
      processedParams: unknown
      uuid: string
      method: string
      meta: MessageMeta | undefined
      callerService: string | undefined
    }
  ): Promise<DispatchStrategyResult> {
    const { parsed, uuid, method, meta, callerService } = args
    let processedParams = args.processedParams

    let serviceInstances = this.serviceInstancesCache.get(controller)
    if (!serviceInstances) {
      serviceInstances = findServiceInstances(controller, this.serviceType)
      if (serviceInstances.length > 0) this.serviceInstancesCache.set(controller, serviceInstances)
    }
    if (serviceInstances.length === 0) {
      this.logger.error({ event: "signal.no_service", topic: this.eventPattern, serviceType: String(this.serviceType) }, "No service instances found")
      return this.fail(uuid, method, meta, ErrorCode.SERVICE_NOT_FOUND, "Service not found")
    }

    const requestedVersion = parsed.version
    const candidates = this.signalsByName.get(parsed.name)
    if (!candidates || candidates.length === 0) {
      const suggestion = IS_PROD ? null : suggestClosestMethod(parsed.name, this.signalNames)
      const message = suggestion ? `Invalid method name '${parsed.name}', did you mean '${suggestion}'?` : `Method ${parsed.name} not found`
      return this.fail(uuid, method, meta, ErrorCode.METHOD_NOT_FOUND, message)
    }

    const signalHandler = requestedVersion
      ? candidates.find((c) => (c.version || this.defaultVersion) === requestedVersion)
      : (candidates.find((c) => (c.version || this.defaultVersion) === this.defaultVersion) ?? candidates[0])
    if (!signalHandler) {
      return this.fail(uuid, method, meta, ErrorCode.UNSUPPORTED_VERSION, `No handler matching version ${requestedVersion} for ${parsed.name}`)
    }
    if (!isVersionCompatible(requestedVersion, signalHandler.version || this.defaultVersion)) {
      return this.fail(
        uuid,
        method,
        meta,
        ErrorCode.UNSUPPORTED_VERSION,
        `Method ${parsed.name} version mismatch (requested ${requestedVersion}, available ${signalHandler.version || this.defaultVersion})`
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
      this.logger.error({ event: "signal.method_not_found", serviceMethod }, "Method not found on any service instance")
      return this.fail(uuid, method, meta, ErrorCode.METHOD_NOT_FOUND, `Method ${serviceMethod} does not exist`)
    }

    const descriptor = this.describeMethod(serviceInstance, serviceMethod)

    if (descriptor.limiter) {
      try {
        descriptor.limiter.check({ topic: this.eventPattern, method: parsed.name, callerService, tenantId: meta?.tenantId, meta })
      } catch (err) {
        if (err instanceof MessagingError && err.code === ErrorCode.RATE_LIMITED) {
          return { response: { uuid, method, params: { result: "error", error: toWireError(err) }, meta } }
        }
        throw err
      }
    }

    const cacheable = descriptor.cacheable
    const methodCache = descriptor.cache
    let cacheKey: string | null = null
    if (cacheable && methodCache) {
      const keyCtx: CacheableKeyContext = { method: parsed.name, version: parsed.version, tenantId: meta?.tenantId, callerService, meta }
      cacheKey = cacheable.keyBy
        ? cacheable.keyBy(processedParams, keyCtx)
        : buildDefaultCacheKey(keyCtx, processedParams, cacheable.scope ?? DEFAULT_CACHEABLE_SCOPE)
      if (cacheKey) {
        const cached = methodCache.get(cacheKey)
        if (cached !== undefined) {
          return { response: { uuid, method, params: { result: cached as any }, meta }, result: cached, skipAfterHook: true }
        }
      }
    }

    const schema = signalHandler.options?.schema ?? descriptor.ownSchema
    if (schema) {
      const validator = toValidator(schema)
      if (validator) {
        try {
          processedParams = validator.parse(processedParams)
        } catch (err: any) {
          const errPayload =
            err instanceof MessagingError ? toWireError(err) : { code: ErrorCode.VALIDATION_FAILED, message: err?.message || "validation failed" }
          return { response: { uuid, method, params: { result: "error", error: errPayload }, meta } }
        }
      }
    }

    const invokeArgs = signalHandler.paramTransformer ? signalHandler.paramTransformer(processedParams) : [processedParams]
    if (this.debugEnabled) this.logger.debug({ event: "signal.call", topic: this.eventPattern, serviceMethod })

    const resilience = descriptor.resilience
    const invokeWithSpan = async (span: SpanLike | null): Promise<unknown> => {
      try {
        const invoke = () => serviceInstance[serviceMethod](...invokeArgs)
        const value = resilience
          ? await applyResilience({ config: resilience, ctx: { key: `${this.serviceName}:${parsed.name}` }, invoke })
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
      result = this.tracer
        ? await runWithSpan(
            this.tracer,
            `nevo.serve ${this.eventPattern}.${method}`,
            {
              "nevo.method": method,
              "nevo.service": this.serviceName,
              "nevo.uuid": uuid ?? "",
              "nevo.caller": callerService ?? ""
            },
            meta,
            invokeWithSpan
          )
        : await invokeWithSpan(null)
    } catch (err) {
      // @Backpressure admission failure is load-shedding, not a crash.
      if (err instanceof MessagingError && err.code === ErrorCode.RATE_LIMITED) {
        return { response: { uuid, method, params: { result: "error", error: toWireError(err) }, meta } }
      }
      throw err
    }

    const transformedResult = signalHandler.resultTransformer ? signalHandler.resultTransformer(result) : result
    return {
      response: { uuid, method, params: { result: transformedResult as any }, meta },
      result: transformedResult,
      onCommitted: methodCache && cacheKey ? (resp) => methodCache.set(cacheKey!, resp.params.result) : undefined
    }
  }

  async dispose(): Promise<void> {
    if (this.ownsRateLimiter) this.rateLimiter.stop()
    for (const limiter of this.ownedLimiters) limiter.stop()
    this.ownedLimiters.length = 0
  }
}

// On globalThis so a re-import shares one registry.
type RouterClass = abstract new (...args: any[]) => unknown

const routers: Map<RouterClass, RouterRuntime> = ((globalThis as any).__nevoRouterRuntimes ??= new Map())

export function getRouterClassMetadata(target: any): RouterClassMetadata | undefined {
  return (Reflect as any).getOwnMetadata?.(NEVO_ROUTER_METADATA, target) ?? Reflect.getMetadata(NEVO_ROUTER_METADATA, target)
}

export function setRouterClassMetadata(target: any, meta: RouterClassMetadata): void {
  Reflect.defineMetadata(NEVO_ROUTER_METADATA, meta, target)
}

export function bindNevoRouter(target: any, options: NevoRuntimeOptions): RouterRuntime {
  const classMeta = getRouterClassMetadata(target)
  if (!classMeta) {
    throw new Error(
      `bindNevoRouter: ${target?.name ?? "class"} carries no Nevo router metadata. ` +
        "Decorate it with @NatsSignalRouter / @KafkaSignalRouter / @HttpSignalRouter / @WsSignalRouter / @SocketSignalRouter."
    )
  }
  const existing = routers.get(target)
  if (existing) return existing
  const runtime = new RouterRuntime(target, classMeta, options)
  routers.set(target, runtime)
  return runtime
}

export function tryGetRouterRuntime(target: any): RouterRuntime | undefined {
  return routers.get(target)
}

export function getRouterRuntime(target: any): RouterRuntime {
  const runtime = routers.get(target)
  if (!runtime) {
    throw new Error(
      `Nevo router for ${target?.name ?? "controller"} is not initialised. ` +
        "Import NevoModule.forRoot(...) (or forRootAsync) in the module that declares this controller."
    )
  }
  return runtime
}

export async function resetNevoRouters(): Promise<void> {
  const all = [...routers.values()]
  routers.clear()
  for (const runtime of all) {
    try {
      await runtime.dispose()
    } catch {}
  }
}

export async function disposeNevoRouter(target: any): Promise<void> {
  const runtime = routers.get(target)
  if (!runtime) return
  routers.delete(target)
  await runtime.dispose()
}
