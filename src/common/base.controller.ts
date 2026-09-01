import { ErrorCode, MessagingError } from "./"
import { IS_PROD } from "./env"
import {
  AfterHook,
  BeforeHook,
  ServiceMethodHandler,
  ServiceMethodMapping,
  SystemAfterHook,
  SystemBeforeHook,
  MessageResponse,
  AccessControlConfig,
  MessageMeta,
  IdempotencyOptions,
  SecurityOptions,
  MetricsOptions,
  TracingOptions
} from "./types"
import { suggestClosestMethod } from "./levenshtein"
import { getDefaultLogger, NevoLogger } from "./logger"
import { LruIdempotencyCache } from "./idempotency"
import type { IdempotencyEnvelope, IdempotencyStore } from "./idempotency-store"
import { TwoTierIdempotency } from "./idempotency-runtime"
import { ReplayGuard } from "./replay-protection"
import { getSchemaFor, toValidator } from "./schema"
import { isVersionCompatible, parseMethod, DEFAULT_METHOD_VERSION } from "./version"
import { getDefaultMetrics, methodLabel } from "./metrics"
import { getDefaultTracer, NevoTracer, runWithSpan, SpanLike } from "./tracing"
import { DlqRouter } from "./dlq"
import { RateLimiter, resolveRateLimiter, RateLimiterOptions } from "./rate-limit"
import { NEVO_CONTRACT_METHOD, buildContract, ServiceContract } from "./contract"
import { NEVO_HEALTH_METHOD, NEVO_LIVENESS_METHOD, NEVO_READINESS_METHOD, HealthRegistry } from "./health"
import { getDevToolsBus, DevToolsBus } from "./devtools"
import { runInChain, resolveInboundChainId } from "./chain-context"
import { AuditLog } from "./audit-log"
import {
  runDispatchPipeline,
  shapeDispatchError,
  toWireError,
  type DispatchPipelineConfig,
  type DispatchStrategyArgs,
  type DispatchStrategyResult
} from "./dispatch-pipeline"

/** @deprecated Use a `@*SignalRouter` controller with `NevoModule`. Removed in the next major. */
export abstract class BaseMessageController {
  protected readonly methodRegistry: ServiceMethodMapping = {}
  serviceInstances: any[] = []
  protected readonly serviceName: string
  protected readonly beforeHook?: BeforeHook
  protected readonly afterHook?: AfterHook
  protected readonly systemBeforeHook: SystemBeforeHook
  protected readonly systemAfterHook: SystemAfterHook
  protected readonly debug: boolean
  protected readonly accessControl?: AccessControlConfig
  private _logger: NevoLogger | null = null
  private _loggerOverride: NevoLogger | null = null
  protected get logger(): NevoLogger {
    if (this._logger) return this._logger
    this._logger = this._loggerOverride ?? getDefaultLogger().child({ component: "controller", service: this.serviceName })
    return this._logger
  }
  protected readonly idempotency: LruIdempotencyCache<IdempotencyEnvelope<MessageResponse>>
  /** Optional distributed idempotency backend (Redis, Memcached, …). */
  protected readonly distributedIdempotency?: IdempotencyStore<IdempotencyEnvelope<MessageResponse>>
  /** Shared two-tier idempotency runtime (L1 + claim-before-execute). */
  private readonly idem: TwoTierIdempotency<MessageResponse>
  protected readonly replayGuard: ReplayGuard
  protected readonly dlq: DlqRouter
  protected readonly tracer: NevoTracer | null
  protected readonly defaultVersion: string
  protected readonly rateLimiter: RateLimiter
  private readonly ownsRateLimiter: boolean
  private readonly disableBuiltinHandlers: boolean
  protected readonly healthRegistry?: HealthRegistry
  protected readonly instanceId?: string
  protected readonly capabilities?: string[]
  protected readonly serviceVersion?: string
  protected readonly devtoolsBus: DevToolsBus | null
  /** Optional append-only audit log. */
  protected readonly auditLog?: AuditLog
  private _pipelineCfg: DispatchPipelineConfig | null = null

  protected constructor(
    serviceName: string,
    serviceInstances: any[],
    methodHandlers: ServiceMethodMapping,
    options?: {
      onBefore?: BeforeHook
      onAfter?: AfterHook
      debug?: boolean
      accessControl?: AccessControlConfig
      logger?: NevoLogger
      idempotency?: IdempotencyOptions
      idempotencyStore?: IdempotencyStore<IdempotencyEnvelope<MessageResponse>>
      security?: SecurityOptions
      metrics?: MetricsOptions
      tracing?: TracingOptions
      dlq?: DlqRouter
      defaultVersion?: string
      rateLimit?: RateLimiterOptions | RateLimiter
      health?: HealthRegistry
      instanceId?: string
      serviceVersion?: string
      capabilities?: string[]
      disableBuiltinHandlers?: boolean
      devtools?: DevToolsBus | boolean
      /** Append-only audit log of every request/response. */
      auditLog?: AuditLog
    }
  ) {
    this.serviceName = serviceName
    this.serviceInstances = serviceInstances || []
    this.beforeHook = options?.onBefore
    this.afterHook = options?.onAfter
    this.debug = options?.debug || false
    this.accessControl = options?.accessControl
    this._loggerOverride = options?.logger ?? null
    this.idempotency = new LruIdempotencyCache<IdempotencyEnvelope<MessageResponse>>(options?.idempotency)
    this.distributedIdempotency = options?.idempotencyStore
    this.idem = new TwoTierIdempotency<MessageResponse>({
      l1: this.idempotency,
      distributed: this.distributedIdempotency,
      logger: this.logger
    })
    this.replayGuard = new ReplayGuard({
      enabled: (options?.security?.replayWindowMs ?? 0) > 0,
      windowMs: options?.security?.replayWindowMs
    })
    this.dlq = options?.dlq ?? new DlqRouter({ enabled: false })
    this.tracer = options?.tracing?.enabled === false ? null : getDefaultTracer()
    this.defaultVersion = options?.defaultVersion || DEFAULT_METHOD_VERSION
    this.rateLimiter = options?.rateLimit !== undefined ? resolveRateLimiter(options.rateLimit) : new RateLimiter()
    this.ownsRateLimiter = !(options?.rateLimit instanceof RateLimiter)
    this.disableBuiltinHandlers = options?.disableBuiltinHandlers === true
    this.healthRegistry = options?.health
    this.instanceId = options?.instanceId
    this.capabilities = options?.capabilities
    this.serviceVersion = options?.serviceVersion
    this.devtoolsBus = options?.devtools === false ? null : options?.devtools instanceof Object ? (options.devtools as DevToolsBus) : getDevToolsBus()
    this.auditLog = options?.auditLog

    this.systemBeforeHook = (context) => {
      if (this.debug) {
        this.logger.debug({ event: "ctl.received", method: context.method, uuid: context.uuid })
      }
    }

    this.systemAfterHook = (context) => {
      if (this.debug) {
        this.logger.debug({
          event: "ctl.responding",
          method: context.method,
          uuid: context.uuid,
          success: context.response.params.result !== "error"
        })
      }
    }

    if (methodHandlers) {
      this.registerMethodHandlers(methodHandlers)
    }
  }

  protected registerMethodHandlers(handlers: ServiceMethodMapping): void {
    Object.entries(handlers).forEach(([methodName, handler]) => {
      this.methodRegistry[methodName] = handler
    })
  }

  // Version-stripped metric label; unregistered/forged methods bucket to `<unknown>`.
  private metricMethodLabel(method: string): string {
    return methodLabel(method, (name) => Object.prototype.hasOwnProperty.call(this.methodRegistry, name))
  }

  protected findServiceInstance(methodName: string): any {
    for (const instance of this.serviceInstances) {
      if (instance && typeof instance[methodName] === "function") {
        return instance
      }
    }
    return null
  }

  protected async executeHandler(handler: ServiceMethodHandler, params: unknown): Promise<unknown> {
    const { serviceMethod, paramTransformer, resultTransformer, schema } = handler

    const serviceInstance = this.findServiceInstance(serviceMethod)

    if (!serviceInstance) {
      throw new MessagingError(ErrorCode.METHOD_NOT_FOUND, { message: `No service found with method: ${serviceMethod}` })
    }

    const validator = toValidator(schema ?? getSchemaFor(serviceInstance, serviceMethod))
    let validated = params
    if (validator) {
      try {
        validated = validator.parse(params)
      } catch (err) {
        if (err instanceof MessagingError) throw err
        throw new MessagingError(ErrorCode.VALIDATION_FAILED, { message: (err as Error)?.message ?? "validation failed" })
      }
    }

    const methodArgs = paramTransformer ? paramTransformer(validated) : [validated]

    try {
      const result = await serviceInstance[serviceMethod](...methodArgs)
      return resultTransformer ? resultTransformer(result) : result
    } catch (error: any) {
      if (error?.message?.includes?.("is not a function")) {
        throw new MessagingError(ErrorCode.METHOD_NOT_FOUND, { message: `Method '${serviceMethod}' not found in service` })
      }
      throw error
    }
  }

  protected async formatResult(result: unknown): Promise<unknown> {
    if (result instanceof Promise) {
      result = await result
    }
    return result
  }

  protected createErrorResponse(uuid: string, method: string, error: any, meta?: MessageMeta): MessageResponse {
    if (error instanceof MessagingError) {
      return { uuid, method, params: { result: "error", error: toWireError(error) }, meta }
    }

    this.logger.error({ event: "ctl.unexpected_error", method, err: error?.message || String(error) }, "Unexpected error")

    return {
      uuid,
      method,
      params: {
        result: "error",
        error: {
          code: ErrorCode.INTERNAL,
          message: !IS_PROD ? (error?.message ?? String(error)) : "Internal server error",
          details: {},
          service: this.serviceName
        }
      },
      meta
    }
  }

  protected async handleBuiltinMethod(method: string, uuid: string, meta?: MessageMeta): Promise<MessageResponse | null> {
    const parsed = parseMethod(method)
    if (parsed.name === NEVO_CONTRACT_METHOD) {
      const contract: ServiceContract = buildContract(this.serviceName, this.methodRegistry, {
        instanceId: this.instanceId,
        serviceVersion: this.serviceVersion,
        capabilities: this.capabilities
      })
      return { uuid, method, params: { result: contract as any }, meta }
    }
    if (this.healthRegistry) {
      if (parsed.name === NEVO_HEALTH_METHOD) {
        const report = await this.healthRegistry.report()
        return { uuid, method, params: { result: report as any }, meta }
      }
      if (parsed.name === NEVO_LIVENESS_METHOD) {
        const report = await this.healthRegistry.liveness()
        return { uuid, method, params: { result: report as any }, meta }
      }
      if (parsed.name === NEVO_READINESS_METHOD) {
        const report = await this.healthRegistry.readiness()
        return { uuid, method, params: { result: report as any }, meta }
      }
    }
    return null
  }

  private pipelineCfg(): DispatchPipelineConfig {
    if (!this._pipelineCfg) {
      this._pipelineCfg = {
        serviceName: this.serviceName,
        topic: this.serviceName,
        logger: this.logger,
        metrics: getDefaultMetrics(),
        devtoolsBus: this.devtoolsBus,
        auditLog: this.auditLog,
        replayGuard: this.replayGuard,
        rateLimiter: this.rateLimiter,
        idem: this.idem,
        dlq: this.dlq,
        accessControl: this.accessControl,
        before: this.beforeHook,
        after: this.afterHook,
        disableBuiltinHandlers: this.disableBuiltinHandlers,
        methodLabelFor: (m) => this.metricMethodLabel(m ?? ""),
        builtin: (_parsedName, method, uuid, meta) => this.handleBuiltinMethod(method, uuid, meta)
      }
    }
    return this._pipelineCfg
  }

  // Registry-based dispatch strategy for the shared pipeline.
  private async dispatchRegistered(args: DispatchStrategyArgs): Promise<DispatchStrategyResult> {
    const { parsed, processedParams, uuid, method, meta } = args

    const handler = this.methodRegistry[parsed.name] ?? this.methodRegistry[method]
    if (!handler) {
      const suggestion = IS_PROD ? null : suggestClosestMethod(parsed.name, Object.keys(this.methodRegistry))
      const message = suggestion ? `Invalid method name '${parsed.name}', did you mean '${suggestion}'?` : `Method handler not found: ${parsed.name}`
      return { response: shapeDispatchError(this.serviceName, uuid, method, new MessagingError(ErrorCode.METHOD_NOT_FOUND, { message }), meta) }
    }

    if (handler.version && parsed.version && !isVersionCompatible(parsed.version, handler.version)) {
      const message = `Version mismatch for ${parsed.name}: requested ${parsed.version}, available ${handler.version}`
      return { response: shapeDispatchError(this.serviceName, uuid, method, new MessagingError(ErrorCode.UNSUPPORTED_VERSION, { message }), meta) }
    }

    const invokeWithSpan = async (span: SpanLike | null): Promise<unknown> => {
      try {
        const value = await this.executeHandler(handler, processedParams)
        span?.setStatus({ code: 1 })
        return value
      } catch (err) {
        span?.recordException(err)
        span?.setStatus({ code: 2, message: (err as Error)?.message })
        throw err
      }
    }

    const result: unknown = this.tracer
      ? await runWithSpan(
          this.tracer,
          `nevo.serve ${this.serviceName}.${parsed.name}`,
          {
            "nevo.method": method,
            "nevo.service": this.serviceName
          },
          meta,
          invokeWithSpan
        )
      : await invokeWithSpan(null)
    const formattedResult = await this.formatResult(result)

    return { response: { uuid, method, params: { result: formattedResult as any }, meta }, result: formattedResult }
  }

  async processMessage(data: any): Promise<MessageResponse> {
    const startMs = Date.now()
    let method = ""
    let uuid = ""
    let params: any
    let meta: MessageMeta | undefined
    let malformed = false
    try {
      ;({ method, uuid, params, meta } = this.extractMessageData(data))
    } catch {
      malformed = true
      method = String((data as any)?.method ?? "")
      uuid = String((data as any)?.uuid ?? "")
    }

    // Establish a chain context so outbound calls inherit the same chain id.
    const chainId = resolveInboundChainId(meta?.nevoChainId)
    return runInChain({ chainId, parentUuid: uuid }, async () => {
      await this.systemBeforeHook({ method, serviceName: this.serviceName, uuid, rawData: data, meta, params })
      const response = await runDispatchPipeline(
        this.pipelineCfg(),
        // A malformed envelope dispatches with an empty method → BAD_REQUEST.
        { data, method: malformed ? "" : method, uuid, params, meta, chainId, startMs },
        (strategyArgs) => this.dispatchRegistered(strategyArgs)
      )
      await this.systemAfterHook({
        method,
        serviceName: this.serviceName,
        uuid,
        rawData: data,
        meta,
        params,
        result: response.params.result === "error" ? undefined : response.params.result,
        response
      })
      return response
    })
  }

  protected abstract extractMessageData(data: any): { method: string; uuid: string; params: any; meta?: MessageMeta }

  public abstract handleMessage(data: any): Promise<MessageResponse>

  async close(): Promise<void> {
    if (this.ownsRateLimiter) this.rateLimiter.stop()
  }

  async onModuleDestroy(): Promise<void> {
    await this.close()
  }
}
