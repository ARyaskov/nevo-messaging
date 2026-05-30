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
import { createAccessDeniedError, extractCallerService, isAccessAllowed, logAccessDenied } from "./access-control"
import { suggestClosestMethod } from "./levenshtein"
import { getDefaultLogger, NevoLogger } from "./logger"
import { LruIdempotencyCache } from "./idempotency"
import type { IdempotencyStore } from "./idempotency-store"
import { TwoTierIdempotency } from "./idempotency-runtime"
import { ReplayGuard } from "./replay-protection"
import { getSchemaFor, toValidator } from "./schema"
import { parseMethod, isVersionCompatible, DEFAULT_METHOD_VERSION } from "./version"
import { getDefaultMetrics, NEVO_METRIC_NAMES, methodLabel } from "./metrics"
import { getDefaultTracer, NevoTracer } from "./tracing"
import { DlqRouter } from "./dlq"
import { RateLimiter, resolveRateLimiter, RateLimiterOptions } from "./rate-limit"
import { NEVO_CONTRACT_METHOD, buildContract, ServiceContract } from "./contract"
import { NEVO_HEALTH_METHOD, NEVO_LIVENESS_METHOD, NEVO_READINESS_METHOD, HealthRegistry } from "./health"
import { getDevToolsBus, DevToolsBus } from "./devtools"
import { runInChain, resolveInboundChainId } from "./chain-context"
import { AuditLog } from "./audit-log"
import { assertTenantAllowed } from "./tenant-policy"

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
  protected readonly idempotency: LruIdempotencyCache<MessageResponse>
  /**
   * Optional distributed idempotency backend (Redis, Memcached, …).
   * When set, takes precedence over the local LRU on miss and is written
   * through on success. See `idempotency-store.ts`.
   */
  protected readonly distributedIdempotency?: IdempotencyStore<MessageResponse>
  /**
   * Shared two-tier idempotency runtime (L1 + claim-before-execute over the
   * distributed store). Single source of truth with the live signal-router path.
   */
  private readonly idem: TwoTierIdempotency<MessageResponse>
  protected readonly replayGuard: ReplayGuard
  protected readonly dlq: DlqRouter
  protected readonly tracer: NevoTracer | null
  protected readonly defaultVersion: string
  protected readonly rateLimiter: RateLimiter
  protected readonly healthRegistry?: HealthRegistry
  protected readonly instanceId?: string
  protected readonly capabilities?: string[]
  protected readonly serviceVersion?: string
  protected readonly devtoolsBus: DevToolsBus | null
  /**
   * Optional append-only audit log. Records every successful or failed
   * request when enabled. Sinks are pluggable — see `audit-log.ts`.
   */
  protected readonly auditLog?: AuditLog

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
      idempotencyStore?: IdempotencyStore<MessageResponse>
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
      /**
       * Append-only audit log of every request/response. When set, the
       * controller writes one redacted entry per call via `auditLog.recordFromResponse`.
       */
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
    this.idempotency = new LruIdempotencyCache<MessageResponse>(options?.idempotency)
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

  // Version-stripped method name for metric labels, bucketing any unregistered
  // / forged method (e.g. on METHOD_NOT_FOUND) to a single `<unknown>` series.
  // `hasOwnProperty` (not `in`) so inherited Object members like "toString" are
  // not mistaken for registered handlers.
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
      return { uuid, method, params: { result: "error", error: error.toJSON() }, meta }
    }

    this.logger.error({ event: "ctl.unexpected_error", method, err: error?.message || String(error) }, "Unexpected error")

    return {
      uuid,
      method,
      params: {
        result: "error",
        error: {
          code: ErrorCode.INTERNAL,
          message: !IS_PROD ? error?.message ?? String(error) : "Internal server error",
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

  async processMessage(data: any): Promise<MessageResponse> {
    const metrics = getDefaultMetrics()
    const nowMs = Date.now()
    const startMs = nowMs
    const { method, uuid, params, meta } = this.extractMessageData(data)
    let success = true

    // Establish a chain context for the duration of this handler so any
    // outbound calls picks up the same chain id via AsyncLocalStorage.
    // See `chain-context.ts` and the DevTools /traces view.
    const chainId = resolveInboundChainId(meta?.nevoChainId)
    return runInChain({ chainId, parentUuid: uuid }, () => this.runProcessMessage(data, method, uuid, params, meta, nowMs, startMs, success, chainId, metrics))
  }

  private async runProcessMessage(
    data: any,
    method: string,
    uuid: string,
    params: any,
    meta: MessageMeta | undefined,
    nowMs: number,
    startMs: number,
    successInit: boolean,
    chainId: string,
    metrics: ReturnType<typeof getDefaultMetrics>
  ): Promise<MessageResponse> {
    let success = successInit
    let idemKey: string | undefined
    let idemBegan = false
    let idemCommitted = false

    if (!this["__disableBuiltinHandlers"]) {
      const builtin = await this.handleBuiltinMethod(method, uuid, meta).catch(() => null)
      if (builtin) return builtin
    }

    const baseContext = { method, serviceName: this.serviceName, uuid, rawData: data, meta }
    const requestContext = { ...baseContext, params }
    let capturedError: any = null
    let finalResponse: MessageResponse | null = null
    let auditCaller: string | null = null

    try {
      await this.systemBeforeHook(requestContext)

      try {
        this.replayGuard.check(uuid, meta?.ts)
      } catch (err: any) {
        success = false
        await this.dlq.route({
          topic: this.serviceName,
          reason: "replay",
          error: err instanceof MessagingError ? err.toJSON() : { message: err?.message ?? String(err) },
          meta,
          rawPayload: data,
          ts: nowMs
        })
        return this.createErrorResponse(uuid, method, err, meta)
      }

      // Idempotency (claim-before-execute) via the shared two-tier runtime: dedup
      // on the wire-level idempotency key when the client stamped one, else the
      // envelope uuid. A hit returns the stored response; otherwise we hold the
      // claim until `finally` commits it (success) or releases it.
      idemKey = meta?.idempotencyKey || uuid
      if (idemKey && this.idem.isEnabled()) {
        const began = await this.idem.begin(idemKey)
        if (began.status === "hit") return began.value
        idemBegan = true
      }

      let processedParams = params
      if (this.beforeHook) {
        const hookResult = await this.beforeHook(requestContext)
        if (hookResult !== undefined) processedParams = hookResult
      }

      const callerService = await extractCallerService(meta, this.accessControl?.jwtVerifier)
      auditCaller = callerService ?? null
      const parsed = parseMethod(method)
      const topic = this.serviceName

      if (this.rateLimiter.isEnabled()) {
        this.rateLimiter.check({ topic, method: parsed.name, callerService, tenantId: meta?.tenantId, meta })
      }

      // Tenant kill-switch — checked after rate-limit so disabled tenants
      // are charged a token but get a clean `UNAUTHORIZED` response.
      assertTenantAllowed(this.serviceName, meta?.tenantId)

      if (!isAccessAllowed(this.accessControl, topic, parsed.name, callerService)) {
        logAccessDenied(this.accessControl, { topic, method, serviceName: this.serviceName, callerService })
        success = false
        finalResponse = {
          uuid,
          method,
          params: { result: "error", error: createAccessDeniedError(method, this.serviceName, callerService) },
          meta
        }
        return finalResponse
      }

      const handler = this.methodRegistry[parsed.name] ?? this.methodRegistry[method]
      if (!handler) {
        const suggestion = suggestClosestMethod(parsed.name, Object.keys(this.methodRegistry))
        const message = suggestion
          ? `Invalid method name '${parsed.name}', did you mean '${suggestion}'?`
          : `Method handler not found: ${parsed.name}`
        throw new MessagingError(ErrorCode.METHOD_NOT_FOUND, { message })
      }

      if (handler.version && parsed.version && !isVersionCompatible(parsed.version, handler.version)) {
        throw new MessagingError(ErrorCode.UNSUPPORTED_VERSION, {
          message: `Version mismatch for ${parsed.name}: requested ${parsed.version}, available ${handler.version}`
        })
      }

      const span = this.tracer?.startSpan(`nevo.serve ${this.serviceName}.${parsed.name}`, {
        "nevo.method": method,
        "nevo.service": this.serviceName
      })

      let result: unknown
      try {
        result = await this.executeHandler(handler, processedParams)
        span?.setStatus({ code: 1 })
      } catch (err) {
        span?.recordException(err)
        span?.setStatus({ code: 2, message: (err as Error)?.message })
        throw err
      } finally {
        span?.end()
      }
      const formattedResult = await this.formatResult(result)

      let response: MessageResponse = { uuid, method, params: { result: formattedResult }, meta }

      const responseContext = { ...baseContext, params: processedParams, result: formattedResult, response }

      if (this.afterHook) {
        const hookResponse = await this.afterHook(responseContext)
        if (hookResponse !== undefined) response = hookResponse
      }

      await this.systemAfterHook({ ...responseContext, response })

      // Commit through the shared runtime: L1 write + an AWAITED distributed
      // write-through (closing the window where a peer re-executes before the
      // result is stored — previously this distributed set was fire-and-forget).
      // Errors are not cached; the `finally` block releases the claim instead.
      if (idemKey && idemBegan && response.params.result !== "error") {
        await this.idem.commit(idemKey, response)
        idemCommitted = true
      }
      finalResponse = response
      return response
    } catch (error: any) {
      success = false
      capturedError = error
      if (this.debug) this.logger.debug({ event: "ctl.error", err: error?.message }, "Handler raised")
      try {
        await this.dlq.route({
          topic: this.serviceName,
          reason: "handler-error",
          error: error instanceof MessagingError ? error.toJSON() : { message: error?.message ?? String(error) },
          meta,
          rawPayload: data,
          ts: nowMs
        })
      } catch {}
      finalResponse = this.createErrorResponse(uuid, method, error, meta)
      return finalResponse
    } finally {
      const durationMs = Date.now() - startMs
      const labels = { service: this.serviceName, method: this.metricMethodLabel(method), status: success ? "ok" : "error" }
      metrics.incCounter(NEVO_METRIC_NAMES.requestsTotal, labels)
      if (!success) metrics.incCounter(NEVO_METRIC_NAMES.requestErrors, labels)
      metrics.observeHistogram(NEVO_METRIC_NAMES.requestDuration, labels, durationMs / 1000)
      if (this.devtoolsBus) {
        try {
          const err = capturedError
          this.devtoolsBus.publish({
            ts: nowMs,
            type: success ? "response" : "error",
            service: this.serviceName,
            method,
            uuid,
            chainId: meta?.nevoChainId ?? chainId,
            parentUuid: meta?.nevoParentUuid,
            durationMs,
            status: success ? "ok" : "error",
            error: err ? { code: err instanceof MessagingError ? err.code : err?.code, message: err?.message ?? String(err) } : undefined
          })
        } catch {}
      }
      if (this.auditLog?.isEnabled() && finalResponse) {
        // Fire-and-forget — never block request completion on audit writes.
        Promise.resolve(
          this.auditLog.recordFromResponse({
            service: this.serviceName,
            method,
            uuid,
            startedAt: startMs,
            params,
            response: finalResponse,
            meta,
            caller: auditCaller
          })
        ).catch(() => {})
      }
      // Release a still-held idempotency claim when no result was committed
      // (handler error, policy denial, early return) so a retry can re-execute
      // instead of polling a stranded sentinel until its TTL expires.
      if (idemKey && idemBegan && !idemCommitted) {
        try { await this.idem.release(idemKey) } catch {}
      }
    }
  }

  protected abstract extractMessageData(data: any): { method: string; uuid: string; params: any; meta?: MessageMeta }

  public abstract handleMessage(data: any): Promise<MessageResponse>
}
