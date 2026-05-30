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
import { createAccessDeniedError, extractCallerService, isAccessAllowed, logAccessDenied } from "./common/access-control"
import { ErrorCode } from "./common"
import { getClassSignals, getNevoServiceName, SignalMetadata } from "./signal.decorator"
import { suggestClosestMethod } from "./common/levenshtein"
import { getDefaultLogger, NevoLogger } from "./common/logger"
import { TwoTierIdempotency } from "./common/idempotency-runtime"
import type { IdempotencyStore } from "./common/idempotency-store"
import { AuditLog } from "./common/audit-log"
import { assertTenantAllowed } from "./common/tenant-policy"
import { ReplayGuard } from "./common/replay-protection"
import { getSchemaFor, toValidator } from "./common/schema"
import { parseMethod, isVersionCompatible, DEFAULT_METHOD_VERSION } from "./common/version"
import { getDefaultMetrics, NEVO_METRIC_NAMES, methodLabel } from "./common/metrics"
import { getDefaultTracer } from "./common/tracing"
import { matchesFilter } from "./common/subscription-filters"
import { DlqRouter } from "./common/dlq"
import { MessagingError } from "./common/errors"
import { RateLimiter, resolveRateLimiter, RateLimiterOptions } from "./common/rate-limit"
import { NEVO_CONTRACT_METHOD, buildContract, ContractMethodDescriptor } from "./common/contract"
import { NEVO_HEALTH_METHOD, NEVO_LIVENESS_METHOD, NEVO_READINESS_METHOD, HealthRegistry } from "./common/health"
import { getDevToolsBus, DevToolsBus } from "./common/devtools"
import { getDevToolsRegistry, describeMethodsFromSignals } from "./common/devtools-registry"
import { getMethodRateLimit, getMethodCacheable, rateLimitToOptions } from "./common/method-decorators"
import { LruIdempotencyCache as LruCache } from "./common/idempotency"
import { runInChain, resolveInboundChainId } from "./common/chain-context"
import { readMethodResilience, applyResilience } from "./common/resilience-runtime"

export interface SignalRouterOptions {
  before?: BeforeHook
  after?: AfterHook
  debug?: boolean
  eventPattern?: string
  serviceName?: string
  accessControl?: AccessControlConfig
  idempotency?: IdempotencyOptions
  /**
   * Optional distributed idempotency backend (Redis, …). On a miss the live
   * handler claims the key (claim-before-execute) and writes the result through
   * on success; concurrent duplicates across replicas await the winner's result
   * instead of re-running. See `idempotency-store.ts`.
   */
  idempotencyStore?: IdempotencyStore<MessageResponse>
  /**
   * Optional append-only audit log. When set, one redacted entry is recorded per
   * request (success or failure). See `audit-log.ts`.
   */
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
    const auditLog = options.auditLog
    const replayGuard = new ReplayGuard({
      enabled: (options.security?.replayWindowMs ?? 0) > 0,
      windowMs: options.security?.replayWindowMs
    })
    const dlq = options.dlq instanceof DlqRouter ? options.dlq : new DlqRouter({ enabled: (options.dlq as any)?.enabled === true })
    const defaultVersion = options.defaultVersion || DEFAULT_METHOD_VERSION
    const rateLimiter = options.rateLimit !== undefined ? resolveRateLimiter(options.rateLimit) : new RateLimiter()
    const devtoolsBus: DevToolsBus | null = options.devtools === false ? null : (options.devtools instanceof Object ? (options.devtools as DevToolsBus) : getDevToolsBus())
    const methodLimiters = new Map<string, RateLimiter>()
    const methodCaches = new Map<string, LruCache<unknown>>()

    const allSignals = getClassSignals(target) as SignalMetadata[]
    const signalsByName = new Map<string, SignalMetadata[]>()
    const contractDescriptors: ContractMethodDescriptor[] = []
    const signalNames: string[] = []
    for (const s of allSignals) {
      let arr = signalsByName.get(s.signalName)
      if (!arr) { arr = []; signalsByName.set(s.signalName, arr) }
      arr.push(s)
      if (!s.signalName.startsWith("nevo.")) {
        contractDescriptors.push({ signalName: s.signalName, version: s.version || DEFAULT_METHOD_VERSION })
        signalNames.push(s.signalName)
      }
    }
    contractDescriptors.sort((a, b) => a.signalName.localeCompare(b.signalName))

    // Methods this service actually serves — used to bucket forged / unregistered
    // method names (e.g. on METHOD_NOT_FOUND) to a single `<unknown>` metric label
    // so attacker-chosen strings cannot mint unbounded time series.
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

    target.prototype[handlerName] = async function (data: any) {
      // Peek at the envelope just enough to seed the chain context — every
      // outbound call made inside the handler will inherit this chain id via
      // AsyncLocalStorage, which is what makes the DevTools /traces view
      // possible.
      let peekedMeta: MessageMeta | undefined
      let peekedUuid: string | undefined
      try {
        const peek = messageExtractor(data)
        peekedMeta = peek?.meta
        peekedUuid = peek?.uuid
      } catch {}
      const chainId = resolveInboundChainId(peekedMeta?.nevoChainId)

      return runInChain({ chainId, parentUuid: peekedUuid }, async () => {
      const startMs = Date.now()
      const nowMs = Date.now()
      let response: MessageResponse | undefined
      let finalResponse: MessageResponse | undefined
      let messageData: MessageData | undefined
      let auditCaller: string | null = null
      let idemKey: string | undefined
      let idemBegan = false
      let idemCommitted = false

      try {
        if (debugEnabled) {
          logger.debug({ event: "signal.received", topic: eventPattern })
        }

        messageData = messageExtractor(data)
        const { method, params, uuid, meta } = messageData

        if (!method) {
          logger.error({ event: "signal.invalid", topic: eventPattern }, "Missing 'method' field in message")
          return createErrorResponse("Invalid message format", undefined, undefined, ErrorCode.BAD_REQUEST)
        }

        const parsed = parseMethod(method)
        const requestedVersion = parsed.version

        if (parsed.name === NEVO_CONTRACT_METHOD) {
          const contract = {
            protocol: "1",
            serviceName: serviceNameFromMeta,
            serviceVersion: options.serviceVersion,
            instanceId: options.instanceId,
            capabilities: options.capabilities,
            generatedAt: nowMs,
            methods: contractDescriptors
          }
          return { uuid, method, params: { result: contract as any }, meta }
        }
        if (options.health) {
          if (parsed.name === NEVO_HEALTH_METHOD) {
            const report = await options.health.report()
            return { uuid, method, params: { result: report as any }, meta }
          }
          if (parsed.name === NEVO_LIVENESS_METHOD) {
            const report = await options.health.liveness()
            return { uuid, method, params: { result: report as any }, meta }
          }
          if (parsed.name === NEVO_READINESS_METHOD) {
            const report = await options.health.readiness()
            return { uuid, method, params: { result: report as any }, meta }
          }
        }

        try {
          replayGuard.check(uuid, meta?.ts)
        } catch (err: any) {
          await dlq.route({
            topic: eventPattern,
            reason: "replay",
            error: err instanceof MessagingError ? { code: err.code, message: err.message } : { message: String(err) },
            meta,
            rawPayload: data,
            ts: nowMs
          })
          return createErrorResponse(err.message, uuid, method, ErrorCode.REPLAY_DETECTED, meta)
        }

        // Idempotency (claim-before-execute): dedup on the wire-level idempotency
        // key when the client stamped one, falling back to the envelope uuid (so
        // timeout-retries — which carry a fresh uuid but the same idempotencyKey —
        // collapse to one execution). A hit returns the stored response; otherwise
        // we hold the claim until `finally` commits it (success) or releases it.
        idemKey = meta?.idempotencyKey || uuid
        if (idemKey && idem.isEnabled()) {
          const began = await idem.begin(idemKey)
          if (began.status === "hit") return began.value
          idemBegan = true
        }

        if (debugEnabled) {
          logger.debug({ event: "signal.invoke", topic: eventPattern, method })
        }

        const serviceInstances = findServiceInstances(this, serviceType)
        if (serviceInstances.length === 0) {
          logger.error({ event: "signal.no_service", topic: eventPattern, serviceType: String(serviceType) }, "No service instances found")
          return createErrorResponse("Service not found", uuid, method, ErrorCode.SERVICE_NOT_FOUND, meta)
        }

        const callerService = await extractCallerService(meta, options.accessControl?.jwtVerifier)
        auditCaller = callerService ?? null
        const topic = eventPattern

        if (rateLimiter.isEnabled()) {
          try {
            rateLimiter.check({ topic, method: parsed.name, callerService, tenantId: meta?.tenantId, meta })
          } catch (err: any) {
            if (err instanceof MessagingError && err.code === ErrorCode.RATE_LIMITED) {
              return { uuid, method, params: { result: "error", error: err.toJSON() }, meta }
            }
            throw err
          }
        }

        // Tenant kill-switch — checked after rate-limit (a disabled tenant is
        // still charged a token) and before dispatch. Throws UNAUTHORIZED, which
        // the outer catch turns into a clean error response. Mirrors the order in
        // BaseMessageController.
        assertTenantAllowed(serviceNameFromMeta, meta?.tenantId)

        if (!isAccessAllowed(options.accessControl, topic, parsed.name, callerService)) {
          logAccessDenied(options.accessControl, { topic, method, serviceName: serviceNameFromMeta, callerService })
          response = {
            uuid,
            method,
            params: {
              result: "error",
              error: createAccessDeniedError(method, serviceNameFromMeta, callerService)
            },
            meta
          }
          return response
        }

        let processedParams = params
        if (options.before) {
          const baseContext = {
            method,
            serviceName: serviceNameFromMeta,
            uuid,
            rawData: data,
            params,
            meta
          }
          const hookResult = await options.before(baseContext)
          if (hookResult !== undefined) processedParams = hookResult
        }

        const candidates = signalsByName.get(parsed.name)
        let signalHandler: SignalMetadata | undefined

        if (!candidates || candidates.length === 0) {
          const suggestion = suggestClosestMethod(parsed.name, signalNames)
          const message = suggestion ? `Invalid method name '${parsed.name}', did you mean '${suggestion}'?` : `Method ${parsed.name} not found`
          return createErrorResponse(message, uuid, method, ErrorCode.METHOD_NOT_FOUND, meta)
        } else {
          if (requestedVersion) {
            signalHandler = candidates.find((c) => (c.version || defaultVersion) === requestedVersion)
          } else {
            signalHandler = candidates.find((c) => (c.version || defaultVersion) === defaultVersion) || candidates[0]
          }
          if (!signalHandler) {
            return createErrorResponse(`No handler matching version ${requestedVersion} for ${parsed.name}`, uuid, method, ErrorCode.UNSUPPORTED_VERSION, meta)
          }
          if (!isVersionCompatible(requestedVersion, signalHandler.version || defaultVersion)) {
            return createErrorResponse(
              `Method ${parsed.name} version mismatch (requested ${requestedVersion}, available ${signalHandler.version || defaultVersion})`,
              uuid,
              method,
              ErrorCode.UNSUPPORTED_VERSION,
              meta
            )
          }
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
          return createErrorResponse(`Method ${serviceMethod} does not exist`, uuid, method, ErrorCode.METHOD_NOT_FOUND, meta)
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
          } catch (err: any) {
            if (err instanceof MessagingError && err.code === ErrorCode.RATE_LIMITED) {
              return { uuid, method, params: { result: "error", error: err.toJSON() }, meta }
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
          cacheKey = cacheable.keyBy ? cacheable.keyBy(processedParams) : `${parsed.name}::${JSON.stringify(processedParams ?? {})}`
          if (methodCache.has(cacheKey)) {
            const cached = methodCache.get(cacheKey)
            return { uuid, method, params: { result: cached as any }, meta }
          }
        }

        const schema = signalHandler.options?.schema ?? getSchemaFor(serviceInstance, serviceMethod)
        if (schema) {
          const validator = toValidator(schema)
          if (validator) {
            try {
              processedParams = validator.parse(processedParams)
            } catch (err: any) {
              const errPayload = err instanceof MessagingError
                ? err.toJSON()
                : { code: ErrorCode.VALIDATION_FAILED, message: err?.message || "validation failed" }
              return { uuid, method, params: { result: "error", error: errPayload }, meta }
            }
          }
        }

        const args = signalHandler.paramTransformer ? signalHandler.paramTransformer(processedParams) : [processedParams]

        if (debugEnabled) {
          logger.debug({ event: "signal.call", topic: eventPattern, serviceMethod })
        }

        const span = tracer?.startSpan(`nevo.serve ${eventPattern}.${method}`, {
          "nevo.method": method,
          "nevo.service": serviceNameFromMeta,
          "nevo.uuid": uuid ?? "",
          "nevo.caller": callerService ?? ""
        })

        // Materialise any @Hedge/@CircuitBreaker/@Adaptive/@Backpressure on the
        // target method. When present, the real handler invocation runs through
        // the resilience runtime (keyed by `service:method`); otherwise it is a
        // plain call — zero overhead on undecorated handlers.
        const resilience = readMethodResilience(serviceInstance, serviceMethod)

        let result: unknown
        try {
          const invoke = () => serviceInstance[serviceMethod](...args)
          result = resilience
            ? await applyResilience({ config: resilience, ctx: { key: `${serviceNameFromMeta}:${parsed.name}` }, invoke })
            : await invoke()
          span?.setStatus({ code: 1 })
        } catch (err) {
          span?.recordException(err)
          span?.setStatus({ code: 2, message: (err as Error)?.message })
          // A @Backpressure admission failure surfaces as RATE_LIMITED — that's
          // intentional load-shedding, not a handler crash, so reply with a
          // clean error response instead of throwing into the DLQ / error path.
          if (err instanceof MessagingError && err.code === ErrorCode.RATE_LIMITED) {
            response = { uuid, method, params: { result: "error", error: err.toJSON() }, meta }
            return response
          }
          throw err
        } finally {
          span?.end()
        }

        const transformedResult = signalHandler.resultTransformer ? signalHandler.resultTransformer(result) : result
        if (debugEnabled) {
          logger.debug({ event: "signal.result", topic: eventPattern })
        }

        response = {
          uuid,
          method,
          params: { result: transformedResult },
          meta
        }

        if (options.after) {
          const responseContext = {
            method,
            serviceName: serviceNameFromMeta,
            uuid,
            rawData: data,
            params: processedParams,
            result: transformedResult,
            response,
            meta
          }
          const hookResponse = await options.after(responseContext)
          if (hookResponse !== undefined) response = hookResponse
        }

        // Commit the idempotency result (L1 + AWAITED distributed write-through)
        // so a peer replica observes it before we ack. Errors are not cached —
        // the `finally` block releases the claim so a retry can re-execute.
        if (idemKey && idemBegan && response.params.result !== "error") {
          await idem.commit(idemKey, response)
          idemCommitted = true
        }
        if (methodCache && cacheKey && response.params.result !== "error") {
          methodCache.set(cacheKey, response.params.result)
        }
        finalResponse = response
        return response
      } catch (error: any) {
        const errorMessage = error instanceof Error ? error.message : "Unknown error"
        const code = error instanceof MessagingError ? error.code : error?.code ?? ErrorCode.UNKNOWN
        logger.error({ event: "signal.error", topic: eventPattern, method: messageData?.method, code, err: errorMessage }, "Processing error")
        try {
          await dlq.route({
            topic: eventPattern,
            reason: "handler-error",
            error: error instanceof MessagingError ? error.toJSON() : { message: errorMessage },
            meta: messageData?.meta,
            rawPayload: data,
            ts: nowMs
          })
        } catch {}
        finalResponse = createErrorResponse(errorMessage, messageData?.uuid ?? (data as any)?.uuid, messageData?.method ?? (data as any)?.method, code, messageData?.meta) as MessageResponse
        return finalResponse
      } finally {
        const durationMs = Date.now() - startMs
        // Use the final response (the catch path sets `finalResponse`, not
        // `response`) so handler errors are recorded as errors, not "ok".
        const success = (finalResponse ?? response)?.params?.result !== "error"
        const labels = {
          service: serviceNameFromMeta,
          method: methodLabel(messageData?.method, (name) => knownMethodNames.has(name)),
          status: success ? "ok" : "error"
        }
        metrics.incCounter(NEVO_METRIC_NAMES.requestsTotal, labels)
        if (!success) metrics.incCounter(NEVO_METRIC_NAMES.requestErrors, labels)
        metrics.observeHistogram(NEVO_METRIC_NAMES.requestDuration, labels, durationMs / 1000)
        if (devtoolsBus) {
          try {
            const errPayload = (response as any)?.params?.error as any
            devtoolsBus.publish({
              ts: nowMs,
              type: success ? "response" : "error",
              service: serviceNameFromMeta,
              method: messageData?.method,
              uuid: messageData?.uuid,
              chainId: messageData?.meta?.nevoChainId ?? chainId,
              parentUuid: (messageData?.meta?.nevoParentUuid as string | undefined) ?? undefined,
              durationMs,
              status: success ? "ok" : "error",
              error: errPayload ? { code: errPayload.code, message: errPayload.message } : undefined
            })
          } catch {}
        }

        // Release a still-held idempotency claim when no result was committed
        // (handler error, policy denial, early return) so a retry can re-execute
        // instead of polling a stranded sentinel until its TTL expires.
        if (idemKey && idemBegan && !idemCommitted) {
          try { await idem.release(idemKey) } catch {}
        }

        // Append-only audit — fire-and-forget, never block request completion.
        if (auditLog?.isEnabled()) {
          const auditResponse = finalResponse ?? response
          if (auditResponse) {
            Promise.resolve(
              auditLog.recordFromResponse({
                service: serviceNameFromMeta,
                method: messageData?.method ?? "unknown",
                uuid: messageData?.uuid ?? "",
                startedAt: startMs,
                params: messageData?.params,
                response: auditResponse,
                meta: messageData?.meta,
                caller: auditCaller
              })
            ).catch(() => {})
          }
        }
      }
      })
    }

    registerHandler(target, eventPattern, handlerName)

    return target
  }
}

export { matchesFilter, parseMethod, DEFAULT_METHOD_VERSION }
