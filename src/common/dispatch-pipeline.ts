import type { AccessControlConfig, AfterHook, BeforeHook, ErrorDetails, MessageMeta, MessageResponse } from "./types"
import { ErrorCode } from "./error-code"
import { MessagingError } from "./errors"
import { IS_PROD } from "./env"
import { createAccessDeniedError, extractCallerService, isAccessAllowed, logAccessDenied } from "./access-control"
import { assertTenantAllowed } from "./tenant-policy"
import { parseMethod, type ParsedMethod } from "./version"
import { ReplayGuard } from "./replay-protection"
import { RateLimiter } from "./rate-limit"
import { TwoTierIdempotency } from "./idempotency-runtime"
import { payloadFingerprint, serverIdempotencyKey } from "./idempotency"
import { DlqRouter } from "./dlq"
import { AuditLog } from "./audit-log"
import { NEVO_METRIC_NAMES, type MetricsRegistry } from "./metrics"
import { DevToolsBus } from "./devtools"
import type { NevoLogger } from "./logger"

/**
 * The single inbound dispatch pipeline shared by the signal-router decorator and
 * BaseMessageController: replay guard → caller identity → rate limit → tenant →
 * ACL → builtins → idempotency claim → before hook → dispatch strategy → after
 * hook → idempotency commit, with unified metrics/devtools/audit/DLQ bookkeeping.
 * Only method resolution and invocation (the strategy) differ between hosts.
 */

export interface DispatchPipelineConfig {
  serviceName: string
  topic: string
  logger: NevoLogger
  metrics: MetricsRegistry
  devtoolsBus: DevToolsBus | null
  auditLog?: AuditLog
  replayGuard: ReplayGuard
  rateLimiter: RateLimiter
  idem: TwoTierIdempotency<MessageResponse>
  dlq: DlqRouter
  accessControl?: AccessControlConfig
  before?: BeforeHook
  after?: AfterHook
  /** Metric label for a method (host decides which names are "known"). */
  methodLabelFor: (method: string | undefined) => string
  /** Contract/health built-ins; null → not a builtin method. */
  builtin?: (parsedName: string, method: string, uuid: string, meta?: MessageMeta) => Promise<MessageResponse | null>
  disableBuiltinHandlers?: boolean
}

export interface DispatchRequest {
  data: unknown
  method: string
  uuid: string
  params: unknown
  meta?: MessageMeta
  chainId: string
  startMs: number
}

export interface DispatchStrategyArgs {
  parsed: ParsedMethod
  processedParams: unknown
  uuid: string
  method: string
  meta: MessageMeta | undefined
  callerService: string | undefined
}

export interface DispatchStrategyResult {
  response: MessageResponse
  /** Raw handler result (absent for error responses). */
  result?: unknown
  /** Runs once the final (post after-hook) successful response is settled. */
  onCommitted?: (response: MessageResponse) => void
  /**
   * A short-circuit response (e.g. a method-cache hit) that already carries a
   * fully-shaped result: skip the after hook so it isn't applied twice.
   */
  skipAfterHook?: boolean
}

export type DispatchStrategy = (args: DispatchStrategyArgs) => Promise<DispatchStrategyResult>

// Detail keys a caller needs to act on; in production everything else is dropped.
// `errors` stays because validation issues describe the caller's own input.
const PUBLIC_ERROR_DETAIL_KEYS = ["retryable", "retryAfterMs", "size", "limit", "httpStatus", "maxPhysicalCalls", "errors"] as const

function publicErrorDetails(details: Record<string, unknown> | undefined): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  if (!details) return out
  for (const key of PUBLIC_ERROR_DETAIL_KEYS) {
    if (Object.prototype.hasOwnProperty.call(details, key)) out[key] = details[key]
  }
  return out
}

/** MessagingError → wire shape, with `details` reduced to the public set in production. */
export function toWireError(error: MessagingError): ErrorDetails {
  const shaped = error.toJSON()
  if (IS_PROD) shaped.details = publicErrorDetails(shaped.details)
  return shaped
}

/** Uniform error → wire response shaping; internal messages never leak in prod. */
export function shapeDispatchError(
  serviceName: string,
  uuid: string | undefined,
  method: string | undefined,
  error: unknown,
  meta?: MessageMeta
): MessageResponse {
  if (error instanceof MessagingError) {
    return { uuid: uuid as string, method: method as string, params: { result: "error", error: toWireError(error) }, meta }
  }
  const err = error as { code?: unknown; message?: string } | undefined
  const code = typeof err?.code === "number" ? err.code : ErrorCode.INTERNAL
  const message = !IS_PROD ? (err?.message ?? String(error)) : "Internal server error"
  return {
    uuid: uuid as string,
    method: method as string,
    params: { result: "error", error: { code, message, details: {}, service: serviceName } },
    meta
  }
}

export async function runDispatchPipeline(cfg: DispatchPipelineConfig, req: DispatchRequest, strategy: DispatchStrategy): Promise<MessageResponse> {
  const { data, method, uuid, params, meta } = req
  let response: MessageResponse | undefined
  let finalResponse: MessageResponse | undefined
  let auditCaller: string | null = null
  let idemKey: string | undefined
  let idemFingerprint: string | undefined
  let idemBegan = false
  let idemCommitted = false
  let capturedError: unknown = null

  try {
    if (!method) {
      cfg.logger.error({ event: "dispatch.invalid", topic: cfg.topic }, "Missing 'method' field in message")
      response = shapeDispatchError(
        cfg.serviceName,
        uuid,
        method,
        new MessagingError(ErrorCode.BAD_REQUEST, { message: "Invalid message format" }),
        meta
      )
      return response
    }
    const parsed = parseMethod(method)

    try {
      cfg.replayGuard.check(uuid, meta?.ts)
    } catch (err) {
      await cfg.dlq.route({
        topic: cfg.topic,
        reason: "replay",
        error: err instanceof MessagingError ? { code: err.code, message: err.message } : { message: String(err) },
        meta,
        rawPayload: data,
        ts: req.startMs
      })
      response = shapeDispatchError(cfg.serviceName, uuid, method, err, meta)
      return response
    }

    const callerService = await extractCallerService(meta, cfg.accessControl?.jwtVerifier)
    auditCaller = callerService ?? null

    if (cfg.rateLimiter.isEnabled()) {
      try {
        cfg.rateLimiter.check({ topic: cfg.topic, method: parsed.name, callerService, tenantId: meta?.tenantId, meta })
      } catch (err) {
        // Load shedding is an expected outcome, not a dead letter.
        if (err instanceof MessagingError && err.code === ErrorCode.RATE_LIMITED) {
          response = { uuid, method, params: { result: "error", error: toWireError(err) }, meta }
          return response
        }
        throw err
      }
    }

    assertTenantAllowed(cfg.serviceName, meta?.tenantId)

    if (!isAccessAllowed(cfg.accessControl, cfg.topic, parsed.name, callerService)) {
      logAccessDenied(cfg.accessControl, { topic: cfg.topic, method, serviceName: cfg.serviceName, callerService })
      response = { uuid, method, params: { result: "error", error: createAccessDeniedError(method, cfg.serviceName, callerService) }, meta }
      return response
    }

    if (!cfg.disableBuiltinHandlers && cfg.builtin) {
      const builtinResponse = await cfg.builtin(parsed.name, method, uuid, meta)
      if (builtinResponse) {
        response = builtinResponse
        return response
      }
    }

    const suppliedIdemKey = meta?.idempotencyKey
    idemKey = serverIdempotencyKey({
      callerService,
      tenantId: meta?.tenantId,
      method,
      suppliedKey: suppliedIdemKey,
      envelopeUuid: uuid
    })
    // Only a caller-supplied key can be reused across payloads; an envelope uuid can't.
    idemFingerprint = suppliedIdemKey ? payloadFingerprint(params) : undefined
    if (idemKey && cfg.idem.isEnabled()) {
      const began = await cfg.idem.begin(idemKey, idemFingerprint)
      if (began.status === "conflict") {
        response = shapeDispatchError(
          cfg.serviceName,
          uuid,
          method,
          new MessagingError(ErrorCode.IDEMPOTENCY_KEY_CONFLICT, {
            message: `Idempotency key was already used for a different request payload on ${method}`
          }),
          meta
        )
        return response
      }
      if (began.status === "hit") {
        response = { ...began.value, uuid, meta }
        return response
      }
      idemBegan = true
    }

    let processedParams = params
    if (cfg.before) {
      const hookResult = await cfg.before({ method, serviceName: cfg.serviceName, uuid, rawData: data, params, meta })
      if (hookResult !== undefined) processedParams = hookResult
    }

    const dispatched = await strategy({ parsed, processedParams, uuid, method, meta, callerService })
    response = dispatched.response

    if (cfg.after && response.params.result !== "error" && !dispatched.skipAfterHook) {
      const hookResponse = await cfg.after({
        method,
        serviceName: cfg.serviceName,
        uuid,
        rawData: data,
        params: processedParams,
        result: dispatched.result,
        response,
        meta
      })
      if (hookResponse !== undefined) response = hookResponse
    }

    if (response.params.result !== "error") {
      if (idemKey && idemBegan) {
        await cfg.idem.commit(idemKey, response, idemFingerprint)
        idemCommitted = true
      }
      dispatched.onCommitted?.(response)
    }
    finalResponse = response
    return response
  } catch (error: unknown) {
    capturedError = error
    const errorMessage = error instanceof Error ? error.message : String(error)
    const code = error instanceof MessagingError ? error.code : ((error as { code?: unknown })?.code ?? ErrorCode.UNKNOWN)
    cfg.logger.error({ event: "dispatch.error", topic: cfg.topic, method, code, err: errorMessage }, "Processing error")
    try {
      await cfg.dlq.route({
        topic: cfg.topic,
        reason: "handler-error",
        error: error instanceof MessagingError ? error.toJSON() : { message: errorMessage },
        meta,
        rawPayload: data,
        ts: req.startMs
      })
    } catch (dlqErr) {
      cfg.logger.error(
        { event: "dispatch.dlq_failed", topic: cfg.topic, method, err: (dlqErr as Error)?.message ?? String(dlqErr) },
        "Failed to route a dead letter; the payload is not recoverable from the DLQ"
      )
    }
    finalResponse = shapeDispatchError(
      cfg.serviceName,
      uuid ?? (data as { uuid?: string })?.uuid,
      method ?? (data as { method?: string })?.method,
      error,
      meta
    )
    return finalResponse
  } finally {
    const durationMs = Date.now() - req.startMs
    const settled = finalResponse ?? response
    const success = settled?.params?.result !== "error"
    const labels = { service: cfg.serviceName, method: cfg.methodLabelFor(method), status: success ? "ok" : "error" }
    cfg.metrics.incCounter(NEVO_METRIC_NAMES.requestsTotal, labels)
    if (!success) cfg.metrics.incCounter(NEVO_METRIC_NAMES.requestErrors, labels)
    cfg.metrics.observeHistogram(NEVO_METRIC_NAMES.requestDuration, labels, durationMs / 1000)
    if (cfg.devtoolsBus) {
      try {
        const errDetails = settled?.params?.error
        const err = capturedError as { code?: number; message?: string } | null
        cfg.devtoolsBus.publish({
          ts: req.startMs,
          type: success ? "response" : "error",
          service: cfg.serviceName,
          method,
          uuid,
          chainId: meta?.nevoChainId ?? req.chainId,
          parentUuid: meta?.nevoParentUuid as string | undefined,
          durationMs,
          status: success ? "ok" : "error",
          error: success
            ? undefined
            : {
                code: errDetails?.code ?? (err && typeof err.code === "number" ? err.code : undefined),
                message: errDetails?.message ?? err?.message
              }
        })
      } catch {}
    }
    // Release a still-held idempotency claim when nothing was committed.
    if (idemKey && idemBegan && !idemCommitted) {
      try {
        await cfg.idem.release(idemKey)
      } catch (releaseErr) {
        cfg.logger.warn(
          { event: "dispatch.idem_release_failed", method, err: (releaseErr as Error)?.message },
          "Could not release the idempotency claim; retries wait for its TTL"
        )
      }
    }
    if (cfg.auditLog?.isEnabled() && settled) {
      // Fire-and-forget.
      Promise.resolve(
        cfg.auditLog.recordFromResponse({
          service: cfg.serviceName,
          method: method || "unknown",
          uuid: uuid || "",
          startedAt: req.startMs,
          params,
          response: settled,
          meta,
          caller: auditCaller
        })
      ).catch(() => {})
    }
  }
}
