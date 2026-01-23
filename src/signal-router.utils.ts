import { Type } from "@nestjs/common"
import {
  BeforeHook,
  AfterHook,
  stringifyWithBigInt,
  serializeBigInt,
  AccessControlConfig,
  MessageMeta,
  MessageResponse
} from "./common"
import { createAccessDeniedError, extractCallerService, isAccessAllowed, logAccessDenied } from "./common/access-control"
import { ErrorCode } from "./common"
import { getClassSignals } from "./signal.decorator"

export interface SignalRouterOptions {
  before?: BeforeHook
  after?: AfterHook
  debug?: boolean
  eventPattern?: string
  accessControl?: AccessControlConfig
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
  return Array.isArray(serviceType)
    ? // @ts-ignore
      serviceType.map((type) => instance[findPropertyByType(instance, type)]).filter(Boolean)
    : // @ts-ignore
      [instance[findPropertyByType(instance, serviceType)]].filter(Boolean)
}

export function createErrorResponse(message: string, uuid?: string, method?: string, code: number = 0) {
  return {
    uuid,
    method,
    params: {
      result: "error",
      error: {
        message,
        code
      }
    }
  }
}

function levenshteinDistance(a: string, b: string): number {
  if (a === b) {
    return 0
  }
  if (!a.length) {
    return b.length
  }
  if (!b.length) {
    return a.length
  }

  const matrix: number[][] = Array.from({ length: a.length + 1 }, () => [])

  for (let i = 0; i <= a.length; i++) {
    matrix[i][0] = i
  }
  for (let j = 0; j <= b.length; j++) {
    matrix[0][j] = j
  }

  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1
      matrix[i][j] = Math.min(matrix[i - 1][j] + 1, matrix[i][j - 1] + 1, matrix[i - 1][j - 1] + cost)
    }
  }

  return matrix[a.length][b.length]
}

function suggestClosestMethod(method: string, candidates: string[]): string | null {
  if (!candidates.length) {
    return null
  }

  const normalized = method.toLowerCase()
  let best: { name: string; score: number } | null = null

  for (const candidate of candidates) {
    const score = levenshteinDistance(normalized, candidate.toLowerCase())
    if (!best || score < best.score) {
      best = { name: candidate, score }
    }
  }

  if (!best) {
    return null
  }

  const threshold = Math.max(2, Math.floor(method.length * 0.4))
  return best.score <= threshold ? best.name : null
}

export function createSignalRouterDecorator(
  serviceType: Type<any> | Type<any>[],
  options: SignalRouterOptions = {},
  messageExtractor: MessageExtractor,
  registerHandler: (target: any, eventPattern: string, handlerName: string, context?: any) => void
) {
  const debug = options?.debug || process.env["NODE_ENV"] !== "production"

  return function (target: any): any {
    const eventPattern = options?.eventPattern || target.name.toLowerCase().replace("controller", "") + "-events"

    const handlerName = "handleSignalMessage"

    target.prototype[handlerName] = async function (data: any) {
      try {
        if (debug) {
          console.log(`[${eventPattern}] Received message:`, stringifyWithBigInt(data))
        }

        const messageData = messageExtractor(data)
        const { method, params, uuid, meta } = messageData

        if (!method) {
          console.error("Missing 'method' field in message")
          return createErrorResponse("Invalid message format")
        }

        if (debug) {
          console.log(`[${eventPattern}] Invoking method:`, method)
        }

        const serviceInstances = findServiceInstances(this, serviceType)
        if (serviceInstances.length === 0) {
          console.error(`No service instances found for:`, serviceType)
          return createErrorResponse("Service not found", uuid, method)
        }

        const callerService = extractCallerService(meta)
        const topic = eventPattern

        if (!isAccessAllowed(options.accessControl, topic, method, callerService)) {
          logAccessDenied(options.accessControl, { topic, method, serviceName: eventPattern, callerService })
          return {
            uuid,
            method,
            params: {
              result: "error",
              error: createAccessDeniedError(method, eventPattern, callerService)
            },
            meta
          }
        }

        let processedParams = params
        if (options.before) {
          const hookResult = await options.before({
            method,
            serviceName: eventPattern,
            uuid,
            rawData: data,
            params,
            meta
          })
          if (hookResult !== undefined) {
            processedParams = hookResult
          }
        }

        const signals = getClassSignals(target)
        const signalHandler = signals.find((s) => s.signalName === method)

        if (!signalHandler) {
          console.error(`No handler found for method:`, method)
          const suggestion = suggestClosestMethod(
            method,
            signals.map((s) => s.signalName)
          )
          const message = suggestion ? `Invalid method name '${method}', did you mean '${suggestion}'?` : `Method ${method} not found`
          return createErrorResponse(message, uuid, method)
        }

        let serviceInstance = null
        const serviceMethod = signalHandler.methodName

        for (let s of serviceInstances) {
          if (s[serviceMethod]) {
            serviceInstance = s
          }
        }

        if (!serviceInstance[serviceMethod]) {
          console.error(`Method ${serviceMethod} not found in service`)
          return createErrorResponse(`Method ${serviceMethod} does not exist`, uuid, method)
        }

        const args = signalHandler.paramTransformer ? signalHandler.paramTransformer(processedParams) : [processedParams]

        if (debug) {
          console.log(`[${eventPattern}] Calling ${serviceMethod} with parameters:`, stringifyWithBigInt(args))
        }

        const result = await serviceInstance[serviceMethod](...args)

        const transformedResult = signalHandler.resultTransformer ? signalHandler.resultTransformer(result) : result
        const serializedResult = serializeBigInt(transformedResult)

        if (debug) {
          console.log(`[${eventPattern}] Result:`, stringifyWithBigInt(serializedResult))
        }

        let response: MessageResponse = {
          uuid,
          method,
          params: { result: serializedResult },
          meta
        }

        if (options.after) {
          const hookResponse = await options.after({
            method,
            serviceName: eventPattern,
            uuid,
            rawData: data,
            params: processedParams,
            result: serializedResult,
            response,
            meta
          })
          if (hookResponse !== undefined) {
            response = hookResponse
          }
        }

        return response
      } catch (error: any) {
        const errorMessage = error instanceof Error ? error.message : "Unknown error"
        console.error(`[${eventPattern}] Processing error:`, error)
        return createErrorResponse(errorMessage, data.uuid, data.method, error.code || ErrorCode.UNKNOWN)
      }
    }

    registerHandler(target, eventPattern, handlerName)

    return target
  }
}
