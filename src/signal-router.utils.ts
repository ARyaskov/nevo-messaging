import { Type } from "@nestjs/common"
import { BeforeHook, AfterHook, stringifyWithBigInt, parseWithBigInt, serializeBigInt } from "./common"
import { getClassSignals } from "./signal.decorator"

export interface SignalRouterOptions {
  before?: BeforeHook
  after?: AfterHook
  debug?: boolean
  eventPattern?: string
}

export interface MessageData {
  method: string
  params: any
  uuid: string
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
        const { method, params, uuid } = messageData

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

        let processedParams = params
        if (options.before) {
          const hookResult = await options.before({
            method,
            serviceName: eventPattern,
            uuid,
            rawData: data,
            params
          })
          if (hookResult !== undefined) {
            processedParams = hookResult
          }
        }

        const signals = getClassSignals(target)
        const signalHandler = signals.find((s) => s.signalName === method)

        if (!signalHandler) {
          console.error(`No handler found for method:`, method)
          return createErrorResponse(`Method ${method} not found`, uuid, method)
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

        let response = {
          uuid,
          method,
          params: { result: serializedResult }
        }

        if (options.after) {
          const hookResponse = await options.after({
            method,
            serviceName: eventPattern,
            uuid,
            rawData: data,
            params: processedParams,
            result: serializedResult,
            response
          })
          if (hookResponse !== undefined) {
            response = hookResponse
          }
        }

        return response
      } catch (error: any) {
        const errorMessage = error instanceof Error ? error.message : "Unknown error"
        console.error(`[${eventPattern}] Processing error:`, error)
        return createErrorResponse(errorMessage, data.uuid, data.method, error.code)
      }
    }

    registerHandler(target, eventPattern, handlerName)

    return target
  }
}
