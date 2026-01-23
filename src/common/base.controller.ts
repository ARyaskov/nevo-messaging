import { ErrorCode, MessagingError, serializeBigInt } from "./"
import {
  AfterHook,
  BeforeHook,
  ServiceMethodHandler,
  ServiceMethodMapping,
  SystemAfterHook,
  SystemBeforeHook,
  MessageResponse,
  AccessControlConfig,
  MessageMeta
} from "./types"
import { createAccessDeniedError, extractCallerService, isAccessAllowed, logAccessDenied } from "./access-control"

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

  protected constructor(
    serviceName: string,
    serviceInstances: any[],
    methodHandlers: ServiceMethodMapping,
    options?: {
      onBefore?: BeforeHook
      onAfter?: AfterHook
      debug?: boolean
      accessControl?: AccessControlConfig
    }
  ) {
    this.serviceName = serviceName
    this.serviceInstances = serviceInstances || []
    this.beforeHook = options?.onBefore as any
    this.afterHook = options?.onAfter as any
    this.debug = options?.debug || false
    this.accessControl = options?.accessControl

    this.systemBeforeHook = (context) => {
      if (this.debug) {
        console.log(`[${this.constructor.name}] Received:`, {
          method: context.method,
          uuid: context.uuid
        })
      }
    }

    this.systemAfterHook = (context) => {
      if (this.debug) {
        console.log(`[${this.constructor.name}] Responding:`, {
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

  protected findServiceInstance(methodName: string): any {
    for (const instance of this.serviceInstances) {
      if (typeof instance[methodName] === "function") {
        return instance
      }
    }
    return null
  }

  protected async executeHandler(handler: ServiceMethodHandler, params: unknown): Promise<unknown> {
    const { serviceMethod, paramTransformer, resultTransformer } = handler

    const serviceInstance = this.findServiceInstance(serviceMethod)

    if (!serviceInstance) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `No service found with method: ${serviceMethod}`
      })
    }

    const methodArgs = paramTransformer ? paramTransformer(params) : [params]

    try {
      const result = await serviceInstance[serviceMethod](...methodArgs)
      return resultTransformer ? resultTransformer(result) : result
    } catch (error: any) {
      if (error.message?.includes("is not a function")) {
        throw new MessagingError(ErrorCode.UNKNOWN, {
          message: `Method '${serviceMethod}' not found in service`
        })
      }
      throw error
    }
  }

  private suggestClosestMethod(method: string): string | null {
    const candidates = Object.keys(this.methodRegistry || {})
    if (!candidates.length) {
      return null
    }

    const normalized = method.toLowerCase()
    let best: { name: string; score: number } | null = null

    for (const candidate of candidates) {
      const score = this.levenshteinDistance(normalized, candidate.toLowerCase())
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

  private levenshteinDistance(a: string, b: string): number {
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

  protected async formatResult(result: unknown): Promise<unknown> {
    if (result instanceof Promise) {
      result = await result
    }

    return serializeBigInt(result)
  }

  protected createErrorResponse(uuid: string, method: string, error: any): MessageResponse {
    if (error instanceof MessagingError) {
      return {
        uuid,
        method,
        params: {
          result: "error",
          error: error.toJSON()
        }
      }
    }

    console.error(`Unexpected error in ${method}:`, error)

    return {
      uuid,
      method,
      params: {
        result: "error",
        error: {
          code: ErrorCode.UNKNOWN,
          message: process.env["NODE_ENV"] !== "production" ? error.message : "Internal server error",
          details: {},
          service: this.serviceName
        }
      }
    }
  }

  async processMessage(data: any): Promise<MessageResponse> {
    const { method, uuid, params, meta } = this.extractMessageData(data)

    const baseContext = {
      method,
      serviceName: this.serviceName,
      uuid,
      rawData: data,
      meta
    }

    const requestContext = {
      ...baseContext,
      params
    }

    try {
      await this.systemBeforeHook(requestContext)

      let processedParams = params
      if (this.beforeHook) {
        const hookResult = await this.beforeHook(requestContext)
        if (hookResult !== undefined) {
          processedParams = hookResult
        }
      }

      const callerService = extractCallerService(meta)
      const topic = this.serviceName

      if (!isAccessAllowed(this.accessControl, topic, method, callerService)) {
        logAccessDenied(this.accessControl, { topic, method, serviceName: this.serviceName, callerService })
        return {
          uuid,
          method,
          params: {
            result: "error",
            error: createAccessDeniedError(method, this.serviceName, callerService)
          },
          meta
        }
      }

      const handler = this.methodRegistry[method]
      if (!handler) {
        const suggestion = this.suggestClosestMethod(method)
        const message = suggestion ? `Invalid method name '${method}', did you mean '${suggestion}'?` : `Method handler not found: ${method}`
        throw new MessagingError(ErrorCode.UNKNOWN, {
          message
        })
      }

      const result = await this.executeHandler(handler, processedParams)
      const formattedResult = await this.formatResult(result)

      let response: MessageResponse = {
        uuid,
        method,
        params: { result: formattedResult },
        meta
      }

      const responseContext = {
        ...baseContext,
        params: processedParams,
        result: formattedResult,
        response
      }

      if (this.afterHook) {
        const hookResponse = await this.afterHook(responseContext)
        if (hookResponse !== undefined) {
          response = hookResponse
        }
      }

      await this.systemAfterHook({
        ...responseContext,
        response
      })

      return response
    } catch (error: any) {
      return this.createErrorResponse(uuid, method, error)
    }
  }

  protected abstract extractMessageData(data: any): { method: string; uuid: string; params: any; meta?: MessageMeta }

  public abstract handleMessage(data: any): Promise<MessageResponse>
}
