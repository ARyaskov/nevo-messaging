import { ErrorCode, MessagingError, serializeBigInt } from "./"
import { AfterHook, BeforeHook, ServiceMethodHandler, ServiceMethodMapping, SystemAfterHook, SystemBeforeHook, MessageResponse } from "./types"

export abstract class BaseMessageController {
  protected readonly methodRegistry: ServiceMethodMapping = {}
  serviceInstances: any[] = []
  protected readonly serviceName: string
  protected readonly beforeHook?: BeforeHook
  protected readonly afterHook?: AfterHook
  protected readonly systemBeforeHook: SystemBeforeHook
  protected readonly systemAfterHook: SystemAfterHook
  protected readonly debug: boolean

  protected constructor(
    serviceName: string,
    serviceInstances: any[],
    methodHandlers: ServiceMethodMapping,
    options?: {
      onBefore?: BeforeHook
      onAfter?: AfterHook
      debug?: boolean
    }
  ) {
    this.serviceName = serviceName
    this.serviceInstances = serviceInstances || []
    this.beforeHook = options?.onBefore as any
    this.afterHook = options?.onAfter as any
    this.debug = options?.debug || false

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
    const { method, uuid, params } = this.extractMessageData(data)

    const baseContext = {
      method,
      serviceName: this.serviceName,
      uuid,
      rawData: data
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

      const handler = this.methodRegistry[method]
      if (!handler) {
        throw new MessagingError(ErrorCode.UNKNOWN, {
          message: `Method handler not found: ${method}`
        })
      }

      const result = await this.executeHandler(handler, processedParams)
      const formattedResult = await this.formatResult(result)

      let response: MessageResponse = {
        uuid,
        method,
        params: { result: formattedResult }
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

  protected abstract extractMessageData(data: any): { method: string; uuid: string; params: any }

  public abstract handleMessage(data: any): Promise<MessageResponse>
}
