import "reflect-metadata"
import { Controller, Type } from "@nestjs/common"
import { BeforeHook, AfterHook, ServiceMethodMapping, AccessControlConfig, IdempotencyOptions, SecurityOptions, MetricsOptions, TracingOptions } from "./types"
import { getClassSignals, getNevoServiceName, SignalMetadata } from "../signal.decorator"
import { DEFAULT_METHOD_VERSION } from "./version"
import type { RateLimiter, RateLimiterOptions } from "./rate-limit"

export function createSignalRouterDecorator<T>(
  controllerClass: Type<T>,
  serviceType: Type<any> | Type<any>[],
  options?: {
    before?: BeforeHook
    after?: AfterHook
    debug?: boolean
    eventPattern?: string
    serviceName?: string
    accessControl?: AccessControlConfig
    idempotency?: IdempotencyOptions
    security?: SecurityOptions
    metrics?: MetricsOptions
    tracing?: TracingOptions
    defaultVersion?: string
    rateLimit?: RateLimiterOptions | RateLimiter
  }
) {
  return function (target: any): any {
    Controller()(target)
    const originalPrototype = target.prototype

    Reflect.defineProperty(target, "prototype", {
      value: {
        ...originalPrototype,

        onModuleInit() {
          if (originalPrototype.onModuleInit) {
            originalPrototype.onModuleInit.call(this)
          }

          const types = Array.isArray(serviceType) ? serviceType : [serviceType]
          const serviceInstances: any[] = []
          for (const t of types) {
            const propName = findPropertyNameByType(this, t)
            if (propName && this[propName]) serviceInstances.push(this[propName])
          }

          const signals = getClassSignals(target)

          const explicit = options?.serviceName || options?.eventPattern?.replace(/-events$/, "") || getNevoServiceName(target)
          const fallback = (target.name as string | undefined)?.toLowerCase().replace("controller", "") || "service"
          const serviceName = explicit || fallback

          const controller: any = new (controllerClass as any)(serviceName, serviceInstances, createHandlersFromSignals(signals), {
            onBefore: options?.before,
            onAfter: options?.after,
            debug: options?.debug,
            accessControl: options?.accessControl,
            idempotency: options?.idempotency,
            security: options?.security,
            metrics: options?.metrics,
            tracing: options?.tracing,
            defaultVersion: options?.defaultVersion ?? DEFAULT_METHOD_VERSION,
            rateLimit: options?.rateLimit
          })

          Object.getOwnPropertyNames(controller).forEach((key) => {
            if (key !== "constructor" && key !== "serviceInstances") {
              this[key] = controller[key]
            }
          })

          const controllerProto = Object.getPrototypeOf(controller)
          Object.getOwnPropertyNames(controllerProto).forEach((key) => {
            if (key !== "constructor" && typeof controllerProto[key] === "function") {
              this[key] = controllerProto[key].bind(this)
            }
          })
        }
      },
      writable: true,
      configurable: true
    })

    return target
  }
}

function findPropertyNameByType(instance: any, type: Type<any>): string | null {
  for (const key of Object.keys(instance)) {
    if (instance[key] instanceof type) {
      return key
    }
  }
  return null
}

function createHandlersFromSignals(signals: SignalMetadata[]): ServiceMethodMapping {
  return signals.reduce((handlers, signal) => {
    handlers[signal.signalName] = {
      serviceMethod: signal.methodName,
      paramTransformer: signal.paramTransformer,
      resultTransformer: signal.resultTransformer,
      options: signal.options,
      schema: signal.options?.schema,
      version: signal.version || signal.options?.version
    }
    return handlers
  }, {} as ServiceMethodMapping)
}
