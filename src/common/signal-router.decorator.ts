import "reflect-metadata"
import { Controller, Type } from "@nestjs/common"
import { BeforeHook, AfterHook, ServiceMethodMapping } from "./types"
import { getClassSignals, SignalMetadata } from "../signal.decorator"

export function createSignalRouterDecorator<T>(
  controllerClass: Type<T>,
  serviceType: Type<any> | Type<any>[],
  options?: {
    before?: BeforeHook
    after?: AfterHook
    debug?: boolean
    eventPattern?: string
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

          const serviceInstances = Array.isArray(serviceType)
            ? // @ts-ignore
              serviceType.map((type) => this[findPropertyNameByType(this, type)]).filter(Boolean)
            : // @ts-ignore
              [this[findPropertyNameByType(this, serviceType)]].filter(Boolean)

          const signals = getClassSignals(target)

          const serviceName = options?.eventPattern || target.name.toLowerCase().replace("controller", "")

          const controller: any = new controllerClass(serviceName, serviceInstances, createHandlersFromSignals(signals), {
            onBefore: options?.before,
            onAfter: options?.after,
            debug: options?.debug
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
      options: signal.options
    } as any
    return handlers
  }, {} as ServiceMethodMapping)
}
