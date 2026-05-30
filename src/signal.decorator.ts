import "reflect-metadata"
import { DEFAULT_METHOD_VERSION } from "./common/version"

export const SIGNALS_METADATA_KEY = "nevo:signals"
export const LEGACY_SIGNALS_METADATA_KEY = "kafka:signals"

export interface SignalOptions {
  version?: string
  schema?: unknown
  resultSchema?: unknown
  [key: string]: any
}

export interface SignalMetadata {
  signalName: string
  methodName: string
  paramTransformer?: (data: any) => any[]
  resultTransformer?: (result: any) => any
  options?: SignalOptions
  version?: string
}

export function addSignalMetadata(
  target: any,
  signalName: string,
  methodName: string,
  paramTransformer?: (data: any) => any[],
  resultTransformer?: (result: any) => any,
  options?: SignalOptions
) {
  const own = Reflect.getOwnMetadata(SIGNALS_METADATA_KEY, target) as SignalMetadata[] | undefined
  const list: SignalMetadata[] = own ? [...own] : [...(Reflect.getMetadata(SIGNALS_METADATA_KEY, target) || [])]

  list.push({
    signalName,
    methodName,
    paramTransformer,
    resultTransformer,
    options,
    version: options?.version || DEFAULT_METHOD_VERSION
  })

  Reflect.defineMetadata(SIGNALS_METADATA_KEY, list, target)
  Reflect.defineMetadata(LEGACY_SIGNALS_METADATA_KEY, list, target)
}

export function Signal(
  signalName: string,
  methodNameOrParamTransformer?: string | ((data: any) => any[]),
  paramTransformerOrOptions?: ((data: any) => any[]) | ((result: any) => any) | SignalOptions,
  resultTransformerOrOptions?: ((result: any) => any) | SignalOptions,
  options?: SignalOptions
): MethodDecorator {
  return function (target: any, _propertyKey: string | symbol, descriptor: PropertyDescriptor): any {
    let methodName: string
    let paramTransformer: ((data: any) => any[]) | undefined
    let resultTransformer: ((result: any) => any) | undefined
    let signalOptions: SignalOptions | undefined

    if (typeof methodNameOrParamTransformer === "function") {
      methodName = signalName
      paramTransformer = methodNameOrParamTransformer
      if (typeof paramTransformerOrOptions === "function") {
        resultTransformer = paramTransformerOrOptions as (result: any) => any
        signalOptions = resultTransformerOrOptions as SignalOptions
      } else {
        signalOptions = paramTransformerOrOptions as SignalOptions
      }
    } else if (typeof methodNameOrParamTransformer === "string") {
      methodName = methodNameOrParamTransformer
      if (typeof paramTransformerOrOptions === "function") {
        paramTransformer = paramTransformerOrOptions as (data: any) => any[]
        if (typeof resultTransformerOrOptions === "function") {
          resultTransformer = resultTransformerOrOptions as (result: any) => any
          signalOptions = options
        } else {
          signalOptions = resultTransformerOrOptions as SignalOptions
        }
      } else {
        signalOptions = paramTransformerOrOptions as SignalOptions
      }
    } else {
      methodName = signalName
    }

    addSignalMetadata(target.constructor, signalName, methodName, paramTransformer, resultTransformer, signalOptions)

    return descriptor
  } as MethodDecorator
}

export function getClassSignals(target: any): SignalMetadata[] {
  const collected: SignalMetadata[] = []
  let cur = target
  while (cur && cur !== Function.prototype && cur !== Object) {
    const own = Reflect.getOwnMetadata(SIGNALS_METADATA_KEY, cur) as SignalMetadata[] | undefined
    if (own && own.length) {
      collected.push(...own)
    } else if (cur === target) {
      const legacy = Reflect.getOwnMetadata(LEGACY_SIGNALS_METADATA_KEY, cur) as SignalMetadata[] | undefined
      if (legacy) collected.push(...legacy)
    }
    cur = Object.getPrototypeOf(cur)
  }
  const seen = new Set<string>()
  const out: SignalMetadata[] = []
  for (const s of collected) {
    const k = `${s.signalName}@${s.version ?? DEFAULT_METHOD_VERSION}`
    if (seen.has(k)) continue
    seen.add(k)
    out.push(s)
  }
  return out
}

export const NEVO_SERVICE_NAME_METADATA = "nevo:service:name"

export function NevoService(name: string): ClassDecorator {
  return (target: any) => {
    Reflect.defineMetadata(NEVO_SERVICE_NAME_METADATA, name, target)
  }
}

export function getNevoServiceName(target: any): string | undefined {
  return Reflect.getMetadata(NEVO_SERVICE_NAME_METADATA, target) as string | undefined
}
