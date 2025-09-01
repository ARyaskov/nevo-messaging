import { ServiceMethodHandler, ServiceMethodMapping } from "./types"

export type MethodOf<T> = {
  [K in keyof T]: T[K] extends (...args: any[]) => any ? K : never
}[keyof T] &
  string

export type ServiceMethod<TParams = unknown, TResult = unknown> = {
  params: TParams
  result: TResult
}

export type ServiceMethodMap = Record<string, ServiceMethod>

export function mapServiceMethods<T>(
  service: T,
  customMappings?: Record<
    string,
    | string
    | [
        string,
        {
          paramTransformer?: (params: unknown) => unknown[]
          resultTransformer?: (result: unknown) => unknown
        }?
      ]
  >
): Record<string, ServiceMethodHandler> {
  const serviceMethods = Object.getOwnPropertyNames(Object.getPrototypeOf(service)).filter(
    (name) => name !== "constructor" && typeof (service as any)[name] === "function"
  )

  const resultMappings: Record<string, ServiceMethodHandler> = {}

  if (customMappings) {
    Object.entries(customMappings).forEach(([eventName, mapping]) => {
      let methodPrefix: string
      let options:
        | {
            paramTransformer?: (params: unknown) => unknown[]
            resultTransformer?: (result: unknown) => unknown
          }
        | undefined

      if (typeof mapping === "string") {
        methodPrefix = mapping
      } else {
        ;[methodPrefix, options] = mapping
      }

      const matchingMethod = serviceMethods.find((method) => method.toLowerCase().startsWith(methodPrefix.toLowerCase()))

      if (matchingMethod) {
        resultMappings[eventName] = {
          serviceMethod: matchingMethod,
          paramTransformer: options?.paramTransformer,
          resultTransformer: options?.resultTransformer
        } as any
      }
    })
  }

  return resultMappings
}

// @ts-ignore
export const createServiceClient = <T extends ServiceMethodMap>(serviceName: string) => ({
  // @ts-ignore
  query: <M extends keyof T & string>(method: M, params: T[M]["params"]): Promise<T[M]["result"]> => {
    throw new Error("Implementation required in subclass")
  },
  // @ts-ignore
  emit: <M extends keyof T & string>(method: M, params: T[M]["params"]): Promise<void> => {
    throw new Error("Implementation required in subclass")
  }
})

export function createMethodHandlers(
  mappings: Record<string, [string, ((params: unknown) => unknown[])?, ((result: unknown) => unknown)?]>
): ServiceMethodMapping {
  return Object.entries(mappings).reduce((handlers, [methodName, config]) => {
    const [serviceMethod, paramTransformer, resultTransformer] = config

    handlers[methodName] = {
      serviceMethod,
      paramTransformer,
      resultTransformer
    } as any

    return handlers
  }, {} as ServiceMethodMapping)
}
