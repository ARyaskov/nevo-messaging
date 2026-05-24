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

export interface TypedServiceClient<T extends ServiceMethodMap> {
  query<M extends keyof T & string>(method: M, params: T[M]["params"], opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string> }): Promise<T[M]["result"]>
  emit<M extends keyof T & string>(method: M, params: T[M]["params"], opts?: { version?: string; idempotencyKey?: string; headers?: Record<string, string> }): Promise<void>
  publish<M extends keyof T & string>(method: M, params: T[M]["params"], opts?: { version?: string; headers?: Record<string, string> }): Promise<void>
  subscribe<M extends keyof T & string>(method: M, handler: (data: T[M]["result"]) => void | Promise<void>, opts?: { ack?: boolean; durableKey?: string }): Promise<{ unsubscribe: () => Promise<void> }>
}

export interface ClientTransportLike {
  query(serviceName: string, method: string, params: any, opts?: any): Promise<any>
  emit(serviceName: string, method: string, params: any, opts?: any): Promise<void>
  publish?(serviceName: string, method: string, params: any, opts?: any): Promise<void>
  subscribe?(serviceName: string, method: string, options: any, handler: any): Promise<any>
}

export function createServiceClient<T extends ServiceMethodMap>(serviceName: string, client: ClientTransportLike): TypedServiceClient<T> {
  return {
    query<M extends keyof T & string>(method: M, params: T[M]["params"], opts?: any) {
      return client.query(serviceName, method as string, params as any, opts) as Promise<T[M]["result"]>
    },
    emit<M extends keyof T & string>(method: M, params: T[M]["params"], opts?: any) {
      return client.emit(serviceName, method as string, params as any, opts) as Promise<void>
    },
    publish<M extends keyof T & string>(method: M, params: T[M]["params"], opts?: any) {
      if (!client.publish) throw new Error("Transport does not support publish")
      return client.publish(serviceName, method as string, params as any, opts) as Promise<void>
    },
    subscribe<M extends keyof T & string>(method: M, handler: (data: T[M]["result"]) => void | Promise<void>, opts?: { ack?: boolean; durableKey?: string }) {
      if (!client.subscribe) throw new Error("Transport does not support subscribe")
      return client.subscribe(serviceName, method as string, opts, (data: any) => handler(data)) as Promise<{ unsubscribe: () => Promise<void> }>
    }
  }
}

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

      const matchingMethod = serviceMethods.find((m) => m.toLowerCase().startsWith(methodPrefix.toLowerCase()))

      if (matchingMethod) {
        resultMappings[eventName] = {
          serviceMethod: matchingMethod,
          paramTransformer: options?.paramTransformer,
          resultTransformer: options?.resultTransformer
        }
      }
    })
  }

  return resultMappings
}

export function createMethodHandlers(
  mappings: Record<string, [string, ((params: unknown) => unknown[])?, ((result: unknown) => unknown)?]>
): ServiceMethodMapping {
  return Object.entries(mappings).reduce((handlers, [methodName, config]) => {
    const [serviceMethod, paramTransformer, resultTransformer] = config

    handlers[methodName] = {
      serviceMethod,
      paramTransformer,
      resultTransformer
    }

    return handlers
  }, {} as ServiceMethodMapping)
}
