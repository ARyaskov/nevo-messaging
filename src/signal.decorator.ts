import "reflect-metadata"

// FIXME
export const SIGNALS_METADATA_KEY = "kafka:signals"

export interface SignalOptions {
  [key: string]: any
}

export interface SignalMetadata {
  signalName: string
  methodName: string
  paramTransformer?: (data: any) => any[]
  resultTransformer?: (result: any) => any
  options?: SignalOptions
}

export function addSignalMetadata(
  target: any,
  signalName: string,
  methodName: string,
  paramTransformer?: (data: any) => any[],
  resultTransformer?: (result: any) => any,
  options?: SignalOptions
) {
  const existingSignals: SignalMetadata[] = Reflect.getMetadata(SIGNALS_METADATA_KEY, target) || []

  existingSignals.push({
    signalName,
    methodName,
    paramTransformer,
    resultTransformer,
    options
  } as any)

  Reflect.defineMetadata(SIGNALS_METADATA_KEY, existingSignals, target)
}

export function Signal(
  signalName: string,
  methodNameOrParamTransformer?: string | ((data: any) => any[]),
  paramTransformerOrOptions?: ((data: any) => any[]) | ((result: any) => any) | SignalOptions,
  resultTransformerOrOptions?: ((result: any) => any) | SignalOptions,
  options?: SignalOptions
): MethodDecorator {
  // @ts-ignore
  return function (target: any, propertyKey: string | symbol, descriptor: PropertyDescriptor) {
    let methodName: string
    let paramTransformer: ((data: any) => any[]) | undefined
    let resultTransformer: ((result: any) => any) | undefined
    let signalOptions: SignalOptions | undefined

    // Case 1: @Signal("create", (data) => [data.id, data.param2])
    if (typeof methodNameOrParamTransformer === "function") {
      methodName = signalName
      paramTransformer = methodNameOrParamTransformer

      if (typeof paramTransformerOrOptions === "function") {
        resultTransformer = paramTransformerOrOptions as (result: any) => any
        signalOptions = resultTransformerOrOptions as SignalOptions
      } else {
        signalOptions = paramTransformerOrOptions as SignalOptions
      }
    }
    // Case 2, 3, 4: @Signal("modify", "modifyInvoice", ...)
    else if (typeof methodNameOrParamTransformer === "string") {
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
    }
    // Default case: @Signal("create")
    else {
      methodName = signalName
    }

    addSignalMetadata(target.constructor, signalName, methodName, paramTransformer, resultTransformer, signalOptions)

    return descriptor
  }
}

export function getClassSignals(target: any): SignalMetadata[] {
  return Reflect.getMetadata(SIGNALS_METADATA_KEY, target) || []
}
