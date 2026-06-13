import { Type, Post, Body } from "@nestjs/common"
import { createSignalRouterDecorator, SignalRouterOptions } from "../../signal-router.utils"
import { getCodec } from "../../common"

export type HttpSignalRouterOptions = SignalRouterOptions

const decodedBodies = new WeakMap<object, any>()

function decodeBinaryBody(data: Uint8Array): any {
  const cached = decodedBodies.get(data)
  if (cached !== undefined) return cached
  let decoded: any
  try {
    decoded = getCodec("msgpack").decode(data)
  } catch {
    decoded = getCodec("json").decode(data)
  }
  decodedBodies.set(data, decoded)
  return decoded
}

export function HttpSignalRouter(serviceType: Type<any> | Type<any>[], options?: HttpSignalRouterOptions) {
  return createSignalRouterDecorator(
    serviceType,
    options,
    (data) => {
      const messageData: any = (data instanceof Uint8Array ? decodeBinaryBody(data) : data) || {}
      return {
        method: messageData.method,
        params: messageData.params,
        uuid: messageData.uuid,
        meta: messageData.meta
      }
    },
    (target, eventPattern, handlerName) => {
      Post(`/${eventPattern}`)(target.prototype, handlerName, Object.getOwnPropertyDescriptor(target.prototype, handlerName)!)
      Body()(target.prototype, handlerName, 0)
    }
  )
}
