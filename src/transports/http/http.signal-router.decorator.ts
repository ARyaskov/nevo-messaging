import { Type } from "@nestjs/common"
import { Post } from "@nestjs/common"
import { createSignalRouterDecorator, SignalRouterOptions } from "../../signal-router.utils"

export function HttpSignalRouter(serviceType: Type<any> | Type<any>[], options?: SignalRouterOptions) {
  return createSignalRouterDecorator(
    serviceType,
    options,
    (data) => {
      const messageData = data || {}
      return {
        method: messageData.method,
        params: messageData.params,
        uuid: messageData.uuid,
        meta: messageData.meta
      }
    },
    (target, eventPattern, handlerName) => {
      Post(`/${eventPattern}`)(target.prototype, handlerName, Object.getOwnPropertyDescriptor(target.prototype, handlerName)!)
    }
  )
}
