import { Type, Post } from "@nestjs/common"
import { createSignalRouterDecorator, SignalRouterOptions } from "../../signal-router.utils"

export interface HttpSignalRouterOptions extends SignalRouterOptions {}

export function HttpSignalRouter(serviceType: Type<any> | Type<any>[], options?: HttpSignalRouterOptions) {
  return createSignalRouterDecorator(
    serviceType,
    options,
    (data) => {
      const messageData: any = data || {}
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
