import { Type } from "@nestjs/common"
import { ErrorCode } from "./common"
import type { MessageMeta } from "./common/types"
import { matchesFilter } from "./common/subscription-filters"
import { parseMethod, DEFAULT_METHOD_VERSION } from "./common/version"
import {
  bindNevoRouter,
  deriveServiceName,
  findPropertyByType,
  findServiceInstances,
  getRouterClassMetadata,
  getRouterRuntime,
  setRouterClassMetadata,
  tryGetRouterRuntime,
  type MessageData,
  type MessageExtractor,
  type NevoRuntimeOptions,
  type SignalRouterMetadata
} from "./router-runtime"

/** Request-processing config lives in `NevoModule`; see docs/migration-nevo-module.md. */
export type SignalRouterOptions = SignalRouterMetadata

export { findPropertyByType, findServiceInstances, matchesFilter, parseMethod, DEFAULT_METHOD_VERSION }
export type { MessageData, MessageExtractor }

export function createErrorResponse(message: string, uuid?: string, method?: string, code: number = ErrorCode.UNKNOWN, meta?: MessageMeta) {
  return {
    uuid,
    method,
    params: {
      result: "error",
      error: { message, code }
    },
    meta
  }
}

const HANDLER_NAME = "handleSignalMessage"

/** Metadata only; the handler resolves its `RouterRuntime` on first use. */
export function createSignalRouterDecorator(
  serviceType: Type<any> | Type<any>[],
  options: SignalRouterOptions = {},
  messageExtractor: MessageExtractor,
  registerHandler: (target: any, eventPattern: string, handlerName: string, context?: any) => void
) {
  return function (target: any): any {
    const serviceName = deriveServiceName(target, options)
    const eventPattern = options.eventPattern || `${serviceName}-events`

    setRouterClassMetadata(target, {
      serviceType,
      eventPattern,
      serviceName,
      routerMeta: options,
      extract: messageExtractor
    })

    target.prototype[HANDLER_NAME] = async function (data: any) {
      return getRouterRuntime(this.constructor).handle(this, data)
    }

    registerHandler(target, eventPattern, HANDLER_NAME)
    return target
  }
}

export function bindSignalRouterForTesting(target: any, options: NevoRuntimeOptions = {}) {
  return bindNevoRouter(target, options)
}

export { bindNevoRouter, getRouterClassMetadata, getRouterRuntime, tryGetRouterRuntime }
export type { NevoRuntimeOptions, SignalRouterMetadata }
