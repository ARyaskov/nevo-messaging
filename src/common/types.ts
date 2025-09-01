import { ClientProxy } from "@nestjs/microservices"

export interface MessagingClientProxy extends ClientProxy {
  subscribeToResponseOf(pattern: string): void
}

export interface MessagePayload {
  key: string
  value: string
}

export interface MessageRequest<T = unknown> {
  uuid: string
  method: string
  params: T
}

export interface MessageResponse<T = unknown> {
  uuid: string
  method: string
  params: {
    result: T | "error"
    error?: ErrorDetails
  }
}

export interface ErrorDetails {
  code: number
  message: string
  details?: Record<string, unknown>
  service?: string
  stack?: string
}

export interface HookContext {
  method: string
  serviceName: string
  uuid: string
  rawData: unknown
}

export interface BeforeHookContext extends HookContext {
  params: unknown
  options?: Record<string, unknown>
}

export interface AfterHookContext extends HookContext {
  params: unknown
  result: unknown
  response: MessageResponse
  options?: Record<string, unknown>
}

export type BeforeHook = (context: BeforeHookContext) => Promise<unknown> | unknown
export type AfterHook = (context: AfterHookContext) => Promise<MessageResponse> | MessageResponse

export type SystemBeforeHook = (context: BeforeHookContext) => Promise<void> | void
export type SystemAfterHook = (context: AfterHookContext) => Promise<void> | void

export interface ServiceMethodHandler {
  serviceMethod: string
  paramTransformer?: (params: unknown) => unknown[]
  resultTransformer?: (result: unknown) => unknown
  options?: Record<string, unknown>
}

export interface ServiceMethodMapping {
  [methodName: string]: ServiceMethodHandler
}

export interface TransportClientOptions {
  clientId?: string
  timeout?: number
  debug?: boolean
  [key: string]: any
}

export interface TransportServerOptions {
  serviceName: string
  debug?: boolean
  [key: string]: any
}

export interface MicroserviceConfig {
  serviceName: string
  clientName: string
}
