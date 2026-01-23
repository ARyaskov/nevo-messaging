import { ClientProxy } from "@nestjs/microservices"

export interface MessagingClientProxy extends ClientProxy {
  subscribeToResponseOf(pattern: string): void
}

export interface MessagePayload {
  key: string
  value: string
}

export type MessageType = "query" | "emit" | "sub" | "broadcast" | "discovery"

export interface MessageMeta {
  type?: MessageType
  service?: string
  ts?: number
  auth?: {
    token?: string
  }
}

export interface MessageRequest<T = unknown> {
  uuid: string
  method: string
  params: T
  meta?: MessageMeta
}

export interface MessageResponse<T = unknown> {
  uuid: string
  method: string
  params: {
    result: T | "error"
    error?: ErrorDetails
  }
  meta?: MessageMeta
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
  meta?: MessageMeta
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
  serviceName?: string
  authToken?: string
  timeout?: number
  debug?: boolean
  backoff?: {
    enabled?: boolean
    baseMs?: number
    maxMs?: number
    maxAttempts?: number
    jitter?: boolean
  }
  discovery?: {
    enabled?: boolean
    heartbeatIntervalMs?: number
    ttlMs?: number
  }
  [key: string]: any
}

export interface TransportServerOptions {
  serviceName: string
  debug?: boolean
  authToken?: string
  [key: string]: any
}

export interface MicroserviceConfig {
  serviceName: string
  clientName: string
}

export interface SubscriptionOptions {
  ack?: boolean
  durableKey?: string
  groupId?: string
  fromBeginning?: boolean
}

export interface SubscriptionContext {
  ack(): Promise<void>
  nack?(reason?: string): Promise<void>
  meta: MessageMeta
}

export interface Subscription {
  unsubscribe(): Promise<void>
}

export interface AccessRule {
  topic?: string
  method?: string
  allow?: string[]
  deny?: string[]
}

export interface AccessControlConfig {
  rules?: AccessRule[]
  allowAllByDefault?: boolean
  logDenied?: boolean
}

export interface DiscoveryAnnouncement {
  serviceName: string
  clientId?: string
  transport: string
  ts: number
  meta?: Record<string, unknown>
}

export interface DiscoveryEntry extends DiscoveryAnnouncement {
  lastSeen: number
}
