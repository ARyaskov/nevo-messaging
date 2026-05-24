import { ClientProxy } from "@nestjs/microservices"

export interface MessagingClientProxy extends ClientProxy {
  subscribeToResponseOf(pattern: string): void
}

export interface MessagePayload {
  key: string
  value: string | Uint8Array
}

export type MessageType = "query" | "emit" | "sub" | "broadcast" | "discovery" | "health"

export interface TraceContext {
  traceparent?: string
  tracestate?: string
  baggage?: string
}

export interface MessageMeta {
  type?: MessageType
  service?: string
  instanceId?: string
  ts?: number
  version?: string
  auth?: {
    token?: string
  }
  trace?: TraceContext
  attempt?: number
  idempotencyKey?: string
  contentEncoding?: "gzip" | "deflate" | "zstd" | "identity"
  codec?: string
  tenantId?: string
  headers?: Record<string, string>
  /**
   * Nevo-internal correlation id propagated across a fan-out of calls so
   * DevTools can group all envelopes that belong to one originating request.
   * See `src/common/chain-context.ts`.
   */
  nevoChainId?: string
  /**
   * Envelope uuid of the inbound message that caused this outbound one.
   * Optional — only set when an outbound call is issued from inside a handler.
   * Used by DevTools to reconstruct a tree instead of a flat ordered list.
   */
  nevoParentUuid?: string
  [key: string]: unknown
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
  schema?: unknown
  version?: string
}

export interface ServiceMethodMapping {
  [methodName: string]: ServiceMethodHandler
}

export interface BackoffOptions {
  enabled?: boolean
  baseMs?: number
  maxMs?: number
  maxAttempts?: number
  jitter?: boolean
}

export interface RetryOptions {
  enabled?: boolean
  maxAttempts?: number
  baseMs?: number
  maxMs?: number
  jitter?: boolean
  retryOnCodes?: number[]
}

export interface CircuitBreakerOptions {
  enabled?: boolean
  failureThreshold?: number
  resetTimeoutMs?: number
  halfOpenSuccessThreshold?: number
}

export interface IdempotencyOptions {
  enabled?: boolean
  maxEntries?: number
  ttlMs?: number
}

export interface CompressionOptions {
  enabled?: boolean
  algorithm?: "gzip" | "deflate" | "zstd"
  threshold?: number
  level?: number
  async?: boolean
}

export interface SecurityOptions {
  maxPayloadBytes?: number
  replayWindowMs?: number
  redactPaths?: string[]
  tls?: TlsOptions
}

export interface TlsOptions {
  enabled?: boolean
  ca?: string | Buffer | (string | Buffer)[]
  cert?: string | Buffer
  key?: string | Buffer
  passphrase?: string
  rejectUnauthorized?: boolean
  servername?: string
}

export interface NatsAuthOptions {
  user?: string
  pass?: string
  token?: string
  nkey?: string
  jwt?: string
  seed?: string
  credsFile?: string
}

export interface KafkaSaslOptions {
  mechanism: "plain" | "scram-sha-256" | "scram-sha-512" | "aws" | "oauthbearer"
  username?: string
  password?: string
  authorizationIdentity?: string
  accessKeyId?: string
  secretAccessKey?: string
  sessionToken?: string
  oauthBearerProvider?: () => Promise<{ value: string }>
}

export interface MetricsOptions {
  enabled?: boolean
}

export interface TracingOptions {
  enabled?: boolean
  serviceName?: string
}

export interface TransportClientOptions {
  clientId?: string
  serviceName?: string
  instanceId?: string
  authToken?: string
  timeout?: number
  debug?: boolean
  codec?: import("./codec").Codec | string
  logger?: import("./logger").NevoLogger
  backoff?: BackoffOptions
  retry?: RetryOptions
  circuitBreaker?: CircuitBreakerOptions
  idempotency?: IdempotencyOptions
  compression?: CompressionOptions
  security?: SecurityOptions
  metrics?: MetricsOptions
  tracing?: TracingOptions
  rateLimit?: import("./rate-limit").RateLimiterOptions | import("./rate-limit").RateLimiter
  devtools?: import("./devtools").DevToolsBus | boolean
  discovery?: {
    enabled?: boolean
    heartbeatIntervalMs?: number
    ttlMs?: number
    pruneIntervalMs?: number
    capabilities?: string[]
    host?: string
    port?: number
    version?: string
  }
  [key: string]: any
}

export interface TransportServerOptions {
  serviceName: string
  debug?: boolean
  authToken?: string
  codec?: import("./codec").Codec | string
  logger?: import("./logger").NevoLogger
  idempotency?: IdempotencyOptions
  security?: SecurityOptions
  metrics?: MetricsOptions
  tracing?: TracingOptions
  dlq?: { enabled?: boolean; topic?: string }
  [key: string]: any
}

export interface MicroserviceConfig {
  serviceName: string
  clientName: string
}

export interface SubscriptionFilter {
  headers?: Record<string, string | RegExp>
  meta?: Record<string, string | RegExp>
}

export interface SubscriptionOptions {
  ack?: boolean
  durableKey?: string
  groupId?: string
  fromBeginning?: boolean
  room?: string
  filter?: SubscriptionFilter
  maxDeliveryAttempts?: number
  dlq?: boolean
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
  jwtVerifier?: (token: string) => Promise<{ service?: string; [k: string]: unknown } | null> | { service?: string; [k: string]: unknown } | null
}

export interface DiscoveryAnnouncement {
  serviceName: string
  instanceId: string
  clientId?: string
  transport: string
  ts: number
  host?: string
  port?: number
  version?: string
  capabilities?: string[]
  meta?: Record<string, unknown>
}

export interface DiscoveryEntry extends DiscoveryAnnouncement {
  lastSeen: number
}
