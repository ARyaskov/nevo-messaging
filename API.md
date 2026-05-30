# API Reference

## Decorators

### `@Signal(signalName, methodName?, paramTransformer?, resultTransformer?)`

Maps external signals to service methods.

**Parameters:**
- `signalName` (string): External signal identifier
- `methodName` (string, optional): Service method name (defaults to signalName)
- `paramTransformer` (function, optional): Transform incoming parameters
- `resultTransformer` (function, optional): Transform outgoing results

### `SignalRouterOptions`

Common options for all signal routers.

**Fields:**
- `before` / `after` hooks
- `debug` (boolean)
- `eventPattern` (string)
- `accessControl` (ACL rules)

## Transport Routers (priority order)

### `@NatsSignalRouter(serviceTypes, options?)`

**Options:** `SignalRouterOptions` + `servers?: string[]` + `reconnect?: { enabled?, maxAttempts?, timeWaitMs?, jitterMs?, jitterTlsMs?, waitOnFirstConnect?, lazyConnect? }`

### `@KafkaSignalRouter(serviceTypes, options?)`

**Options:** `SignalRouterOptions`

### `@SocketSignalRouter(serviceTypes, options?)`

**Options:** `SignalRouterOptions` + `port?`, `path?`, `cors?`, `serviceName?`, `discovery?`

### `@HttpSignalRouter(serviceTypes, options?)`

**Options:** `SignalRouterOptions`

## Clients and Base Classes (priority order)

### NATS

- `NevoNatsClient` (methods: `query`, `emit`, `publish`, `subscribe`, `broadcast`, `getAvailableServices`, `getDiscoveredServices`, `isServiceAvailable`)
- `NatsClientBase` - base class with the same protected methods
- `createNevoNatsClient(serviceNames, options)` - Nest provider (`NEVO_NATS_CLIENT`)
- `createNatsMicroservice(options)` - Nest bootstrap for NATS transport

`NevoNatsClientOptions.reconnect`:
- `enabled?: boolean` (default `true`)
- `maxAttempts?: number` (default `-1`)
- `timeWaitMs?: number` (default `5000`)
- `jitterMs?: number`
- `jitterTlsMs?: number`
- `waitOnFirstConnect?: boolean`
- `lazyConnect?: boolean` (if `true`, sets `waitOnFirstConnect` to `false` by default)

### Kafka

- `NevoKafkaClient` (methods: `query`, `emit`, `publish`, `subscribe`, `broadcast`, `getAvailableServices`, `getDiscoveredServices`, `isServiceAvailable`)
- `KafkaClientBase` - base class with the same protected methods
- `createNevoKafkaClient(serviceNames, options)` - Nest provider (`NEVO_KAFKA_CLIENT`)
- `createKafkaMicroservice(options)` - Nest bootstrap for Kafka transport

### Socket.IO

- `NevoSocketClient` (methods: `query`, `emit`, `publish`, `subscribe`, `broadcast`, `getAvailableServices`, `getDiscoveredServices`, `isServiceAvailable`)
- `SocketClientBase` - base class with the same protected methods
- `createNevoSocketClient(serviceUrls, options)` - Nest provider (`NEVO_SOCKET_CLIENT`)
- `createSocketMicroservice(options)` - Nest bootstrap for Socket.IO transport

### HTTP (SSE)

- `NevoHttpClient` (methods: `query`, `emit`, `publish`, `subscribe`, `broadcast`, `getAvailableServices`, `getDiscoveredServices`, `isServiceAvailable`)
- `HttpClientBase` - base class with the same protected methods
- `createNevoHttpClient(serviceUrls, options)` - Nest provider (`NEVO_HTTP_CLIENT`)
- `createHttpMicroservice(options)` - Nest bootstrap for HTTP transport
- `HttpTransportController` - adds HTTP/SSE endpoints:
  - `POST /:service-events` for query/emit
  - `POST /__nevo/publish` and `GET /__nevo/subscribe` for subscriptions
  - `POST /__broadcast` and `GET /__broadcast`
  - `POST /__nevo.discovery` and `GET /__nevo.discovery`

### HTTP/2

- `NevoHttp2Client` (method: `query` only — HTTP/2 is request/reply, no pub/sub)

### WebSocket / Socket.IO

- `NevoWsClient` / `NevoSocketClient` (methods: `query`, `emit`, `publish`, `subscribe`, `broadcast`)
- `createNevoWsClient(serviceUrls, options)` (token `NEVO_WS_CLIENT`) / `createNevoSocketClient(serviceUrls, options)` (token `NEVO_SOCKET_CLIENT`)

> For the full per-transport support grid (which transports offer `requestMany`, `subscribeWildcard`, JetStream, real `ack`), see [docs/capability-matrix.md](./docs/capability-matrix.md).

## In-memory transport (testing)

For unit/integration tests with zero IO — no brokers, no Docker.

- `createMemoryTransport(opts?)` → `MemoryTransport` (eagerly registers `opts.handlers`)
- `MemoryTransport` — `query`/`emit`/`publish`/`subscribe`/`broadcast`/`subscribeBroadcast`, `registerHandler`, `unregisterHandler`, `reset`
- `MemoryClientBase` — drop-in base class replacing `NatsClientBase`/`KafkaClientBase`/etc.
- `MemoryHarness` — fault injection: `failNext(service, method, err)`, `delayBy(service, method, ms)`, `advanceTime(ms)`, `now()`, `reset()`, and the recorded `calls[]`

See [docs/testing.md](./docs/testing.md).

## Resilience decorators

Declarative method decorators read by the runtime (`resilience-runtime.ts`). See [docs/resilience-decorators.md](./docs/resilience-decorators.md).

- `@Hedge(options?)` — parallel attempts for long-tail reads. Wraps `hedge()`.
- `@CircuitBreaker(options?)` — `{ mode: "sliding" | "count", windowMs?, errorRateThreshold?, minSampleSize?, keyBy? }`. Defaults to the sliding-window breaker.
- `@Adaptive(options?)` — `{ targetP99Ms?, keyBy? }`. Auto-tunes timeout/retries from observed latency.
- `@Backpressure(options?)` — `{ maxInflight, highWatermark?, lowWatermark?, onOverflow?, keyBy? }`. Pauses/resumes the subscription.

Lower-level helpers:

- `readMethodResilience(target, propertyKey)` → compiled config (or `undefined`)
- `wrapMethodWithResilience(target, propertyKey, fn, keyFn)` → wrapped callable
- `makeResilienceRunner(target, propertyKey)` → reusable runner
- `applyResilience(...)`, `snapshotResilience()` → `{ adaptive, sliding, backpressure }` keyed by `service:method`

The functional API is also exported directly: `hedge`, `CircuitBreakerRegistry`, `SlidingCircuitBreakerRegistry`, `AdaptiveTuner`, `BackpressureLimiter`.

## Stores

All Postgres stores take `{ client: PgClient, schema?, table?, logger? }` and expose `migrate()`. See [docs/storage-matrix.md](./docs/storage-matrix.md).

| Export | Interface | Backend |
|---|---|---|
| `PgOutboxStore` | `OutboxStore` | Postgres |
| `PgInboxStore` | `InboxStore` | Postgres |
| `PgSagaStore` | `SagaStore` | Postgres |
| `PgEventStore` | `EventStore` | Postgres |
| `PgDlqStore` | `DlqStore` | Postgres |
| `PgScheduledTaskStore` | `ScheduledTaskStore` | Postgres |
| `SqliteOutboxStore` | `OutboxStore` | `node:sqlite` (single pod) |
| `RedisIdempotencyStore` | `IdempotencyStore` | Redis (+ L1 LRU) |
| `RedisInboxStore` | `InboxStore` | Redis |
| `RedisRateLimiter` | rate limiter | Redis (Lua token bucket) |
| `LruIdempotencyCache` | `IdempotencyStore` | in-memory |
| `InMemoryEventStore` / `InMemoryScheduledTaskStore` | `EventStore` / `ScheduledTaskStore` | in-memory |

- `migrateAllPgStores(client, schema?)` — run every Pg store's `migrate()` (does **not** create the audit table).
- Client shapes: `PgClient`, `IdempotencyRedisLike`, `InboxRedisClient`, `RateLimitRedisClient` (4-line wrappers over `pg`/`postgres`/`pg-promise` and `ioredis`/`node-redis`).

## Workflow engine

See [docs/workflow.md](./docs/workflow.md).

- `WorkflowEngine` — `{ store?, scheduler?, logger? }`. Methods: `register`, `start`, `resume`, `signal`, `cancel`, `getState`.
- `@Workflow(options?)` — `{ name? }`. `discoverAndRegisterWorkflows(engine, instances)`, `getWorkflowMethods(target)`.
- `WorkflowContext<C>` — `workflowId`, `input`, `step(name, fn)`, `sleep(ms)`, `waitForSignal(name, { timeoutMs? })`, `now()`.
- `isWorkflowSuspended(err)`, `WORKFLOW_SUSPEND`, `WorkflowSignalTimeout` (thrown by `waitForSignal` on `timeoutMs`), `WorkflowState`, `WorkflowStatus`.

## Scheduler

See [docs/scheduler.md](./docs/scheduler.md).

- `Scheduler` — `{ store?, pollIntervalMs?, batchSize?, claimTtlMs?, maxAttempts?, workerId?, logger? }`. Methods: `registerHandler`, `enqueueAt`, `enqueueIn`, `enqueueCron`, `cancel`, `list`, `start`, `stop`, `flushOnce`.
- `@Scheduled(options?)` — `{ name?, cron?, at?, in?, maxAttempts?, timezone?, utc? }`. `discoverAndRegisterScheduled(scheduler, instances)`.
- Cron helpers: `nextCronTick(expr, from, opts?)`, `isValidCron(expr)`, `parseCron(expr)`, `CronOptions`.

## Audit log

See [docs/audit-log.md](./docs/audit-log.md).

- `AuditLog` — `{ enabled?, redactPaths?, maxEntryBytes?, sink?, logger? }`. Methods: `record`, `recordFromResponse`, `flush`, `close`, `isEnabled`.
- Sinks: `InMemoryAuditSink`, `FileAuditSink`, `PgAuditSink`, `TeeAuditSink` (all implement `AuditSink`).
- `AuditEntry`, `AuditOutcome`.

## Tenant policy

See [docs/tenant-policy.md](./docs/tenant-policy.md).

- `TenantPolicyRegistry` — `set`, `get`, `setEnabled`, `isAllowed`, `list`, `remove`, `clear`.
- `getTenantPolicyRegistry()`, `setTenantPolicyRegistry(r)`.
- `assertTenantAllowed(serviceName, tenantId)` — throws `UNAUTHORIZED` for a disabled tenant.
- `buildResilienceKey(ctx, keyBy?)`, `TenantKeyDimension`, `ResilienceKeyContext`.

## Chain context

See [docs/chain-context.md](./docs/chain-context.md).

- `runInChain(ctx, fn)`, `getCurrentChainContext()`, `getCurrentChainId()`, `newChainId()`.
- `resolveInboundChainId(metaChainId)`, `resolveOutboundChainId(explicit?)`, `getChainStorage()`.

## Health

See [docs/health-checks.md](./docs/health-checks.md).

- `HealthRegistry` — `{ serviceName, instanceId?, version?, timeoutMs?, cacheMs? }`. `register(name, fn, opts?)`, `liveness()`, `readiness()`, `report(kind)`.
- Built-in checks: `pgPing`, `redisPing`, `kafkaAdminPing`, `natsPing`, `httpPing`, `memoryUsagePing(thresholdMb)`, `eventLoopLagPing(thresholdMs)`.

## Metrics

See [docs/metrics.md](./docs/metrics.md).

- `InMemoryMetrics` implements `MetricsRegistry`; `getDefaultMetrics()`, `setDefaultMetrics(m)`.
- `NEVO_METRIC_NAMES` (includes `storeErrors`), `methodLabel(method, isKnown?)`, `UNKNOWN_METHOD_LABEL`.

## Codec

See [docs/codecs.md](./docs/codecs.md).

- Codecs: `JsonCodec`, `JsonCodecFast`, `MessagePackCodec` (all implement `Codec`).
- `getDefaultCodec()` (MessagePack if available, else JSON), `setDefaultCodec(codec)`, `getCodec(name)`, `registerCodec(codec)`.
- `CodecName` = `"msgpack" | "json" | "json-fast" | string`.

## Error codes

See [docs/error-codes.md](./docs/error-codes.md).

- `ErrorCode` enum (0–19), `isRetryable(code)`, `MessagingError`.
