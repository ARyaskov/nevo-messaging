# Signal routing & method decorators

Nevo exposes a unified declarative routing layer on top of NestJS, plus two method-level decorators for rate limiting and result caching.

## `@<Transport>SignalRouter`

Apply to a controller class to register it with the framework. The decorator takes the list of services whose methods can be invoked:

```ts
@Controller()
@NatsSignalRouter([UserService, ProfileService])
export class UserController { ... }
```

Variants by transport:

- `@NatsSignalRouter`
- `@KafkaSignalRouter`
- `@HttpSignalRouter`
- `@WsSignalRouter` (and the `WsSignalRouter` function for non-controller usage)
- `@SocketSignalRouter`

### Common router options

```ts
@NatsSignalRouter([UserService], {
  before: async (ctx) => ctx.params,
  after:  async (ctx) => ctx.response,
  debug: true,
  accessControl: {
    rules: [{ topic: "user-events", method: "*", allow: ["frontend"] }],
    logDenied: true,
    allowAllByDefault: false,
    jwtVerifier: createJwksVerifier({ jwksUri: "..." })
  }
})
```

`before` may mutate `ctx.params`; `after` may mutate `ctx.response`. Throw from either to reject the call. See [access-control.md](./access-control.md), [security.md](./security.md).

Transport-specific options (e.g. `port`, `cors`, `perMessageDeflate`, `reuseClient`) live on the corresponding router — see the per-transport docs.

## `@Signal`

Map a wire signal to a service method:

```ts
@Signal("user.getById", "getById", (d) => [d.id])
getById() {}
```

Arguments:

1. `signal` — wire name (`"user.getById"` or versioned `"user.getById@v2"`)
2. `methodName` — name of the method on the injected service
3. `paramTransformer?` — `(data) => any[]` mapping the envelope params to method args
4. `resultTransformer?` — `(result) => any` to reshape the reply

If the transformer is omitted, `data` is passed verbatim as the single argument.

### Result transformer

```ts
@Signal(
  "user.getProfile", "getById",
  (d) => [d.id],
  (u) => ({ ...u, password: undefined })
)
getProfile() {}
```

## Method-level decorators

The framework ships exactly two:

### `@RateLimit`

```ts
import { RateLimit } from "@riaskov/nevo-messaging"

@RateLimit({ capacity: 100, refillPerSec: 50, keyBy: ["tenantId"] })
async create(input: CreateDto) { ... }
```

Token bucket — `capacity` is the burst, `refillPerSec` is the sustained rate. `keyBy` partitions buckets by `service`/`method`/`callerService`/`tenantId`. See [rate-limiting.md](./rate-limiting.md).

### `@Cacheable`

In-process LRU cache for handler results.

```ts
import { Cacheable } from "@riaskov/nevo-messaging"

@Cacheable({ ttlMs: 60_000, maxEntries: 1024, keyBy: (params) => `u:${params.id}` })
async getById(id: bigint) { ... }
```

`keyBy` receives the **envelope params** (not the method args) and returns a cache key string.

## What the framework does NOT ship

The following decorators were never implemented; please ignore any earlier documentation that referenced them:

- `@Hedge` — call `hedge()` directly; see [hedging.md](./hedging.md)
- `@Retry` — retry is a client/option-level concern; see [retry.md](./retry.md)
- `@CircuitBreaker` — the breaker is a registry, not a decorator; see [circuit-breaker.md](./circuit-breaker.md)
- `@Adaptive` — `AdaptiveTuner` is a class; see [adaptive.md](./adaptive.md)

If you want method-level retry/hedging behavior, wrap the handler call site or build a small custom decorator on top of `withRetry()` / `hedge()`.

## Hook context

`before` / `after` hooks share a context shape across transports:

```ts
interface SignalContext {
  uuid: string
  serviceName: string
  method: string
  params: unknown
  response: { params: { result: unknown } } | { params: { error: { code, message } } }
  rawData: { headers?: Record<string, string>; ... }
}
```

Mutate `ctx.params` in `before` to rewrite arguments. Mutate `ctx.response` in `after` to rewrite the reply. Throwing rejects the call.
