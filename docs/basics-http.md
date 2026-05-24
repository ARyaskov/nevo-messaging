# HTTP / SSE transport

HTTP transport is ideal for services that must be reachable from external clients (browsers, load balancers, integration partners) without a broker.

- `query` / `emit` → plain `POST` requests
- `subscribe` → Server-Sent Events (SSE)
- `broadcast` → SSE fan-out from the server

## Server side

```ts
import { HttpSignalRouter, HttpTransportController, createHttpMicroservice } from "@riaskov/nevo-messaging"

@Controller()
@HttpSignalRouter([UserService], {
  accessControl: {
    rules: [{ topic: "user-events", method: "*", allow: ["frontend"] }],
    logDenied: true
  }
})
export class UserController { ... }

@Module({
  controllers: [UserController, HttpTransportController],
  providers: [UserService]
})
export class AppModule {}

createHttpMicroservice({
  microserviceName: "user",
  module: AppModule,
  port: 8090
})
```

`HttpTransportController` is required when you use `subscribe` / `publish` / `broadcast`; it exposes the SSE endpoint and publish hook. For pure `query` / `emit` you can omit it.

## Client side

```ts
import { createNevoHttpClient } from "@riaskov/nevo-messaging"

const provider = createNevoHttpClient(
  { coordinator: "http://127.0.0.1:8091" },
  {
    clientIdPrefix: "frontend",
    timeoutMs: 10_000,

    // HTTP/1.1 agent tuning
    keepAlive: true,
    maxSockets: 64,
    maxFreeSockets: 16,
    tcpNoDelay: true,
    socketKeepAliveMs: 30_000,
    recvBufferSize: 256 * 1024,

    // DNS cache (peer-optional: install `cacheable-lookup`)
    cacheableDns: { ttl: 60_000, maxTtl: 600_000 },

    // Optional: swap http.Agent for undici.Agent (peer-optional)
    useUndici: false
  }
)
```

DI token: `"NEVO_HTTP_CLIENT"` (exported as `NEVO_HTTP_CLIENT_TOKEN`).

The `serviceUrls` map is `{ <serviceName>: <baseUrl> }`. The framework appends `/<serviceName>-events` to the URL for each call.

## Performance options

| Option | Effect |
|---|---|
| `keepAlive` | TCP keep-alive on sockets in the agent pool |
| `maxSockets` / `maxFreeSockets` | Agent pool sizing |
| `tcpNoDelay` | Disable Nagle — lower latency for small requests |
| `socketKeepAliveMs` | Idle interval for TCP keepalive |
| `recvBufferSize` | OS receive buffer hint |
| `cacheableDns` | Memoise DNS lookups (peer-optional dep `cacheable-lookup`) |
| `useUndici` | Use undici's pooled agent (peer-optional dep `undici`) |
| `useMessagePack` | Force MessagePack codec on outbound (otherwise inferred) |

The peer-optional deps are loaded with try/catch and silently fall back if not installed.

## Authentication

Pass `meta.auth.token` on outbound calls, or read `Authorization: Bearer <token>` from request headers in a `before` hook. To verify JWTs, configure `accessControl.jwtVerifier` — see [security.md](./security.md).

## Idempotency-Key header

If you use the [idempotency cache](./idempotency.md) and pass `idempotencyKey` per call, the HTTP transport surfaces it on the wire so server-side dedupe works the same as on any other transport.

## Discovery

HTTP discovery is opt-in. The typical setup is to run a coordinator service exposing `HttpTransportController` + a heartbeat endpoint, and point clients at it via the `coordinator` URL:

```ts
createNevoHttpClient({ coordinator: "http://discovery.internal:8091" }, { clientIdPrefix: "frontend" })
```

For service-mesh or DNS-SD setups, skip the coordinator and configure `serviceUrls` directly.

## SSE keepalive

The framework sends `: keepalive` comments at a sensible cadence so browsers and reverse proxies treat the connection as live.

## Production tips

- Terminate TLS at a real reverse proxy (nginx, Caddy, Envoy) in front of the service
- Tune `maxSockets` to match your downstream capacity
- For very high RPS to a single origin, consider [HTTP/2](./basics-http2.md) instead
- Combine with [graceful shutdown](./graceful-shutdown.md) so in-flight SSE streams drain cleanly
