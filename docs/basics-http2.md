# HTTP/2 client

The HTTP/2 client multiplexes many concurrent requests over a single TCP connection per origin. It is useful when you talk to a small number of upstream hosts at high request rates.

> The framework ships **only the client** for HTTP/2 today. There is no `Http2SignalRouter`, no DI factory, no `createNevoHttp2Client` helper. Construct `NevoHttp2Client` directly. For HTTP/1.1 server-side (`@HttpSignalRouter`), see [basics-http.md](./basics-http.md).

## When to use

- High request fan-out to the same origin
- Latency-sensitive RPC where TCP/TLS handshake overhead dominates
- You already terminate HTTP/2 at a gateway (Envoy, Caddy, nginx with `http2`)

## API

```ts
interface NevoHttp2ClientOptions extends TransportClientOptions {
  timeoutMs?: number
}

class NevoHttp2Client {
  constructor(serviceUrls: Record<string, string>, options?: NevoHttp2ClientOptions)
  query<T>(serviceName: string, method: string, params: unknown, opts?: { timeoutMs?: number }): Promise<T>
  getAvailableServices(): string[]
  getInstanceId(): string
  close(): Promise<void>
}
```

## Usage

```ts
import { NevoHttp2Client } from "@riaskov/nevo-messaging"

const client = new NevoHttp2Client(
  { user: "https://user.internal:8443" },
  { timeoutMs: 5_000 }
)

const u = await client.query<User>("user", "user.getById", { id: 1n })

await client.close()
```

The client keeps one `Http2Session` per origin and opens a fresh stream per call. Sessions are reused; `close()` tears them down on shutdown.

## ALPN

HTTP/2 requires TLS in practice. The client uses ALPN to negotiate `h2`. For cleartext `h2c` in-cluster, pass an `http://` origin — the framework will not enforce ALPN in that case.

## Concurrent streams

The peer advertises `SETTINGS_MAX_CONCURRENT_STREAMS`. The client respects that limit and queues additional requests in process. There is no automatic pool — for more parallelism, run multiple `NevoHttp2Client` instances.

## Compared to HTTP/1.1

| | HTTP/1.1 (`NevoHttpClient`) | HTTP/2 (`NevoHttp2Client`) |
|---|---|---|
| Connections per origin | Pool (agent) | 1 session |
| Head-of-line blocking | Yes (TCP-level) | No (HTTP-layer) |
| TLS handshake | Per pooled socket | Once |
| Best for | Mixed origins, idle traffic | Few origins, high RPS |

For request-reply only — there is no subscribe / publish on this transport at the moment.

## Limitations

- **Query only.** `emit`, `publish`, `subscribe`, `broadcast` are not implemented for HTTP/2.
- **No DI integration.** No NestJS factory. Instantiate the class in a module provider yourself.
- **No middleware** (no `accessControl` / `JWT` on the client). For protected upstreams, set headers via the underlying transport options.

For a fuller server-side HTTP setup, use [`@HttpSignalRouter`](./basics-http.md) which exposes all four patterns over HTTP/1.1 + SSE.
