# WebSocket transport

WebSocket transport is for low-latency, full-duplex apps — typically browser dashboards or mobile clients that need long-lived bidirectional streams.

## Server

The WebSocket transport ships the `WsSignalRouter` function. There is no `@WsSignalRouter` class decorator with a built-in microservice bootstrap — you stand up the server using NestJS's built-in WebSockets gateway or by wiring `WsSignalRouter` to a class manually.

```ts
import { WsSignalRouter } from "@riaskov/nevo-messaging"

@Controller()
export class UserController { ... }

// Apply as a function, not a decorator
WsSignalRouter([UserService], {
  port: 8095,
  path: "/ws",
  perMessageDeflate: {
    threshold: 1024,
    serverMaxWindowBits: 12,
    clientMaxWindowBits: 12
  },
  maxPayload: 1024 * 1024,
  accessControl: {
    rules: [{ topic: "user-events", method: "*", allow: ["frontend"] }],
    logDenied: true
  }
})(UserController)
```

### Options

```ts
interface WsSignalRouterOptions extends SignalRouterOptions {
  port?: number
  host?: string
  path?: string
  codec?: Codec
  perMessageDeflate?: boolean | {
    threshold?: number
    serverMaxWindowBits?: number
    clientMaxWindowBits?: number
    zlibDeflateOptions?: { level?: number; memLevel?: number }
  }
  maxPayload?: number
}
```

### Per-message deflate

| Option | Default | Notes |
|---|---|---|
| `threshold` | `1024` | Don't compress payloads smaller than this |
| `serverMaxWindowBits` | `15` | Lower → less memory per connection |
| `clientMaxWindowBits` | `15` | Lower → less memory per connection |
| `zlibDeflateOptions.level` | `6` | Zlib level 1 (fast) … 9 (small) |

`maxPayload` caps inbound frames; oversize frames close the connection with code 1009.

## Client

```ts
import { NevoWsClient } from "@riaskov/nevo-messaging"

const client = new NevoWsClient(
  { user: "ws://127.0.0.1:8095" },
  {
    timeoutMs: 5_000,
    reconnectIntervalMs: 1_000,
    maxReconnectAttempts: -1,
    headers: { authorization: `Bearer ${token}` },
    protocols: ["nevo.v1"]
  }
)

const u = await client.query<User>("user", "user.getById", { id: 1n })
```

There is no DI factory or token for the WS client — instantiate it directly. (Construct it inside a NestJS provider if you want injection.)

## Backpressure

`ws` buffers data in memory when a peer is slow. The framework watches `socket.bufferedAmount` and surfaces it as a metric; combine with [backpressure.md](./backpressure.md) for pause/resume on inbound subscriptions.

## Notes vs. Socket.IO

- **Native WebSocket**: smaller payload overhead, no Socket.IO handshake, no fallback to long-polling. Use when you control both endpoints.
- **Socket.IO**: richer ecosystem, room broadcast, automatic reconnect, transport fallback. Use when you have heterogeneous clients. See [basics-socket.md](./basics-socket.md).
