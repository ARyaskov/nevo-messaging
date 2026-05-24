# Socket.IO transport

Socket.IO is convenient when the client side is heterogeneous (browser, mobile, legacy proxies) and you want automatic reconnect, room broadcast, and transport fallback.

## Install

```bash
npm install socket.io socket.io-client
```

## Server

```ts
import { SocketSignalRouter, createSocketMicroservice } from "@riaskov/nevo-messaging"

@Controller()
@SocketSignalRouter([UserService], {
  serviceName: "user",
  port: 8093,
  cors: { origin: "*" },
  discovery: { enabled: true, heartbeatIntervalMs: 5_000 },
  accessControl: {
    rules: [{ topic: "user-events", method: "*", allow: ["frontend"] }],
    logDenied: true
  }
})
export class UserController { ... }

createSocketMicroservice({
  microserviceName: "user",
  module: AppModule,
  port: 8092
})
```

### Router options

```ts
interface SocketSignalRouterOptions extends SignalRouterOptions {
  port?: number
  path?: string
  cors?: CorsOptions
  discovery?: { enabled?: boolean; heartbeatIntervalMs?: number }
}
```

Plus the common router options (`accessControl`, `before`, `after`, `debug`).

## Client

```ts
import { createNevoSocketClient } from "@riaskov/nevo-messaging"

createNevoSocketClient(
  { coordinator: "http://127.0.0.1:8094" },
  { clientIdPrefix: "user" }
)
```

DI token: `"NEVO_SOCKET_CLIENT"` (exported as `NEVO_SOCKET_CLIENT_TOKEN`).

## Patterns at this transport

All four patterns work: `query`, `emit`, `publish`/`subscribe`, `broadcast`. The Socket.IO room abstraction is used internally for fan-out.

## Per-user routing ("sticky")

There is no static "sticky session" option on the router. Per-user routing is done dynamically at the protocol level:

- A client emits the `nevo:identify` event with `{ stickyUserId: "user-123" }`
- The server joins that socket to a room named `user:user-123`
- To target a specific user from the server, send an envelope with `meta.headers["nevo-target-user"] = "user-123"`

This is implemented inside the Socket.IO transport — your application code typically calls `publish` / `broadcast` and the routing happens transparently.

For horizontal scaling, put a Socket.IO Redis adapter underneath; the framework doesn't manage that.

## Performance

- Default codec is MessagePack (smaller and faster than JSON over Socket.IO's text/binary frames)
- Browser clients will negotiate `polling` → `websocket` upgrade automatically
- Use `@RateLimit` and ACL — Socket.IO clients are typically untrusted

## See also

- [basics-websocket.md](./basics-websocket.md) — leaner alternative when both ends are under your control
- [discovery.md](./discovery.md) — discovery over Socket.IO
- [access-control.md](./access-control.md) — ACL for untrusted callers
