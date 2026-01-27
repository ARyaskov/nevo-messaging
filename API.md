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

**Options:** `SignalRouterOptions` + `servers?: string[]`

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
