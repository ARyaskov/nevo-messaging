# Nevo Messaging

A powerful microservices messaging framework for NestJS 11+ with multi-transport support (NATS, Kafka, Socket.IO, HTTP/SSE), designed for building scalable distributed systems with type-safe inter-service communication.

## Features

- 🚀 **Type-safe messaging** - Full TypeScript support with auto-completion
- 🔄 **Dual communication patterns** - Both request-response (query) and fire-and-forget (emit)
- 📡 **Subscriptions** - Publish/subscribe updates without direct requests
- 📢 **Broadcast** - System-wide messages for all connected consumers
- 🎯 **Signal-based routing** - Declarative method mapping with `@Signal` decorator
- 📡 **Kafka transport** - Production-ready Apache Kafka integration
- 🧭 **Service discovery** - Heartbeat-based registry topic
- 🔐 **Access control** - Topic + method + service-level ACLs
- 🔌 **Multiple transports** - NATS, Kafka, Socket.IO, HTTP (SSE)
- 🔧 **Auto-configuration** - Automatic topic creation and client setup
- 🛡️ **Error handling** - Comprehensive error propagation and timeout management
- 📊 **BigInt support** - Native handling of large integers across services
- 🪝 **Lifecycle hooks** - Before/after message processing hooks
- 🔍 **Debug mode** - Built-in logging for development and troubleshooting

## Installation

```bash
npm install @riaskov/nevo-messaging
```

### Peer Dependencies

```bash
npm install @nestjs/common @nestjs/core @nestjs/microservices @nestjs/config @nestjs/platform-fastify kafkajs nats socket.io socket.io-client rxjs reflect-metadata
```

## Quick Start

### 1. Basic Service Setup (NATS)

Create a simple microservice that responds to messages:

```typescript
// user.service.ts
import { Injectable, Inject } from "@nestjs/common"
import { NatsClientBase, NevoNatsClient } from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends NatsClientBase {
  constructor(@Inject("NEVO_NATS_CLIENT") nevoClient: NevoNatsClient) {
    super(nevoClient)
  }

  async getById(id: bigint) {
    return { id, name: "John Doe", email: "john@example.com" }
  }

  async create(userData: { name: string; email: string }) {
    const newUser = { id: 123n, ...userData }
    return newUser
  }
}
```

### 2. Signal Router Controller

Map service methods to external signals using the `@Signal` decorator:

```typescript
// user.controller.ts
import { Controller, Inject } from "@nestjs/common"
import { NatsSignalRouter, Signal } from "@riaskov/nevo-messaging"
import { UserService } from "./user.service"

@Controller()
@NatsSignalRouter([UserService])
export class UserController {
  constructor(@Inject(UserService) private readonly userService: UserService) {}

  @Signal("user.getById", "getById", (data: any) => [data.id])
  getUserById() {}

  @Signal("user.create", "create", (data: any) => [data])
  createUser() {}
}
```

### 3. Module Configuration

Configure the module with the NATS client:

```typescript
// user.module.ts
import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import { createNevoNatsClient } from "@riaskov/nevo-messaging"
import { UserController } from "./user.controller"
import { UserService } from "./user.service"

@Module({
  imports: [ConfigModule],
  controllers: [UserController],
  providers: [
    UserService,
    createNevoNatsClient(["COORDINATOR"], {
      clientIdPrefix: "user",
      servers: ["nats://127.0.0.1:4222"]
    })
  ]
})
export class UserModule {}
```

### 4. Application Bootstrap

Start your service:

```typescript
// main.ts
import { NestFactory } from "@nestjs/core"
import { AppModule } from "./app.module"

async function bootstrap() {
  const app = await NestFactory.create(AppModule)
  await app.listen(8086)
}

bootstrap()
```
## Core Concepts

### Signal Routing

The Signal Router pattern allows you to declaratively map external message patterns to internal service methods:

```typescript
@Signal("external.signal.name", "internalMethodName", parameterTransformer?, resultTransformer?)
```

### Communication Patterns

#### Query Pattern (Request-Response)
Use for operations that need a response:

```typescript
const user = await this.query("user", "user.getById", { id: 123n })
```

#### Emit Pattern (Fire-and-Forget)
Use for events and notifications:

```typescript
await this.emit("notifications", "user.created", { userId: 123n, email: "user@example.com" })
```

#### Subscription Pattern (Publish/Subscribe)
Use when you want to receive updates without requesting:

```typescript
const sub = await this.subscribe("user", "user.updated", { ack: true }, async (msg, ctx) => {
  await ctx.ack()
})

await sub.unsubscribe()
```

Publish updates:

```typescript
await this.publish("user", "user.updated", { userId: 123n })
```

#### Broadcast Pattern (System-Wide)
Send to everyone connected to the broker:

```typescript
await this.broadcast("system.status", { ok: true })
```

Receive broadcast:

```typescript
await this.subscribe("__broadcast", "system.status", {}, (msg) => {
  console.log("System status:", msg)
})
```

## Advanced Usage

### Parameter Transformation

Transform incoming parameters before passing to service methods:

```typescript
@Signal("user.update", "updateUser", (data: any) => [data.id, data.changes])
updateUser() {}

// Service method signature:
async updateUser(id: bigint, changes: Partial<User>) {
  // Implementation
}
```

### Result Transformation

Transform service method results before sending response:

```typescript
@Signal(
  "user.getProfile", 
  "getById", 
  (data: any) => [data.id],
  (user: User) => ({ ...user, password: undefined }) // Remove sensitive data
)
getProfile() {}
```

### Multiple Service Dependencies

Route signals to different services within the same controller:

```typescript
@Controller()
@KafkaSignalRouter([UserService, ProfileService, NotificationService])
export class UserController {
  constructor(
    @Inject(UserService) private readonly userService: UserService,
    @Inject(ProfileService) private readonly profileService: ProfileService,
    @Inject(NotificationService) private readonly notificationService: NotificationService
  ) {}

  @Signal("user.create", "createUser", (data: any) => [data])
  createUser() {}

  @Signal("profile.update", "updateProfile", (data: any) => [data.userId, data.profile])
  updateProfile() {}

  @Signal("notification.send", "sendNotification", (data: any) => [data.userId, data.message])
  sendNotification() {}
}
```

### Cross-Service Communication

Services can communicate with each other through the messaging layer:

```typescript
@Injectable()
export class OrderService extends KafkaClientBase {
  constructor(@Inject("NEVO_KAFKA_CLIENT") nevoClient: NevoKafkaClient) {
    super(nevoClient)
  }

  async createOrder(orderData: CreateOrderDto) {
    // Create the order
    const order = await this.saveOrder(orderData)

    // Query user service for user details
    const user = await this.query("user", "user.getById", { id: orderData.userId })

    // Query inventory service to reserve items
    const reservation = await this.query("inventory", "item.reserve", {
      items: orderData.items,
      orderId: order.id
    })

    // Emit event to notification service
    await this.emit("notifications", "order.created", {
      orderId: order.id,
      userId: user.id,
      userEmail: user.email
    })

    return order
  }
}
```

### Lifecycle Hooks

Add custom logic before and after message processing:

```typescript
@KafkaSignalRouter([UserService], {
  before: async (context) => {
    console.log(`Processing ${context.method} for ${context.uuid}`)
    // Validate request, log metrics, etc.
    return context.params // Can modify parameters
  },
  after: async (context) => {
    console.log(`Completed ${context.method} with result:`, context.result)
    // Log metrics, audit trail, etc.
    return context.response // Can modify response
  },
  debug: true
})
export class UserController {
  // ...
}
```

### Error Handling

The framework provides comprehensive error handling:

```typescript
import { MessagingError, ErrorCode } from "@riaskov/nevo-messaging"

@Injectable()
export class UserService extends KafkaClientBase {
  async getById(id: bigint) {
    const user = await this.findUser(id)
    
    if (!user) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: "User not found",
        userId: id
      })
    }
    
    return user
  }
}
```

### Method Suggestions (Did You Mean)

If you call a method that doesn't exist, the framework returns a helpful error:

```
Invalid method name 'user.getByI', did you mean 'user.getById'?
```

This works for all transports.

### Exponential Backoff (Client-Side)

Clients apply a backoff for **in-flight requests** to avoid sending a duplicate query while the previous one is still being processed.

```typescript
createNevoKafkaClient(["USER"], {
  clientIdPrefix: "frontend",
  backoff: {
    enabled: true,
    baseMs: 100,
    maxMs: 2000,
    maxAttempts: 0, // 0 = wait until slot is free
    jitter: true
  }
})
```

This prevents repeated sending of the same request while the service is busy (e.g., stopped on a breakpoint).

### Access Control (ACL)

Restrict who can read messages by topic + method + service:

```typescript
@KafkaSignalRouter([UserService], {
  accessControl: {
    rules: [
      { topic: "user-events", method: "*", allow: ["frontend", "coordinator"] },
      { topic: "user-events", method: "user.delete", deny: ["frontend"] }
    ],
    logDenied: true
  }
})
export class UserController {}
```

By default, all services are allowed.

### Service Discovery (Registry Topic)

Each client sends heartbeats to `__nevo.discovery`. You can read the registry:

```typescript
const services = this.getDiscoveredServices()
const isUserAvailable = this.isServiceAvailable("user")
```

Discovery is enabled by default for Kafka/NATS. HTTP and Socket.IO discovery are available when enabled (HTTP requires `discoveryUrl`).

## Configuration

### Environment Variables

```bash
# Kafka Configuration
KAFKA_HOST=localhost
KAFKA_PORT=9092
NODE_ENV=production
```

### Kafka Client Options

```typescript
createNevoKafkaClient(["USER", "INVENTORY", "NOTIFICATIONS"], {
  clientIdPrefix: "order-service",
  groupIdPrefix: "order-consumer",
  sessionTimeout: 30000,
  allowAutoTopicCreation: true,
  retryAttempts: 5,
  brokerRetryTimeout: 2000,
  timeoutMs: 25000,
  debug: false,
  discovery: {
    enabled: true,
    heartbeatIntervalMs: 5000,
    ttlMs: 15000
  }
})
```

### Microservice Startup Options

```typescript
createKafkaMicroservice({
  microserviceName: "user",
  module: AppModule,
  port: 8086,
  host: "0.0.0.0",
  debug: true,
  onInit: async (app) => {
    // Custom initialization logic
    await app.get(DatabaseService).runMigrations()
  }
})
```

## Transports

| Transport | Patterns | Discovery | Infra | Notes |
| --- | --- | --- | --- | --- |
| NATS | query/emit/publish/subscribe/broadcast | on by default | NATS server | Lowest latency, simple ops |
| Kafka | query/emit/publish/subscribe/broadcast | on by default | Kafka broker | Durable log, topic setup |
| Socket.IO | query/emit/publish/subscribe/broadcast | optional | Socket.IO server | WebSocket-friendly apps |
| HTTP (SSE) | query/emit + SSE subscribe/broadcast | optional | HTTP server | Simple HTTP/SSE integration |

### NATS (priority)
Client factory:

```typescript
createNevoNatsClient(["USER", "COORDINATOR"], {
  clientIdPrefix: "user",
  servers: ["nats://127.0.0.1:4222"]
})
```

Controller decorator:

```typescript
@NatsSignalRouter([UserService])
export class UserController {}
```

### Kafka
Use `createKafkaMicroservice` + `KafkaSignalRouter` as before.

### Socket.IO
Socket.IO server is started inside the router decorator:

```typescript
@SocketSignalRouter([UserService], { serviceName: "user", port: 8093 })
export class UserController {}
```

Client:

```typescript
createNevoSocketClient(
  { coordinator: "http://127.0.0.1:8094" },
  { clientIdPrefix: "user" }
)
```

### HTTP (SSE)
HTTP uses plain POST for `query/emit` and SSE for `subscribe`.

```typescript
@HttpSignalRouter([UserService])
export class UserController {}
```

Include transport controller to enable SSE + publish endpoints:

```typescript
controllers: [UserController, HttpTransportController]
```

Client:

```typescript
createNevoHttpClient(
  { coordinator: "http://127.0.0.1:8091" },
  { clientIdPrefix: "user" }
)
```
## BigInt Support

The framework automatically handles BigInt serialization across service boundaries:

```typescript
// Service returns BigInt
async getUserId(): Promise<bigint> {
  return 9007199254740991n // Large integer
}

// Automatically serialized as "9007199254740991n"
// Automatically deserialized back to BigInt on the receiving end
```

## Architecture Patterns

### Event Sourcing Pattern

Use events to maintain state consistency across services:

```typescript
@Injectable()
export class OrderService extends KafkaClientBase {
  async createOrder(orderData: CreateOrderDto) {
    const order = await this.saveOrder(orderData)
    
    // Emit domain events
    await Promise.all([
      this.emit("events", "order.created", {
        orderId: order.id,
        userId: order.userId,
        timestamp: new Date(),
        aggregateVersion: 1
      }),
      this.emit("events", "inventory.reserved", {
        orderId: order.id,
        items: order.items,
        timestamp: new Date()
      })
    ])
    
    return order
  }
}
```

### CQRS Pattern

Separate command and query responsibilities:

```typescript
// Command Service
@Injectable()
export class UserCommandService extends KafkaClientBase {
  async createUser(userData: CreateUserDto) {
    const user = await this.repository.save(userData)
    
    // Emit event for read model updates
    await this.emit("events", "user.created", {
      userId: user.id,
      email: user.email,
      timestamp: new Date()
    })
    
    return user
  }
}

// Query Service
@Injectable()
export class UserQueryService extends KafkaClientBase {
  async getUserProfile(userId: bigint) {
    // Optimized read model
    return this.readRepository.findUserProfile(userId)
  }
}
```

## Advanced Configuration

### Custom Message Extractors

For complex message formats:

```typescript
export function createCustomSignalRouter(serviceType: Type<any>[], options?: SignalRouterOptions) {
  return createSignalRouterDecorator(
    serviceType,
    options,
    (data) => {
      // Custom message extraction logic
      const envelope = parseWithBigInt(data.value)
      return {
        method: envelope.command.action,
        params: envelope.payload,
        uuid: envelope.metadata.correlationId
      }
    },
    (target, eventPattern, handlerName) => {
      // Custom handler registration
      MessagePattern(eventPattern)(target.prototype, handlerName, 
        Object.getOwnPropertyDescriptor(target.prototype, handlerName)!)
    }
  )
}
```

### Distributed Tracing

Implement correlation IDs for request tracing:

```typescript
@KafkaSignalRouter([UserService], {
  before: async (context) => {
    // Inject correlation ID
    const correlationId = context.uuid
    context.params.correlationId = correlationId
    
    console.log(`[${correlationId}] Starting ${context.method}`)
    return context.params
  },
  after: async (context) => {
    const correlationId = context.params.correlationId
    console.log(`[${correlationId}] Completed ${context.method}`)
    return context.response
  }
})
export class UserController {
  // ...
}
```

### Retry Policies

Configure retry behavior for failed operations:

```typescript
@Injectable()
export class ResilientService extends KafkaClientBase {
  async performOperation(data: any) {
    const maxRetries = 3
    let attempt = 0
    
    while (attempt < maxRetries) {
      try {
        return await this.query("external", "risky.operation", data)
      } catch (error) {
        attempt++
        if (attempt >= maxRetries) throw error
        
        await new Promise(resolve => setTimeout(resolve, 1000 * attempt))
      }
    }
  }
}
```

## Performance Optimization

### Batch Operations

Process multiple operations efficiently:

```typescript
@Injectable()
export class BatchUserService extends KafkaClientBase {
  async processBatch(userIds: bigint[]) {
    // Process in chunks to avoid overwhelming downstream services
    const chunkSize = 10
    const results = []
    
    for (let i = 0; i < userIds.length; i += chunkSize) {
      const chunk = userIds.slice(i, i + chunkSize)
      const chunkResults = await Promise.all(
        chunk.map(id => this.query("user", "user.getById", { id }))
      )
      results.push(...chunkResults)
    }
    
    return results
  }
}
```

### Caching Layer

Implement service-level caching:

```typescript
@Injectable()
export class CachedUserService extends KafkaClientBase {
  private cache = new Map<string, any>()
  private readonly cacheTimeout = 300000 // 5 minutes

  async getCachedUser(id: bigint) {
    const cacheKey = `user:${id}`
    const cached = this.cache.get(cacheKey)
    
    if (cached && Date.now() - cached.timestamp < this.cacheTimeout) {
      return cached.data
    }
    
    const user = await this.query("user", "user.getById", { id })
    this.cache.set(cacheKey, { data: user, timestamp: Date.now() })
    
    return user
  }
}
```

## Monitoring and Observability

### Health Checks

Implement service health monitoring:

```typescript
@Injectable()
export class HealthService extends KafkaClientBase {
  async checkServiceHealth() {
    const services = this.getAvailableServices()
    const healthChecks = await Promise.allSettled(
      services.map(async (service) => {
        try {
          await this.query(service, "health.check", {})
          return { service, status: "healthy" }
        } catch (error) {
          return { service, status: "unhealthy", error: error.message }
        }
      })
    )
    
    return healthChecks.map(result => 
      result.status === "fulfilled" ? result.value : result.reason
    )
  }
}
```

### Metrics Collection

Track message processing metrics:

```typescript
@KafkaSignalRouter([MetricsService], {
  before: async (context) => {
    context.startTime = Date.now()
    return context.params
  },
  after: async (context) => {
    const duration = Date.now() - context.startTime
    
    await this.emit("metrics", "message.processed", {
      service: context.serviceName,
      method: context.method,
      duration,
      success: context.response.params.result !== "error"
    })
    
    return context.response
  }
})
export class MetricsController {
  // ...
}
```

## Security

### Message Validation

Implement input validation:

```typescript
import { IsString, IsEmail, validate } from "class-validator"

class CreateUserDto {
  @IsString()
  name: string

  @IsEmail()
  email: string
}

@Injectable()
export class SecureUserService extends KafkaClientBase {
  async createUser(userData: any) {
    const dto = Object.assign(new CreateUserDto(), userData)
    const errors = await validate(dto)
    
    if (errors.length > 0) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: "Validation failed",
        errors: errors.map(e => e.constraints)
      })
    }
    
    return this.repository.save(dto)
  }
}
```

### Authentication Context

Pass authentication context between services:

```typescript
@KafkaSignalRouter([UserService], {
  before: async (context) => {
    // Extract and validate auth token
    const authHeader = context.rawData.headers?.authorization
    const user = await this.validateToken(authHeader)
    
    return {
      ...context.params,
      authContext: { userId: user.id, roles: user.roles }
    }
  }
})
export class SecureUserController {
  // ...
}
```

## Production Deployment

### Docker Compose Setup

```yaml
services:
  kafka:
    image: apache/kafka:4.0.0
    environment:
      - KAFKA_PROCESS_ROLES=broker,controller
      - KAFKA_NODE_ID=1
      - KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
      - KAFKA_AUTO_CREATE_TOPICS_ENABLE=true

  user-service:
    build: ./user-service
    environment:
      - KAFKA_HOST=kafka
      - KAFKA_PORT=9092
    depends_on:
      - kafka

  order-service:
    build: ./order-service
    environment:
      - KAFKA_HOST=kafka
      - KAFKA_PORT=9092
    depends_on:
      - kafka
```

### Scaling Considerations

Configure partition strategy for high throughput:

```typescript
createNevoKafkaClient(["HIGH_VOLUME_SERVICE"], {
  clientIdPrefix: "processor",
  partitionStrategy: "round-robin",
  maxInFlightRequests: 5,
  batchSize: 100,
  lingerMs: 10
})
```

## API Reference

See `API.md`.

## Examples

### NATS
- `examples/nats-user` - NATS request/response + publish/subscribe + broadcast

### Kafka
- `examples/user` - standard Kafka microservice

### Socket.IO
- `examples/socket-user` - Socket.IO transport with subscribe/broadcast

### HTTP (SSE)
- `examples/http-user` - HTTP query/emit + SSE subscribe + broadcast + discovery
## Troubleshooting

### Common Issues

**Topic Creation Failures**
```bash
# Ensure Kafka is running and accessible
docker-compose up kafka

# Check topic creation logs
docker-compose logs kafka
```

**Connection Timeouts**
```typescript
// Increase timeouts for slow networks
createNevoKafkaClient(["USER"], {
  clientIdPrefix: "app",
  timeoutMs: 30000,
  sessionTimeout: 45000
})
```

**Serialization Errors**
```typescript
// Enable debug mode to see message payloads
@KafkaSignalRouter([UserService], { debug: true })
```

### Debug Mode

Enable comprehensive logging:

```bash
NODE_ENV=development
```

```typescript
createKafkaMicroservice({
  microserviceName: "user",
  module: AppModule,
  debug: true
})
```

## Migration Guide

### From Other Messaging Libraries

If migrating from other microservice messaging solutions:

1. **Replace message handlers** with `@Signal` decorators
2. **Update service injection** to use `KafkaClientBase`
3. **Configure Kafka clients** with `createNevoKafkaClient`
4. **Update message patterns** to use signal names

### Version Compatibility

- **Node.js**: ≥24.0.0
- **NestJS**: ≥11.1.0
- **Kafka**: ≥2.8.0 (≥4.0.0 is recommended)

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Support

- GitHub Issues: [Report bugs and request features](https://github.com/ARyaskov/nevo-messaging/issues)
- Documentation: This README and inline code documentation
- Examples: Check the `examples/` directory for complete working examples

## Aux
There are many anys in core code - the simple temporary solution for changeable Nest.js microservices API.
