/**
 * Resilience decorators example.
 *
 * Demonstrates the four declarative decorators introduced in v2.3 of
 * `@riaskov/nevo-messaging`:
 *
 *   - @Hedge          — latency-driven duplicate request
 *   - @CircuitBreaker — sliding-window error-rate breaker
 *   - @Adaptive       — auto-tune retries / timeout against observed p99
 *   - @Backpressure   — high/low-watermark gate around a subscribe handler
 *
 * Plus the two surrounding pieces:
 *   - RedisIdempotencyStore     — distributed L2 idempotency cache (Redis)
 *   - ConsulDiscoveryProvider   — feed Consul health into DiscoveryRegistry
 *   - KubernetesDnsDiscoveryProvider — feed headless-service DNS into the same registry
 *
 * Run with a local NATS broker (`docker run --rm -p 4222:4222 nats:2 -js`)
 * and a local Redis (`docker run --rm -p 6379:6379 redis:7`).
 *
 * NB: this example is illustrative — it shows the API surface and how the
 * pieces compose. It is not meant to be deployed verbatim.
 */

import "reflect-metadata"
import { Inject, Injectable, Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import Redis from "ioredis"

import {
  // Transport
  createNatsMicroservice,
  createNevoNatsClient,
  NatsClientBase,
  NevoNatsClient,
  NatsSignalRouter,
  Signal,

  // Resilience decorators (new in v2.3)
  Hedge,
  CircuitBreaker,
  Adaptive,
  Backpressure,
  snapshotResilience,

  // Distributed idempotency (new in v2.3)
  RedisIdempotencyStore,
  type IdempotencyRedisLike,

  // Discovery providers (new in v2.3)
  DiscoveryRegistry,
  ConsulDiscoveryProvider,
  KubernetesDnsDiscoveryProvider,
  attachDiscoveryProvider,

  // Misc
  createLogger,
  setDefaultLogger
} from "@riaskov/nevo-messaging"
import { Controller } from "@nestjs/common"

// ---------------------------------------------------------------------------
// 1. Redis-backed idempotency store
// ---------------------------------------------------------------------------

function buildRedisIdempotency() {
  const ioredis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379")
  const client: IdempotencyRedisLike = {
    async get(key) {
      return ioredis.get(key)
    },
    async set(key, value, opts) {
      // NX = only set if absent; PX = millisecond TTL
      if (opts.ifNotExists) return ioredis.set(key, value, "PX", opts.ttlMs, "NX")
      return ioredis.set(key, value, "PX", opts.ttlMs)
    },
    async del(key) {
      return ioredis.del(key)
    }
  }
  return new RedisIdempotencyStore({
    client,
    enabled: true,
    ttlMs: 5 * 60_000,
    keyPrefix: "demo:idem:"
  })
}

// ---------------------------------------------------------------------------
// 2. Service with all four resilience decorators
// ---------------------------------------------------------------------------

@Injectable()
export class CatalogService extends NatsClientBase {
  constructor(@Inject("NEVO_NATS_CLIENT") client: NevoNatsClient) {
    super(client)
  }

  // Idempotent read with hedging + sliding-window breaker + adaptive tuning.
  // Layered as: breaker → hedge → invoke; adaptive observes the full duration.
  @Hedge({ copies: 1, delayMs: 40 })
  @CircuitBreaker({
    mode: "sliding",
    windowMs: 10_000,
    errorRateThreshold: 0.5,
    minSampleSize: 20,
    resetTimeoutMs: 30_000
  })
  @Adaptive({ targetP99Ms: 250, minRetries: 1, maxRetries: 4 })
  async getProduct(id: bigint) {
    return this.query("catalog", "catalog.getProduct", { id })
  }

  // Subscribe handler with explicit backpressure. When 160 messages are
  // in flight, the JetStream consumer is paused; it resumes below 80.
  // Overflow nacks so the broker redelivers later.
  @Backpressure({
    maxInflight: 200,
    highWatermark: 160,
    lowWatermark: 80,
    onOverflow: "nack"
  })
  async onProductUpdated(msg: { id: bigint; ts: number }) {
    // Do something expensive (project to a read model, etc.)
    await new Promise((r) => setTimeout(r, 25))
    return { ok: true, id: msg.id }
  }
}

// ---------------------------------------------------------------------------
// 3. Controller
// ---------------------------------------------------------------------------

// The distributed idempotency store is honoured by the signal-router DECORATOR
// (also Kafka/HTTP equivalents accept it the same way) — NOT by
// `createNatsMicroservice`, whose only options are microserviceName / module /
// port? / host? / debug? / onInit?.
@Controller()
@NatsSignalRouter([CatalogService], { idempotencyStore: buildRedisIdempotency() })
export class CatalogController {
  constructor(@Inject(CatalogService) private readonly catalog: CatalogService) {}

  @Signal("catalog.getProduct", "getProduct", (d: any) => [d.id])
  getProduct() {}

  @Signal("catalog.product.updated", "onProductUpdated", (d: any) => [d])
  onProductUpdated() {}
}

// ---------------------------------------------------------------------------
// 4. Module
// ---------------------------------------------------------------------------

@Module({
  imports: [ConfigModule],
  controllers: [CatalogController],
  providers: [
    CatalogService,
    createNevoNatsClient(["CATALOG"], {
      clientIdPrefix: "catalog",
      servers: [process.env.NATS_URL ?? "nats://127.0.0.1:4222"]
    })
  ]
})
export class AppModule {}

// ---------------------------------------------------------------------------
// 5. Discovery wiring (optional — pick one or both)
// ---------------------------------------------------------------------------

async function wireDiscovery(registry: DiscoveryRegistry) {
  const consulUrl = process.env.CONSUL_URL
  const k8sServices = process.env.K8S_DISCOVER_SERVICES // comma-separated

  if (consulUrl) {
    const consul = new ConsulDiscoveryProvider({
      url: consulUrl,
      serviceNames: ["catalog", "billing"],
      pollIntervalMs: 5_000,
      // Use blocking-queries for push-style updates if your Consul allows long polls:
      waitMs: 30_000,
      token: process.env.CONSUL_TOKEN
    })
    await attachDiscoveryProvider(registry, consul)
  }

  if (k8sServices) {
    const k8s = new KubernetesDnsDiscoveryProvider({
      services: k8sServices.split(",").map((name) => ({ name: name.trim(), port: 8080 })),
      clusterDomain: "svc.cluster.local",
      pollIntervalMs: 10_000
    })
    await attachDiscoveryProvider(registry, k8s)
  }
}

// ---------------------------------------------------------------------------
// 6. Bootstrap
// ---------------------------------------------------------------------------

async function bootstrap() {
  const logger = createLogger({ name: "catalog", level: "info" })
  setDefaultLogger(logger)

  // The Redis idempotency store is wired on `@NatsSignalRouter` (see
  // CatalogController above), where the signal-router reads it: L1 LRU first,
  // then the Redis L2, with awaited write-through on success.
  // `createNatsMicroservice` just boots the Nest app hosting that controller.
  const app = await createNatsMicroservice({
    microserviceName: "catalog",
    module: AppModule,
    port: 8090,
    host: "0.0.0.0"
  })

  // External discovery (optional)
  const registry = new DiscoveryRegistry()
  await wireDiscovery(registry)

  // Periodically log the resilience snapshot so you can watch the breaker
  // / adaptive tuner / backpressure react. This is what DevTools renders.
  const timer = setInterval(() => {
    const snap = snapshotResilience()
    if (Object.keys(snap.adaptive).length || snap.sliding || Object.keys(snap.backpressure).length) {
      logger.info({ resilience: snap }, "resilience snapshot")
    }
  }, 5_000)
  timer.unref()
}

bootstrap().catch((err) => {
  console.error(err)
  process.exit(1)
})
