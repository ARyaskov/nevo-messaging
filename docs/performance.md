# Performance tuning

This page collects the knobs Nevo exposes for getting the most out of a deployment.

## Latency

### Codec & compression

- Default codec is `MessagePackCodec` — keep it unless you have a reason to switch.
- For RPC with sub-100 µs targets, **disable compression** (`compression: { enabled: false }`). MessagePack + no compression is the lowest-latency combo.
- For replies with a known stable shape, prefer `FastJsonStringifyCodec` — see [codecs.md](./codecs.md).

### Hot-path dispatch

The framework prefers a synchronous encode path when no async compression is needed (`maybeCompress` vs `maybeCompressAsync`), saving a microtask hop.

### Hidden classes

The transport drivers initialise instance fields to `null` (instead of `undefined`) so V8 keeps a single hidden class for them — already done in the NATS, Kafka, HTTP, and HTTP/2 clients.

## Throughput

### Worker-thread compression

Large payloads (≥ 16 KB) benefit from worker offload:

```ts
import { configureCompressionWorker } from "@riaskov/nevo-messaging"

configureCompressionWorker({
  enabled: true,
  poolSize: 4,
  threshold: 16_384
})
```

See [compression.md](./compression.md).

### Native zstd

```bash
npm install @napi-rs/zstd
```

When present, the compression layer prefers napi-rs over `node:zlib` — ~2× faster at similar ratios.

### Kafka

- Multiple controllers in the same process share a single consumer per group ID (automatic; no flag required).
- Use `groupIdPrefix` per environment.

### NATS slow-consumer guard

```ts
subscribeMaxPending: 1024,
subscribeOnSlow: (info) => logger.warn(info, "slow consumer")
```

Surfaces lag before subscribers fall over.

## Memory

### LRU bounds everywhere

Always set explicit caps:

```ts
idempotency: { enabled: true, maxEntries: 10_000, ttlMs: 60_000 }
replayProtection: { enabled: true, maxEntries: 100_000, windowMs: 300_000 }
rateLimit: { enabled: true, maxEntries: 10_000, idleEvictMs: 600_000 }
```

Unbounded caches are the single most common Node memory leak.

### Detached idempotency buffers

The framework already copies long-lived idempotency entries into freshly allocated buffer slabs (`Buffer.allocUnsafeSlow`) to avoid pinning shared pool memory. No action needed.

### WeakRef DevTools subscribers

`bus.onWeak(holder, handler)` registers a subscriber via `FinalizationRegistry`. Forgotten subscriptions are cleaned up at GC time — no manual `unsubscribe` required.

## Network

### HTTP/1.1 client

```ts
createNevoHttpClient(coords, {
  clientIdPrefix: "frontend",
  keepAlive: true,
  maxSockets: 64,
  tcpNoDelay: true,
  socketKeepAliveMs: 30_000,
  recvBufferSize: 256 * 1024,
  cacheableDns: { ttl: 60_000, maxTtl: 600_000 },
  useUndici: false
})
```

See [basics-http.md](./basics-http.md). `cacheable-lookup` and `undici` are peer-optional.

### HTTP/2 for high RPS

A single TLS session multiplexes many streams. See [basics-http2.md](./basics-http2.md). Currently query-only.

## Observability budget

- Tracing 100% of requests is rarely needed — start at `sampleRate: 0.1` and rely on a tail sampler in your OTel Collector for errors / outliers.
- Metric label cardinality is the silent killer — never label by `userId`, `uuid`, or anything unbounded.

## Profile-driven tuning

Don't tune blindly. Capture a flamegraph:

```bash
node --cpu-prof --cpu-prof-interval=100 dist/main.js
```

Or use `--inspect` + Chrome DevTools' Performance tab. Common bottlenecks:

1. JSON encoding/decoding → switch to MessagePack
2. Pino sync writes → enable async transport
3. Synchronous compression → enable the worker pool
4. Event-loop lag → reduce subscriber `maxInflight` for CPU-bound handlers

## What is NOT a knob

- There is no global retry budget cap — see [retry.md](./retry.md).
- There is no built-in tail-based sampler — use the OTel Collector.
- There is no distributed rate-limit / idempotency store — the in-memory caches are per-process.
