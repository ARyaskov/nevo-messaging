# Compression

Compression reduces wire size at the cost of CPU. Nevo supports gzip, deflate, and zstd, with optional worker-thread offload.

## API

```ts
type CompressionEncoding = "gzip" | "deflate" | "zstd" | "identity"

import {
  maybeCompress, maybeCompressAsync,
  maybeDecompress, maybeDecompressAsync,
  resolveCompressionOptions,
  configureCompressionWorker,
  isCompressionWorkerEnabled,
  compressionWorkerThreshold,
  workerCompress, workerDecompress,
  shutdownCompressionWorker
} from "@riaskov/nevo-messaging"
```

## Configuration

```ts
createNevoNatsClient(["USER"], {
  clientIdPrefix: "frontend",
  compression: {
    enabled: true,
    encoding: "zstd",       // "gzip" | "deflate" | "zstd" | "identity"
    threshold: 1024,        // bytes — skip smaller payloads
    level: 3                // codec-specific level
  }
})
```

Each encoded envelope carries `meta.encoding`. Servers transparently decompress; application code does not change.

## Encoding choice

| Encoding | CPU | Ratio | Notes |
|---|---|---|---|
| `gzip` | Medium | Good | Most compatible |
| `deflate` | Medium | Good | Slightly smaller than gzip |
| `zstd` | Low (with napi-rs) | Best | Requires Node ≥ 23 zstd or `@napi-rs/zstd` |
| `identity` | None | None | No compression |

## zstd

Two implementations are tried in order:

1. `@napi-rs/zstd` (peer-optional, native Rust)
2. `node:zlib` built-in zstd (Node ≥ 23)

```bash
npm install @napi-rs/zstd
```

If neither is available and `encoding: "zstd"` is requested, the framework falls back to gzip with a one-time warning.

## Worker-thread offload

For large payloads, push compression onto a worker pool to keep the event loop free:

```ts
import { configureCompressionWorker } from "@riaskov/nevo-messaging"

configureCompressionWorker({
  enabled: true,
  poolSize: 4,
  threshold: 16_384,
  logger: getDefaultLogger()
})
```

Below `threshold`, compression stays on the main thread (worker round-trip would cost more than the work). Above, the framework uses `maybeCompressAsync` to offload via the pool.

`isCompressionWorkerEnabled()` / `compressionWorkerThreshold()` expose the configured state.

`shutdownCompressionWorker()` tears the pool down — call it from your shutdown hook.

## Decompression

Automatic on receive: the framework reads `meta.encoding` and applies the matching decoder. No configuration needed on the consumer side.

## Sync vs. async paths

The codec API has two encode paths:

- `maybeCompress(buf, opts)` — synchronous (no worker)
- `maybeCompressAsync(buf, opts)` — uses the worker pool when enabled and over threshold

`query()` calls the sync path when possible and falls back to async only when needed, keeping latency low for small messages.

## When to skip

- Payloads smaller than `threshold` (default 1 KB)
- Pre-compressed content (images, video)
- High-frequency low-latency RPC (under 100 µs target)

## Recommendations

| Scenario | Recommendation |
|---|---|
| Default | `zstd` at level 3, threshold 1024 |
| Latency-critical RPC | `identity` |
| Large analytics payloads | `zstd` level 6 + worker pool |
| Cross-WAN replication | `zstd` level 9 + worker pool |
