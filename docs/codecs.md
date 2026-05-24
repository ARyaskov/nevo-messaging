# Codecs

A codec encodes the envelope to bytes and decodes bytes back to the envelope. Nevo ships four implementations and accepts your own via the `Codec` interface.

## API

```ts
interface Codec {
  name: string
  contentType: string
  encode(value: unknown): Uint8Array
  decode(buf: Uint8Array): unknown
}

import {
  JsonCodec,            // name: "json"
  JsonCodecFast,        // name: "json-fast"
  MessagePackCodec,     // name: "msgpack"
  FastJsonStringifyCodec,  // name: configurable
  getDefaultCodec, setDefaultCodec,
  getCodec, registerCodec,
  getSharedTextDecoder, getSharedTextEncoder
} from "@riaskov/nevo-messaging"
```

## Built-in codecs

| Codec | Speed | Size | When to use |
|---|---|---|---|
| `MessagePackCodec` | ★★★ | ★★★★ | Default. Fast, compact, native BigInt. |
| `JsonCodec` | ★★ | ★ | Debuggability — payloads readable in logs/captures. |
| `JsonCodecFast` | ★★★ | ★ | JSON with fewer allocations on hot paths. |
| `FastJsonStringifyCodec` | ★★★★ | ★ | JSON with a precompiled schema — highest throughput when shape is known. |

Default codec is whatever `getDefaultCodec()` returns; the framework initialises it to `MessagePackCodec` on import.

## Selecting a codec

```ts
import { JsonCodec, MessagePackCodec } from "@riaskov/nevo-messaging"

createNevoNatsClient(["USER"], {
  clientIdPrefix: "frontend",
  codec: new JsonCodec()
})
```

The framework reads `meta.codec` (or a one-byte content-type hint) on inbound envelopes and dispatches to the matching registered codec — peers don't need to agree on codec choice.

## MessagePack details

- `useBigInt64: true` — native BigInt → int64 mapping
- Shared encoder / decoder instances per process (created once)
- Custom extension support via the `@msgpack/msgpack` extension API

Peer dep: `npm install @msgpack/msgpack`.

## fast-json-stringify

If reply shapes are static, `FastJsonStringifyCodec` produces serializers ~2–5× faster than `JSON.stringify`:

```ts
import { FastJsonStringifyCodec } from "@riaskov/nevo-messaging"

const codec = new FastJsonStringifyCodec({
  name: "json-fjs-user-getById",
  schema: {
    type: "object",
    properties: {
      id: { type: "string" },     // bigint → string
      name: { type: "string" }
    },
    required: ["id", "name"]
  }
})
```

You can register multiple instances with distinct names — one per reply shape — and route between them in a custom encoder layer.

Peer dep: `npm install fast-json-stringify`.

## Custom codec

```ts
import type { Codec } from "@riaskov/nevo-messaging"

class CborCodec implements Codec {
  name = "cbor"
  contentType = "application/cbor"
  encode(env) { return cborEncode(env) }
  decode(buf) { return cborDecode(buf) }
}

import { registerCodec, setDefaultCodec } from "@riaskov/nevo-messaging"
registerCodec(new CborCodec())
setDefaultCodec(getCodec("cbor")!)
```

## Shared text encoder/decoder

`getSharedTextDecoder()` / `getSharedTextEncoder()` return process-singleton `TextDecoder` / `TextEncoder` instances — use them in custom codecs to avoid allocating a fresh one per call.

## See also

- [BigInt handling](./bigint.md)
- [Compression](./compression.md)
- [Schema validation](./schema.md)
