# BigInt handling

`bigint` is a first-class type in JavaScript but JSON has no native representation for it. Nevo preserves BigInts across the wire transparently.

## How it works

| Codec | Wire representation |
|---|---|
| MessagePack | Native int64 via `useBigInt64: true` |
| JSON / JsonFast | Sentinel string `@@nevo:bigint:<digits>` |
| fast-json-stringify | Sentinel string (declare the property as `{ type: "string" }`) |

A handler that returns `1n` is received by the caller as `1n` regardless of codec.

## Sentinel format

```ts
import { BIGINT_SENTINEL } from "@riaskov/nevo-messaging"
// "@@nevo:bigint:"

JSON.stringify({ id: 1n }, bigIntReplacer)
// → '{"id":"@@nevo:bigint:1"}'
```

`bigIntReplacer` is a `JSON.stringify` replacer; `makeBigIntReviver()` builds a matching reviver:

```ts
import {
  serializeBigInt, deserializeBigInt,
  stringifyWithBigInt, parseWithBigInt,
  bigIntReplacer, makeBigIntReviver
} from "@riaskov/nevo-messaging"

const json = stringifyWithBigInt({ id: 1n })
const parsed = parseWithBigInt(json)
// parsed.id === 1n
```

`parseWithBigInt(str, { acceptLegacy: true })` also recognises the older `"1n"` shape (used pre-2.0) for backwards compatibility on stored data.

## Example

```ts
async getUserId(): Promise<bigint> {
  return 9_007_199_254_740_993n   // larger than Number.MAX_SAFE_INTEGER
}

// Caller
const id = await this.query<bigint>("user", "user.getId", {})
typeof id === "bigint"  // true
```

## Custom extension types

The MessagePack codec supports `@msgpack/msgpack` extensions natively (`Map`, `Set`, `Date`, `Buffer`). The JSON codecs only handle BigInt as a special case — for other non-JSON types in JSON-mode, use a string convention of your own design.

## Performance

The sentinel adds a small per-encode/decode scan to detect BigInts in JSON. For payloads with many fields and few BigInts, prefer MessagePack — it skips the scan entirely. For pure JSON, sentinel overhead is roughly 2–3% on typical payloads.
