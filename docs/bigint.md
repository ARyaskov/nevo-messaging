# BigInt handling

`bigint` is a first-class type in JavaScript but JSON has no native representation for it. Nevo preserves BigInts across the wire transparently.

## How it works

| Codec | Wire representation |
|---|---|
| MessagePack | Sentinel string `@@nevo:bigint:<digits>` |
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

`parseWithBigInt(str, { acceptLegacy: true })` also recognises the older `"1n"` shape (used pre-2.0) for backwards compatibility on stored data. Legacy decoding is opt-in; ordinary strings such as `"42n"` remain strings by default.

## Example

```ts
async getUserId(): Promise<bigint> {
  return 9_007_199_254_740_993n   // larger than Number.MAX_SAFE_INTEGER
}

// Caller
const id = await this.query<bigint>("user", "user.getId", {})
typeof id === "bigint"  // true
```

## Cross-codec type matrix

The built-in codecs normalize the JSON-compatible types consistently:

| JavaScript value | Decoded value |
|---|---|
| `bigint` | Exact arbitrary-precision `bigint` |
| `Date` | ISO-8601 string |
| Object property with `undefined` | Omitted |
| Array element with `undefined` | `null` |

MessagePack deliberately does not use `useBigInt64`: that option silently wraps values outside signed/unsigned 64-bit range. The sentinel representation preserves arbitrary-size BigInts without truncation.

## Performance

Normalization adds a bounded object walk before encoding. MessagePack remains the compact default, while all built-in codecs now preserve the same application-level values.
