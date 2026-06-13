# Migration & upgrade notes

This page collects the behavioural changes that can affect an upgrade, oldest first, ending with the current batch. Most changes are backward-compatible by design (sentinels and decoders accept the legacy form); the ones that need action are called out with **Action**.

> **Always pin both ends to the same minor while rolling.** Wire-format defaults (codec, BigInt encoding, meta keys) must match between caller and callee. During a rolling deploy, prefer to pin the explicit codec on both sides until every pod is upgraded.

## MessagePack is the default codec

The default wire codec is **MessagePack** whenever `@msgpack/msgpack` is resolvable; otherwise the framework falls back to JSON automatically. `getDefaultCodec()` probes msgpack once (`encode({ probe: 1 })`) and caches the result, so an environment without the optional dependency degrades gracefully to `JsonCodec` rather than throwing.

**Action / compatibility:**

- A MessagePack producer and a JSON consumer **cannot** talk. If you mix codecs across services, set one explicitly on every client/router (`codec: new JsonCodec()` or `getCodec("json")`), or ensure msgpack is installed everywhere.
- The content type is negotiated per message (the codec name travels in meta / the `content-type` header), so a service can decode whatever a peer sent — but it *encodes* with its own default. Pin the codec during a rolling upgrade so a half-upgraded fleet doesn't split into msgpack and JSON islands.
- To force JSON globally: `setDefaultCodec(new JsonCodec())`. Available codecs: `json`, `json-fast`, `msgpack` (see [codecs.md](./codecs.md)).

## BigInt sentinel encoding

`bigint` values are encoded as a sentinel string so they survive JSON / MessagePack / JSONB round-trips. The current sentinel is:

```
@@nevo:bigint:<digits>     e.g. 123n  →  "@@nevo:bigint:123"
```

The **legacy** form `"<digits>n"` (e.g. `"123n"`) is accepted only when `acceptLegacy: true` is passed to the BigInt helpers. Built-in wire codecs keep it disabled so ordinary strings are never silently converted.

**Action / compatibility:**

- No action for normal client→handler traffic — encoding/decoding is automatic.
- If you persist payloads yourself (custom store, manual `JSON.stringify`), encode BigInts first with `serializeBigInt(obj)` / `stringifyWithBigInt(obj)` and decode with `deserializeBigInt` / `parseWithBigInt` (from `bigint.utils`). A raw `bigint` passed to `JSON.stringify` throws.
- Old data written with the `"<digits>n"` form is still readable; new writes use the `@@nevo:bigint:` sentinel. See [bigint.md](./bigint.md).

## Metadata key renames

Correlation metadata on `MessageMeta` uses `nevo`-prefixed keys to avoid collisions with user headers:

| Concept | Current key | Legacy alias still accepted |
|---|---|---|
| Chain / correlation id | `nevoChainId` | `chainId` |
| Parent envelope uuid | `nevoParentUuid` | `parentUuid` |
| Calling service | `service` | `callerService` |

The runtime reads the canonical key first and falls back to the legacy alias where one exists (e.g. the audit log reads `meta.callerService` as a fallback for `service`). `MessageMeta` has an index signature, so unknown keys pass through untouched.

**Action:** if any of your code constructs meta by hand or asserts on specific keys, switch reads/writes to the canonical names (`nevoChainId`, `nevoParentUuid`, `service`). You normally don't touch these — the transports and [chain context](./chain-context.md) populate them for you.

## ErrorCode values

`ErrorCode` is a numeric enum and the **codes travel on the wire** inside the error envelope. A peer on a different version maps an unknown number to its own enum, so renumbering shifts meaning. The current set is stable at:

```
UNKNOWN=0  UNAUTHORIZED=1  TIMEOUT=2  METHOD_NOT_FOUND=3  SERVICE_NOT_FOUND=4
SERVICE_UNAVAILABLE=5  BAD_REQUEST=6  VALIDATION_FAILED=7  RATE_LIMITED=8
CIRCUIT_OPEN=9  PAYLOAD_TOO_LARGE=10  PARSE_ERROR=11  REPLAY_DETECTED=12
IDEMPOTENT_REPLAY=13  CONNECTION_LOST=14  INTERNAL=15  CANCELLED=16
UNSUPPORTED_VERSION=17  ACK_FAILED=18  REMOTE_ERROR=19
```

**Action / compatibility:**

- Compare against the **named** members (`ErrorCode.RATE_LIMITED`), never hard-coded numbers, so a future shift doesn't break your checks.
- `isRetryable(code)` is the source of truth for which codes are retryable (`TIMEOUT`, `SERVICE_UNAVAILABLE`, `CONNECTION_LOST`, `INTERNAL`).
- When caller and callee straddle a version that added codes, a newer code received by an older peer falls through to `UNKNOWN`/`REMOTE_ERROR` handling — upgrade consumers first if you depend on a newly added code. See [error-codes.md](./error-codes.md).

## This batch

### `query` / `emit` default result type is now `unknown`

The generic result parameter on `query` (and the payload typing on `emit`) defaults to **`unknown`** instead of `any`. This is a **compile-time** change, not a wire change.

**Action:** untyped call sites that previously inferred `any` now get `unknown` and must narrow or supply the type argument:

```ts
const user = await client.query<User>("user", "user.get", { id })   // explicit type
const raw = await client.query("user", "user.get", { id })          // raw is `unknown` now
if (isUser(raw)) { /* narrowed */ }
```

This catches a class of bugs where an unvalidated response was used as if it were a known shape. The fix is mechanical: add the type argument (ideally backed by a [validated contract](./contracts.md)) or narrow the `unknown`.

### `@nestjs/*` peer dependencies are required

The `@nestjs/common`, `@nestjs/core`, `@nestjs/microservices`, `@nestjs/config`, and `@nestjs/platform-fastify` peers are **no longer optional** — the framework targets NestJS 11 and assumes they are present. Transport SDKs (kafkajs, the NATS packages, socket.io, ws, undici, …) and other integrations (msgpack, zod, prom-client, pino, OTel) remain **optional** peers: install only the transports/features you use, and the relevant module is loaded lazily.

**Action:** ensure `@nestjs/common@^11`, `@nestjs/core@^11`, `@nestjs/microservices@^11`, `@nestjs/config@^4`, and `@nestjs/platform-fastify@^11` are installed in the host app. Missing optional peers only error when you actually touch that transport/feature (e.g. msgpack falls back to JSON; a transport throws a clear "missing optional dependency" message).

### Package exports map

The package ships an `exports` map. Import from the **package root**:

```ts
import { NevoNatsClient, WorkflowEngine, Scheduler } from "@riaskov/nevo-messaging"
```

**Action:** stop deep-importing internal paths like `@riaskov/nevo-messaging/dist/common/...`. Deep paths are not part of the public surface and the exports map may not expose them. Everything documented is re-exported from the root (see [API.md](../API.md)).

### Node engines: `>= 24`

The package requires **Node.js 24 or newer**.

**Action:** upgrade your runtime to Node ≥ 24. The built-in `node:sqlite` module used by `SqliteOutboxStore` is available without a flag on Node 24+ (it was behind `--experimental-sqlite` on Node 22.5–23.x).

## Upgrade checklist

1. Bump the runtime to Node ≥ 24.
2. Install/confirm the required `@nestjs/*` peers; add only the optional peers for the transports and features you use.
3. Replace any deep imports with root imports.
4. Add explicit type arguments (or narrowing) to untyped `query` calls now that the default is `unknown`.
5. Pin an explicit codec on every client/router while rolling, or verify msgpack is installed fleet-wide.
6. Switch any hand-rolled meta reads to the canonical keys (`nevoChainId`, `nevoParentUuid`, `service`).
7. Run the [production checklist](./production-checklist.md) before going live.

## See also

- [codecs.md](./codecs.md) · [bigint.md](./bigint.md) · [error-codes.md](./error-codes.md)
- [production-checklist.md](./production-checklist.md)
- [API.md](../API.md) — the public export surface
