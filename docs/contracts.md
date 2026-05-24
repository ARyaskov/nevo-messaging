# Type-safe contracts

A "contract" in Nevo is the **reflected** description of every signal a service exposes. There is no separate contract DSL — the framework builds the contract automatically from your `@Signal` declarations on the controller and (optionally) `@Schema` declarations on the service.

The contract drives:

- TypeScript client generation (`nevo-contract` CLI)
- OpenAPI / AsyncAPI generation (see [openapi.md](./openapi.md))
- Discovery capabilities (`capabilities` field in heartbeats)

## How it works

1. Decorate signals with `@Signal("user.getById", "getById", ...)` — this registers the signal in the controller's method registry.
2. Optionally decorate the service method with `@Schema(zodOrDtoClass)` — the schema descriptor is attached to the registry entry.
3. The framework exposes a built-in signal `nevo.contract` on every service that returns the full `ServiceContract` object.

A consumer can fetch the contract over the wire and either:

- Generate a typed client offline (CLI)
- Render it to OpenAPI/AsyncAPI
- Compare versions across deploys to detect breaking changes

## The `ServiceContract` shape

```ts
interface ServiceContract {
  protocol: "1"
  serviceName: string
  serviceVersion?: string
  instanceId?: string
  capabilities?: string[]
  generatedAt: number   // unix ms
  methods: Array<{
    signalName: string
    version: string                                 // "v1" by default
    paramsSchema?: SchemaDescriptor | null
    resultSchema?: SchemaDescriptor | null
  }>
}

interface SchemaDescriptor {
  kind: "zod" | "class-validator" | "json-schema" | "unknown"
  shape?: unknown
  className?: string
  raw?: string
}
```

`paramsSchema` is filled when you annotate the service method with `@Schema(...)`; `resultSchema` is currently always `null` (handlers don't declare a result schema).

## Annotating methods

### Zod

```ts
import { Injectable, Inject } from "@nestjs/common"
import { NatsClientBase, NevoNatsClient, Schema } from "@riaskov/nevo-messaging"
import { z } from "zod"

const GetByIdInput = z.object({ id: z.bigint() })

@Injectable()
export class UserService extends NatsClientBase {
  constructor(@Inject("NEVO_NATS_CLIENT") client: NevoNatsClient) { super(client) }

  @Schema(GetByIdInput)
  async getById(id: bigint) { ... }
}
```

The zod schema is walked recursively and serialised as a portable JSON tree (`ZodString` → `{ type: "string" }`, `ZodObject` → `{ type: "object", fields: ... }`, etc.).

### class-validator

```ts
import { IsString, IsEmail } from "class-validator"
import { Schema } from "@riaskov/nevo-messaging"

export class CreateUserDto {
  @IsString() name!: string
  @IsEmail()  email!: string
}

@Schema(CreateUserDto)
async create(input: CreateUserDto) { ... }
```

For class-validator, only `className` is captured in the contract — the framework can validate at runtime but cannot serialise the rules into a portable schema (you would need a class-validator → JSON Schema converter to bridge that gap).

### Anything with `.parse` / `.safeParse` / `.validate`

`@Schema` accepts any object that exposes one of these methods. It is captured with `kind: "unknown"` in the contract, but still used for runtime validation.

## Built-in `nevo.contract` signal

Every Nevo controller automatically responds to `nevo.contract`:

```ts
const contract = await this.query<ServiceContract>("user", "nevo.contract", {})
console.log(contract.methods)
```

This signal is registered for free by the framework (see `src/common/base.controller.ts:211` and `src/signal-router.utils.ts`). You don't need to declare it in your controller.

## CLI: `nevo-contract`

The `nevo-contract` CLI fetches a remote service's contract and generates a typed TypeScript module.

```bash
# Over HTTP
npx nevo-contract --transport http \
                  --url http://user.internal:8086 \
                  --service user \
                  --out src/clients/user.contract.ts

# Over NATS
npx nevo-contract --transport nats \
                  --servers nats://127.0.0.1:4222 \
                  --service user \
                  --out src/clients/user.contract.ts
```

Flags:

| Flag | Default | Meaning |
|---|---|---|
| `-t, --transport` | `http` | `http` or `nats` |
| `-s, --service` | (required) | Service name to query (subject becomes `<service>-events`) |
| `-u, --url` | — | Base URL for HTTP transport |
| `--servers` | `nats://127.0.0.1:4222` | Comma-separated NATS servers |
| `-o, --out` | stdout | Output `.ts` path |
| `--timeout` | `10000` | Request timeout, ms |
| `--name` | — | Override the generated interface name |
| `--auth-token` | — | Bearer token attached as `meta.auth.token` |
| `--print` | off | Force stdout |

The generated module contains:

- The `ServiceContract` as a constant
- TypeScript interfaces for each signal (best-effort, derived from zod descriptors)
- A small client class with one method per signal

## Hot-reloading the contract at runtime

`ContractPoller` re-fetches `nevo.contract` on an interval. Useful in development if you want client types to refresh as the upstream evolves:

```ts
import { ContractPoller } from "@riaskov/nevo-messaging"

const poller = new ContractPoller({
  fetch: () => natsClient.query("user", "nevo.contract", {}),
  intervalMs: 30_000,
  onChange: (next) => console.log("Contract changed", next.methods.length)
})
await poller.start()
```

In production, regenerate types as part of your build pipeline.

## Versioning

A signal can declare a versioned variant by suffixing `@vN`:

```ts
@Signal("user.getById@v2", "getByIdV2", (d) => [d.id])
```

The contract reports both versions in `methods`. Clients can negotiate the version they want via `meta.version` on the envelope.

## What the contract does NOT carry

- **Result schemas** — `resultSchema` is always `null` today; handlers don't declare output types.
- **Pattern kind** — the contract does not distinguish `query`, `emit`, `publish`, or `broadcast`. All signals look the same. If that distinction matters, encode it in the signal name (e.g. `*.event.*` for events, `*.broadcast.*` for broadcasts).
- **Side-effects, idempotency hints, rate limits** — the contract is a wire shape, not a behavior spec.

## See also

- [openapi.md](./openapi.md) — render the contract to OpenAPI 3.0 or AsyncAPI 2.6
- [schema.md](./schema.md) — `@Schema` decorator details
- [discovery.md](./discovery.md) — how `capabilities` flows through heartbeats
