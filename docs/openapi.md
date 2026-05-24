# OpenAPI / AsyncAPI generation

Nevo can render a service's [contract](./contracts.md) to OpenAPI 3.0.3 or AsyncAPI 2.6. The conversion is a pure function — no decorators, no runtime hooks — so you can run it anywhere a `ServiceContract` is available: in-process, in a build script, in a CLI pipeline.

## Two entry points

```ts
import {
  buildContract,
  contractToOpenApi,
  contractToAsyncApi,
  type ServiceContract
} from "@riaskov/nevo-messaging"
```

- `buildContract(serviceName, methodRegistry, opts?)` — assemble a contract from a registered service.
- `contractToOpenApi(contract, opts?)` — render OpenAPI 3.0.3.
- `contractToAsyncApi(contract)` — render AsyncAPI 2.6.

`methodRegistry` is the map of registered signals; on a `*ClientBase`-extending service the framework keeps the registry internally. The same data is what the built-in `nevo.contract` signal returns when called over the wire.

## Quick recipe: live OpenAPI endpoint

Expose `/openapi.json` from the service itself. The simplest path uses the over-the-wire `nevo.contract` signal so it works for every transport:

```ts
import { Controller, Get } from "@nestjs/common"
import { contractToOpenApi, type ServiceContract } from "@riaskov/nevo-messaging"

@Controller()
export class DocsController {
  constructor(private readonly users: UserService) {}

  @Get("/openapi.json")
  async spec() {
    const contract = await this.users.query<ServiceContract>("user", "nevo.contract", {})
    return contractToOpenApi(contract, {
      title: "User Service",
      version: "2.0.0",
      baseUrl: "https://user.internal"
    })
  }
}
```

This works even when the service runs on NATS: the HTTP endpoint is a thin side-car that asks NATS for the contract.

## Build-time recipe

For static API portals (Redocly, Swagger UI):

```bash
# 1. Fetch the contract using the CLI
npx nevo-contract --transport nats --servers nats://127.0.0.1:4222 \
                  --service user --out tmp/user.contract.ts

# 2. Render it
node -e "
  const c = require('./tmp/user.contract.ts').CONTRACT;
  const { contractToOpenApi } = require('@riaskov/nevo-messaging');
  console.log(JSON.stringify(contractToOpenApi(c, { title: 'User Service' }), null, 2));
" > openapi.json
```

## What gets generated

For a service named `user` with four signals (`user.getById`, `user.delete`, `user.updated.notify`, `system.status`), the OpenAPI output has four paths under `/user-events/<signal>`:

```yaml
openapi: 3.0.3
info:
  title: user API
  version: 1.0.0
  description: Auto-generated from nevo-messaging contract (1)
paths:
  /user-events/user.getById:
    post:
      operationId: user.getById
      summary: "user.getById@v1"
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [uuid, method, params]
              properties:
                uuid:   { type: string }
                method: { type: string, const: "user.getById" }
                params:
                  type: object
                  required: [id]
                  properties:
                    id: { type: string, format: bigint }
      responses:
        "200":
          description: OK
          content:
            application/json:
              schema:
                type: object
                properties:
                  uuid:   { type: string }
                  method: { type: string }
                  params:
                    type: object
                    properties:
                      result: { type: object }
```

The path naming follows nevo's HTTP transport convention (`POST /<service>-events/<signal>`). For a public-facing REST API this is rarely the ideal shape — post-process the generated document and remap paths to your taste.

## AsyncAPI

```ts
const asyncDoc = contractToAsyncApi(contract)
// → {
//     asyncapi: "2.6.0",
//     info: { title, version },
//     channels: {
//       "user-events": {
//         publish: { operationId, message: { name, contentType, payload: { ... } } }
//       }
//     }
//   }
```

The current generator places **all** signals in a single channel named `<service>-events` (matching the NATS subject / Kafka topic convention). For richer per-signal channels, post-process the document.

## Customising the output

`contractToOpenApi` accepts a small options object:

```ts
contractToOpenApi(contract, {
  title: "User Service",
  version: "2.0.0",
  description: "Public-facing API for user management",
  baseUrl: "https://api.example.com"
})
```

For anything more substantial — security schemes, tags, route remapping, response examples — treat the function output as a starting document and merge your additions in code:

```ts
import { merge } from "lodash-es"
const base = contractToOpenApi(contract)
const enriched = merge(base, {
  components: {
    securitySchemes: { bearerAuth: { type: "http", scheme: "bearer" } }
  },
  security: [{ bearerAuth: [] }]
})
```

## Schema fidelity

| `paramsSchema.kind` | Output |
|---|---|
| `zod` | Full JSON Schema, with `type`/`properties`/`required`. `ZodBigInt` → `string` with `format: "bigint"`. |
| `class-validator` | `{ type: "object", title: "<className>" }` — class shape is not recovered. |
| `json-schema` | Pass-through. |
| `unknown` / null | `{ type: "object" }` |

To get useful OpenAPI, annotate your handlers with `@Schema(zodSchema)`. See [schema.md](./schema.md).

## What is intentionally not done

- **Pattern kind in OpenAPI**: every signal becomes a `POST`. There is no special treatment for `emit`/`publish`/`broadcast`. If the distinction matters for your docs, encode it in the signal name and post-process.
- **Auth requirements**: the generator does not introspect ACL rules or JWT verifiers. Add `security` and `securitySchemes` manually.
- **Pagination / streaming**: not currently represented in the spec.

## See also

- [contracts.md](./contracts.md) — how the underlying `ServiceContract` is built
- [schema.md](./schema.md) — annotating methods so OpenAPI has real types
- [src/common/openapi-gen.ts](../src/common/openapi-gen.ts) — the (small) source
