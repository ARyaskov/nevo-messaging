# Schema validation

`@Schema(...)` attaches a schema descriptor to a service method. The descriptor is used for runtime validation of inbound params and is also reflected in the [contract](./contracts.md).

## Real API

```ts
import { Schema, getSchemaFor, toValidator } from "@riaskov/nevo-messaging"

function Schema(schema: unknown): MethodDecorator
```

The argument can be:

- A **zod** schema (`z.object(...)`)
- A **class-validator** class
- Any object with `.parse(input)`, `.safeParse(input)`, or `.validate(input)`

## Zod

```ts
import { z } from "zod"
import { Schema } from "@riaskov/nevo-messaging"

const CreateUser = z.object({
  name: z.string().min(2),
  email: z.string().email()
})

@Injectable()
export class UserService extends NatsClientBase {
  @Schema(CreateUser)
  async create(input: z.infer<typeof CreateUser>) {
    return this.repo.save(input)
  }
}
```

The framework calls `.parse(envelope.params)` before invoking the handler. A failure throws `MessagingError(ErrorCode.VALIDATION_FAILED, { details: { issues } })`.

## class-validator

```ts
import { IsEmail, IsString, MinLength } from "class-validator"
import { Schema } from "@riaskov/nevo-messaging"

export class CreateUserDto {
  @IsString() @MinLength(2)
  name!: string

  @IsEmail()
  email!: string
}

@Schema(CreateUserDto)
async create(input: CreateUserDto) { ... }
```

The framework instantiates the DTO with `Object.assign(new CreateUserDto(), input)` and runs `validate()`.

## Custom validator

Any object with one of these methods is accepted:

```ts
const myValidator = {
  parse(input: unknown) {
    if (typeof input !== "object" || !input) throw new Error("expected object")
    return input
  }
}

@Schema(myValidator)
async submit(input: unknown) { ... }
```

`toValidator(schema)` is the helper the framework uses internally — exposed for symmetric custom dispatchers.

## Where it shows up

- **Inbound dispatch** — the framework validates every call before the handler runs.
- **Contract** — `nevo.contract` reports a `paramsSchema` descriptor (zod → portable tree, class-validator → just the class name, anything else → `kind: "unknown"`).
- **OpenAPI** — `contractToOpenApi` translates zod descriptors into JSON Schema. Class-validator gives only the title.

## What is NOT provided

- **No `@Schema({ input, output })` two-arg form.** Only the input is captured.
- **No router-level `schemas: { method: schema }` map.** Apply `@Schema` per method.
- **No fancy output validation** — see "output validation" below.

## Output validation (do-it-yourself)

If you want to enforce reply shape too, validate in code:

```ts
const ReplySchema = z.object({ id: z.bigint(), name: z.string() })

@Schema(GetByIdInput)
async getById(id: bigint) {
  const user = await this.repo.find(id)
  return ReplySchema.parse(user)   // throws if your DB drift broke the contract
}
```

This is more reliable than auto-validation because it's explicit and runs once per call.

## Error shape

```json
{
  "code": 7,
  "message": "Validation failed",
  "details": {
    "issues": [
      { "path": ["email"], "message": "Invalid email" }
    ]
  }
}
```

`details.issues` matches whatever the underlying validator returns (zod issues, class-validator constraints).

## See also

- [contracts.md](./contracts.md) — how `@Schema` flows into the auto-generated contract
- [openapi.md](./openapi.md) — zod schemas → JSON Schema
- [error-codes.md](./error-codes.md) — `VALIDATION_FAILED`
