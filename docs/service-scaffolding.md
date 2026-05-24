# Service scaffolding (`nevo-gen`)

`nevo-gen` is a one-shot CLI that produces a working NestJS microservice wired to your chosen transport.

## Run

```bash
npx nevo-gen <name> [flags]
```

If `@riaskov/nevo-messaging` is installed locally, `nevo-gen` is on the local `node_modules/.bin` path.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `-n`, `--name <name>` | (positional) | Service name (e.g. `user`, `order`) |
| `-t`, `--transport <kind>` | `nats` | One of `nats`, `kafka`, `http`, `socket` |
| `-o`, `--out <path>` | `./<name>` | Output directory |
| `-p`, `--port <number>` | `8086` | Listen port for bootstrap |
| `-f`, `--force` | off | Overwrite existing files |
| `-h`, `--help` | — | Print help |

## Output

For `nevo-gen user --transport nats`:

```
user/
├── src/
│   ├── main.ts                 # createNatsMicroservice bootstrap
│   ├── app.module.ts           # imports UserModule
│   └── user/
│       ├── user.module.ts      # ConfigModule + NEVO_NATS_CLIENT provider
│       ├── user.service.ts     # extends NatsClientBase
│       └── user.controller.ts  # @NatsSignalRouter with sample @Signal handlers
├── package.json                # dev/build/start scripts + deps
└── tsconfig.json               # TypeScript 6, ES2024, decorators on
```

The `user.module.ts` already includes the right factory (`createNevoNatsClient` / `createNevoKafkaClient` / etc.) and DI token.

## Try it

```bash
npx nevo-gen user --transport nats
cd user
npm install
npm run dev
```

In another shell:

```ts
const u = await client.query("user", "user.getById", { id: 1n })
// → { id: 1n, name: "Sample User" }
```

## Transport notes

### NATS

Targets `nats://127.0.0.1:4222`. Run a broker:

```bash
docker run --rm -p 4222:4222 nats:2 -js
```

### Kafka

Expects a broker on `localhost:9092`. Use the docker-compose snippet in [basics-kafka.md](./basics-kafka.md).

### HTTP

Binds to the given `--port`. Update `app.module.ts` to point at your real coordinator URL.

### Socket.IO

Starts a Socket.IO server on the given port plus a coordinator URL (replace with yours).

## Force overwrite

The CLI refuses to overwrite files by default:

```
Refusing to overwrite /path/to/user/src/user/user.service.ts (use --force)
```

Pass `--force` to replace, or delete the directory first.

## After generation

Recommended next steps:

1. Replace the sample methods with your real domain
2. Add `@Schema()` on every public method so the contract has types — see [schema.md](./schema.md)
3. Wire up a database in `user.module.ts`
4. Add a `HealthRegistry` — see [health-checks.md](./health-checks.md)
5. Call `setupNevoTracing` in `main.ts` — see [observability.md](./observability.md)

The companion CLI is `nevo-contract`, which fetches the running contract and generates a TypeScript client — see [contracts.md](./contracts.md).
