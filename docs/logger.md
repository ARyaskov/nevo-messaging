# Logger

Nevo prefers [pino](https://github.com/pinojs/pino) when it is installed, and falls back to a small JSON console logger otherwise. The interface is the same in both cases.

## API

```ts
type NevoLogLevel = "trace" | "debug" | "info" | "warn" | "error" | "fatal"

interface NevoLogger {
  trace(obj: object | string, msg?: string): void
  debug(obj: object | string, msg?: string): void
  info(obj: object | string, msg?: string): void
  warn(obj: object | string, msg?: string): void
  error(obj: object | string, msg?: string): void
  fatal(obj: object | string, msg?: string): void
  child(bindings: Record<string, unknown>): NevoLogger
  setLevel?(level: NevoLogLevel): void
  isLevelEnabled?(level: NevoLogLevel): boolean
}

import {
  createLogger,
  getDefaultLogger,
  setDefaultLogger,
  type NevoLogger,
  type NevoLogLevel,
  type NevoLoggerOptions
} from "@riaskov/nevo-messaging"
```

## Defaults

- Level: `info` in production (`NODE_ENV=production` / `MODE=production`), `debug` otherwise
- Pretty-printing: on in dev when `pino-pretty` is installed; off in production
- Built-in redact paths: `*.auth.token`, `*.password`, `*.secret`, `meta.auth.token`

```ts
createLogger({
  name: "user-service",
  level: "info",
  pretty: false,
  base: { region: "eu-west" },
  redact: ["*.password", "*.creditCard"]
})
```

## Using the default logger

```ts
import { getDefaultLogger } from "@riaskov/nevo-messaging"

const log = getDefaultLogger().child({ component: "user-service" })
log.info({ userId: 1n }, "looking up user")
log.warn({ retry: 2 }, "transient failure, retrying")
```

The framework's own log lines flow through `getDefaultLogger()`.

## Swap for your own logger

```ts
import { setDefaultLogger, type NevoLogger } from "@riaskov/nevo-messaging"

class WinstonAdapter implements NevoLogger {
  trace(o, m) { winston.silly(m ?? "", o) }
  debug(o, m) { winston.debug(m ?? "", o) }
  info(o, m) { winston.info(m ?? "", o) }
  warn(o, m) { winston.warn(m ?? "", o) }
  error(o, m) { winston.error(m ?? "", o) }
  fatal(o, m) { winston.error(m ?? "", o) }
  child(b) { return this }   // or build a child adapter
}

setDefaultLogger(new WinstonAdapter())
```

## Levels and gating

```ts
if (log.isLevelEnabled?.("debug")) {
  log.debug({ envelope: redactObject(envelope) }, "dispatching")
}
```

`isLevelEnabled` is optional in the interface — guard accordingly.

## Pino redaction

When pino is the implementation, the `redact` paths in options are passed to pino's redaction. Use the same format pino accepts (`"*.password"`, `"meta.auth.token"`, etc.). The default list is conservative; extend it for your domain.

For one-off redactions in application code, use `redactObject(value, customKeys?)` — see [redaction.md](./redaction.md).

## Structured fields the framework adds

Internal log lines tagged by the framework include:

- `name` — logger name
- `level` — text level
- `time` — unix ms
- Whatever was in `base` / `child` bindings

The framework does not auto-inject `service` / `transport` / `uuid` / `traceId` — pick those up by passing them in your own `child(...)` bindings:

```ts
const log = getDefaultLogger().child({
  service: "user",
  transport: "nats",
  instance: process.env.HOSTNAME
})
```

## Pino install

Optional:

```bash
npm install pino
npm install pino-pretty   # dev only, for human-readable output
```

Without pino, the framework uses a 50-line `ConsoleLogger` that emits one JSON line per call to `console.{log,warn,error}`. It is fine for tests and small deployments.
