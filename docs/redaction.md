# PII redaction

`redactObject` strips known-sensitive fields before they reach pino, the DevTools bus, or any error path.

## Real API

```ts
import { redactObject } from "@riaskov/nevo-messaging"

const safe = redactObject(payload, ["customField"])
```

That is the entire public API. There is no global configuration object — pass `customKeys` per call site to extend the default list.

## Default keys

Case-insensitive substring match on the following (file-local constants in `src/common/redact.ts`):

- `password`, `passwd`
- `secret`
- `token`
- `authorization`
- `apikey`, `api_key`
- `accesskey`, `access_key`
- `privatekey`, `private_key`
- `cookie`, `set-cookie`
- `ssn`

Matched fields are replaced with the string `"[REDACTED]"`. The function walks objects and arrays recursively.

## Extending per call

```ts
// In a logger hook:
const safeEnvelope = redactObject(envelope, ["pinCode", "deviceFingerprint"])
logger.info({ envelope: safeEnvelope })
```

There is no global registry — every call site decides which extra keys it cares about.

## Where the framework runs redaction

Most internal log lines that include user payloads already go through `redactObject` with the framework's default keys. Where you log user data yourself, redact explicitly:

```ts
import { redactObject } from "@riaskov/nevo-messaging"
this.logger.debug({ input: redactObject(input) }, "processing")
```

## What is NOT provided

- **No `configureRedaction()` global.** The default key list is hardcoded.
- **No `keep()` opt-out helper.** Naming a field with one of the default keys will always redact it.
- **No regex / pattern matching.** Only substring-on-key matching. To redact by content (e.g. credit-card numbers in a string field), write your own pre-log transform.

## Where redaction does NOT happen

- The **wire payload** is not redacted — that's your business data
- **Database writes** are not redacted — at-rest protection is your responsibility
- **Logged objects you don't pass through `redactObject`** are untouched

## See also

- [logger.md](./logger.md) — pino is the default consumer
- [devtools.md](./devtools.md) — events shown in the dashboard are redacted on the way in
