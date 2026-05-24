# Access control (ACL)

ACL restricts which callers can invoke which methods on which topics. It is evaluated by the signal router before the handler runs.

## Configuration shape

```ts
interface AccessControlConfig {
  rules?: AccessRule[]
  allowAllByDefault?: boolean        // default: true (when no rules match)
  logDenied?: boolean
  jwtVerifier?: (token: string) => Promise<VerifiedClaims | null>
}

interface AccessRule {
  topic?: string                     // exact topic or "*"
  method?: string                    // exact method or "*"
  allow?: string[]                   // caller services / identities
  deny?: string[]                    // caller services / identities
}
```

## Apply to a controller

```ts
@NatsSignalRouter([UserService], {
  accessControl: {
    rules: [
      { topic: "user-events", method: "*",          allow: ["frontend", "coordinator"] },
      { topic: "user-events", method: "user.delete", deny:  ["frontend"] }
    ],
    logDenied: true
  }
})
export class UserController { ... }
```

Semantics:

- If `rules` is empty, `allowAllByDefault` (default `true`) decides
- Multiple rules can match a given `(topic, method)` — `deny` always wins over `allow`
- Wildcards `"*"` match any value

## Caller identity

The caller is identified by `meta.callerService` on the envelope. Each Nevo client sets it to its `clientIdPrefix` by default.

With `jwtVerifier`, the framework also verifies the bearer token from envelope `meta.auth.token` and uses the `sub` claim (or full `VerifiedClaims`) as an additional identity.

## Programmatic API

```ts
import {
  isAccessAllowed,
  extractCallerService,
  createAccessDeniedError,
  logAccessDenied
} from "@riaskov/nevo-messaging"

const caller = await extractCallerService(envelope.meta, jwtVerifier)
if (!isAccessAllowed(config, topic, method, caller)) {
  logAccessDenied(config, { topic, method, caller })
  throw createAccessDeniedError(method, "user", caller)
}
```

This is what the signal router does internally — exposed for custom dispatchers.

## What ACL produces on a denial

A denied call rejects with `ErrorCode.UNAUTHORIZED` (no separate `FORBIDDEN` code) and an error message like `"Access denied for service 'frontend' to method 'user.delete'"`. With `logDenied: true` the framework also logs at warn level.

## What is NOT provided

- **No `AclService` class.** ACL is data + a few pure functions; there is no NestJS provider to inject.
- **No regex matching on `topic`/`method`** — exact strings or `"*"` only.
- **No "compiled buckets" tuning knob.** The lookup is already linear in number of rules; rule lists are expected to be small.

## Recipes

### Frontend allowlist

```ts
accessControl: {
  rules: [
    { topic: "*", method: "user.getById",       allow: ["frontend"] },
    { topic: "*", method: "user.searchPublic",  allow: ["frontend"] }
    // everything else: implicitly denied because allowAllByDefault is set false below
  ],
  allowAllByDefault: false
}
```

### Internal-only methods

```ts
{ topic: "*", method: "user.purge", allow: ["admin"], deny: ["frontend", "*"] }
```

The `deny: ["*"]` makes the rule explicit; in practice the `allow` list alone is enough because non-listed callers are denied (when `allowAllByDefault: false`).

## See also

- [security.md](./security.md) — JWT/JWKS verification
- [error-codes.md](./error-codes.md) — `UNAUTHORIZED`
- [discovery.md](./discovery.md) — caller services come from this
