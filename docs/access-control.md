# Access control (ACL)

ACL restricts which callers can invoke which methods on which topics. It is evaluated by the signal router before the handler runs.

## Configuration shape

```ts
interface AccessControlConfig {
  rules?: AccessRule[]
  // Default: true with no rules, FALSE once any rule exists (secure default).
  // Set explicitly to opt out.
  allowAllByDefault?: boolean
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

- **Deny by default once rules exist.** When at least one rule is configured and `allowAllByDefault` is not set, any `(topic, method)` matched by no rule is **denied**. Set `allowAllByDefault: true` to restore the permissive behaviour. With no rules at all, everything is allowed.
- Built-in `nevo.*` methods (contract, health probes) stay reachable under the deny-default; an explicit matching `deny` rule still blocks them.
- Multiple rules can match a given `(topic, method)` — `deny` always wins over `allow`
- Wildcards `"*"` match any value

## Caller identity

How the caller identity is derived depends on whether a `jwtVerifier` is configured:

- **`jwtVerifier` configured (authenticated mode).** Identity comes **only** from the cryptographically verified bearer token in `meta.auth.token` — the `service` / `serviceName` / `svc` / `sub` claim, in that order. The client-supplied `meta.service` is **never** trusted as identity (trusting it would let any caller impersonate any service by stamping `meta.service`). A request with no token, or with a token that fails verification, is treated as anonymous (`undefined`). If a caller stamps a `meta.service` that disagrees with the verified identity, the request is rejected with `UNAUTHORIZED`.
- **No `jwtVerifier` (trusted-network mode).** Identity is taken from `meta.service` on the envelope; each Nevo client sets it to its `clientIdPrefix` by default. An unsigned token in `meta.auth.token` is **ignored** for identity — without a verifier its `alg:none` payload cannot be trusted — and the framework logs a one-time warning that token-based identity requires a verifier.

> Security: never run ACL with `meta.service`-based identity on an untrusted network. Configure a `jwtVerifier` (see [security.md](./security.md)) so identity is bound to a verified token.

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
