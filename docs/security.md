# Security: JWT/JWKS, mTLS, ACL

Nevo can verify caller identity via three mechanisms.

## JWKS-verified JWT

```ts
import { createJwksVerifier } from "@riaskov/nevo-messaging"

const verifier = createJwksVerifier({
  jwksUri: "https://auth.example.com/.well-known/jwks.json",
  issuer: "https://auth.example.com",
  audience: "user-service",
  cacheTtlMs: 600_000,
  clockSkewSec: 30
})

@NatsSignalRouter([UserService], {
  accessControl: {
    rules: [
      { topic: "user-events", method: "*", allow: ["frontend"] }
    ],
    jwtVerifier: verifier,
    logDenied: true
  }
})
export class UserController { ... }
```

The verifier:

- Pulls the JWKS document from `jwksUri` and caches keys by `kid`
- Refreshes the JWKS on cache expiry (`cacheTtlMs`) and on `kid` miss
- Accepts `RS256`, `RS384`, `RS512`, `ES256`, `ES384` (whichever the JWKS advertises)
- Returns a `VerifiedClaims` object (with `iss`, `sub`, `aud`, `exp`, …) or `null` on failure

```ts
const claims = await verifier(token)
if (!claims) throw new Error("invalid token")
```

`fetchImpl` lets you swap `fetch` (e.g. for mocking in tests).

## Where the token comes from

For each transport the framework extracts the token from a standard location:

| Transport | Source |
|---|---|
| HTTP | `Authorization: Bearer <token>` |
| WebSocket | `Sec-WebSocket-Protocol` |
| NATS / Kafka | `meta.auth.token` on the envelope |

For NATS and Kafka the framework expects the caller to put the token into `meta`. The framework does not modify your code's `query()` calls — pass the token explicitly when calling sensitive methods.

## ACL with JWT identity

When a JWT is verified, `claims.sub` is used as the caller identity for ACL evaluation. Override with `extractCallerService` if your identity scheme is different — see [access-control.md](./access-control.md).

## What is NOT provided

- **No HS256 (shared-secret) verifier helper.** Only JWKS-based verification ships. If you need HS256, wrap `jose.jwtVerify` yourself and pass it as the `jwtVerifier` function — the verifier is just a `(token) => Promise<VerifiedClaims | null>` function.
- **No claims-based decorator** (e.g. `@RequireRole("admin")`). Encode authorization rules via ACL or in your handler.

## mTLS

mTLS is configured at the transport layer, not in Nevo itself:

- **HTTP**: pass standard `tls.SecureContextOptions` (`cert`, `key`, `ca`) when constructing the server (`@nestjs/platform-fastify` `httpsOptions`)
- **NATS**: pass `tls: { ca, cert, key }` to the underlying NATS client options
- **Kafka**: pass `ssl: { ca, cert, key }` to `kafkajs`

The framework forwards these options through; check the transport's docs for exact shapes.

For end-to-end identity propagation across services, an mTLS client certificate's subject DN is available in the HTTP request and can be read in a `before` hook:

```ts
@HttpSignalRouter([UserService], {
  before: async (ctx) => {
    const dn = ctx.rawData?.socket?.getPeerCertificate?.()?.subject?.CN
    if (dn) ctx.params.callerDn = dn
    return ctx.params
  }
})
```

## Token propagation across services

To forward an end-user JWT through a chain of service calls, copy it into outbound `meta`:

```ts
@NatsSignalRouter([UserService], {
  before: async (ctx) => {
    const token = ctx.rawData?.headers?.authorization
    if (token) ctx.params.__auth = token  // domain agreement
    return ctx.params
  }
})
```

Or — simpler — make the service inject `NevoClient` and pass `meta.auth.token` on outbound calls explicitly. The framework does not auto-propagate JWTs by default.

## Anti-replay

Pair JWT verification with [replay-protection.md](./replay-protection.md) on sensitive endpoints. A captured token cannot be replayed twice within the window.

## See also

- [access-control.md](./access-control.md) — actor-based ACL
- [redaction.md](./redaction.md) — keep tokens out of logs
- [error-codes.md](./error-codes.md) — `UNAUTHORIZED` and friends
