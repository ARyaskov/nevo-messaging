# Subscription filters

Subscriptions can filter messages on the consumer side using two predicate kinds.

## Real shape

```ts
interface SubscriptionFilter {
  headers?: Record<string, string | RegExp>
  meta?:    Record<string, string | RegExp>
}
```

Predicates are either an exact string (matched literally) or a `RegExp`. No function predicates today.

Both kinds are evaluated against the envelope's `MessageMeta`, which travels inside the codec-encoded payload:

```ts
import { matchesFilter } from "@riaskov/nevo-messaging"

// second argument is the message's MessageMeta
const ok = matchesFilter(
  { headers: { region: /^eu-/ }, meta: { tenantId: "t-42" } },
  { headers: { region: "eu-west" }, tenantId: "t-42", service: "orders" }
) // true
```

## Header filters

Match on `meta.headers` — the string map the publisher passes as `headers` (`publish(service, method, params, { headers: { region: "eu-west" } })`, `broadcast(method, params, { headers })`). Transport-level headers are **not** consulted: NATS headers, Kafka record headers and HTTP request headers added by a proxy, broker or non-Nevo producer never reach the filter. A message without `meta.headers` fails every header filter.

```ts
await this.subscribe(
  "orders", "orders.placed",
  { filter: { headers: { region: "eu-west" } } },
  handler
)
```

## Meta filters

Match on top-level `MessageMeta` fields (`tenantId`, `service`, `version`, `instanceId`, …). Values are compared as strings:

```ts
await this.subscribe(
  "orders", "orders.placed",
  { filter: { meta: { tenantId: "t-42" } } },
  handler
)
```

## Combined

Both filters are AND-ed: every predicate must pass.

```ts
filter: {
  headers: { region: /^eu-/ },
  meta:    { tenantId: "t-42" }
}
```

## Where filters run

Filters always run in-process before the handler is invoked. The framework does not push predicates down to NATS JetStream subject filters or Kafka consumer-side filters — this would couple the framework to transport quirks. If you need a JetStream subject filter for bandwidth reasons, subscribe by subject pattern directly (see [basics-nats.md](./basics-nats.md)).

## Recipes

### Per-tenant subscriber

```ts
for (const t of tenants) {
  await this.subscribe(
    "orders", "orders.placed",
    { filter: { meta: { tenantId: t.id } } },
    t.handler
  )
}
```

### Regional fan-out

```ts
await this.subscribe(
  "alerts", "alert.*",
  { filter: { headers: { region: /^eu-/ } } },
  handleEuAlert
)
```

### Priority queue (regex on numeric strings)

```ts
await this.subscribe(
  "jobs", "job.run",
  { filter: { headers: { priority: /^(8|9|10)$/ } } },
  handleHighPriority
)
```

`priority` is not a `MessageMeta` field, so the publisher sends it as a header: `publish("jobs", "job.run", job, { headers: { priority: "9" } })`. Header values are strings; the regex matches the string form.
