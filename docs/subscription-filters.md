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

```ts
import { matchesFilter } from "@riaskov/nevo-messaging"

const ok = matchesFilter(
  { headers: { region: /^eu-/ }, meta: { tier: "paid" } },
  { headers: { region: "eu-west" }, meta: { tier: "paid", uid: "42" } }
)
```

## Header filters

Match on transport headers (NATS headers, Kafka record headers, HTTP request headers):

```ts
await this.subscribe(
  "orders", "orders.placed",
  { filters: { headers: { region: "eu-west" } } },
  handler
)
```

## Meta filters

Match on envelope `meta` (in-band, codec-encoded):

```ts
await this.subscribe(
  "orders", "orders.placed",
  { filters: { meta: { tenantId: "t-42" } } },
  handler
)
```

## Combined

Both filters are AND-ed: every predicate must pass.

```ts
filters: {
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
    { filters: { meta: { tenantId: t.id } } },
    t.handler
  )
}
```

### Regional fan-out

```ts
await this.subscribe(
  "alerts", "alert.*",
  { filters: { headers: { region: /^eu-/ } } },
  handleEuAlert
)
```

### Priority queue (regex on numeric strings)

```ts
await this.subscribe(
  "jobs", "job.run",
  { filters: { meta: { priority: /^(8|9|10)$/ } } },
  handleHighPriority
)
```

Numbers in `meta` are encoded as strings on the wire; the regex matches the string form.
