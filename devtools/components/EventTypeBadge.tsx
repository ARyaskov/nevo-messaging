import type { DevToolsEvent } from "@riaskov/nevo-messaging"

const CLASS_BY_TYPE: Record<DevToolsEvent["type"], string> = {
  request:    "req",
  response:   "resp",
  error:      "evt-error",
  circuit:    "circuit",
  discovery:  "discovery",
  "rate-limit": "ratelim",
  custom:     "custom"
}

/**
 * Inline coloured badge for `DevToolsEvent["type"]`.
 *
 * Each event kind gets a distinct hue so the Recent traffic table
 * reads at a glance — request vs response is the most common pair
 * and the most useful to distinguish visually.
 */
export function EventTypeBadge({ type }: { type: DevToolsEvent["type"] }) {
  const cls = CLASS_BY_TYPE[type] ?? "custom"
  return <span className={`nv-badge ${cls}`}>{type}</span>
}
