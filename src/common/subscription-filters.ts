import type { SubscriptionFilter, MessageMeta } from "./types"

export function matchesFilter(filter: SubscriptionFilter | undefined, meta?: MessageMeta): boolean {
  if (!filter) return true

  if (filter.headers && meta?.headers) {
    for (const [k, expected] of Object.entries(filter.headers)) {
      const actual = meta.headers[k]
      if (actual === undefined) return false
      if (expected instanceof RegExp) {
        if (!expected.test(actual)) return false
      } else if (expected !== actual) {
        return false
      }
    }
  } else if (filter.headers && !meta?.headers) {
    return false
  }

  if (filter.meta && meta) {
    for (const [k, expected] of Object.entries(filter.meta)) {
      const actual = (meta as any)[k]
      const str = actual == null ? "" : String(actual)
      if (expected instanceof RegExp) {
        if (!expected.test(str)) return false
      } else if (expected !== str) {
        return false
      }
    }
  } else if (filter.meta && !meta) {
    return false
  }

  return true
}
