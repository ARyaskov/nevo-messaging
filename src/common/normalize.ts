// True LRU keyed on insertion order: a Map iterates in insertion order, so
// re-inserting on every hit (delete-then-set) keeps hot keys at the "newest"
// end and evicts the genuinely least-recently-used entry. The previous version
// never refreshed recency on get(), making it FIFO — hot service names could be
// evicted, forcing a repeated toLowerCase on the hot path.
const normalizedCache = new Map<string, string>()
const MAX_NORMALIZED_CACHE = 4096

export function normalizeServiceName(s: string): string {
  if (!s) return s
  const cached = normalizedCache.get(s)
  if (cached !== undefined) {
    // Refresh recency: move this key to the newest end.
    normalizedCache.delete(s)
    normalizedCache.set(s, cached)
    return cached
  }
  const normalized = s.toLowerCase()
  if (normalizedCache.size >= MAX_NORMALIZED_CACHE) {
    const oldestKey = normalizedCache.keys().next().value
    if (oldestKey !== undefined) normalizedCache.delete(oldestKey)
  }
  normalizedCache.set(s, normalized)
  return normalized
}
