// LRU cache (delete-then-set on hit moves the key to the newest end).
const normalizedCache = new Map<string, string>()
const MAX_NORMALIZED_CACHE = 4096

export function normalizeServiceName(s: string): string {
  if (!s) return s
  const cached = normalizedCache.get(s)
  if (cached !== undefined) {
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
