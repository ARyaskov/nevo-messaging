const normalizedCache = new Map<string, string>()
const MAX_NORMALIZED_CACHE = 4096

export function normalizeServiceName(s: string): string {
  if (!s) return s
  let cached = normalizedCache.get(s)
  if (cached) return cached
  cached = s.toLowerCase()
  if (normalizedCache.size >= MAX_NORMALIZED_CACHE) {
    const firstKey = normalizedCache.keys().next().value
    if (firstKey !== undefined) normalizedCache.delete(firstKey)
  }
  normalizedCache.set(s, cached)
  return cached
}
