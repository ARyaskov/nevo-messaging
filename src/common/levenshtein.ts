export function levenshteinDistance(a: string, b: string): number {
  if (a === b) return 0
  if (!a.length) return b.length
  if (!b.length) return a.length

  if (a.length > b.length) {
    ;[a, b] = [b, a]
  }

  let prev = new Array<number>(a.length + 1)
  let curr = new Array<number>(a.length + 1)

  for (let i = 0; i <= a.length; i++) prev[i] = i

  for (let j = 1; j <= b.length; j++) {
    curr[0] = j
    for (let i = 1; i <= a.length; i++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1
      curr[i] = Math.min(curr[i - 1] + 1, prev[i] + 1, prev[i - 1] + cost)
    }
    const tmp = prev
    prev = curr
    curr = tmp
  }

  return prev[a.length]
}

const MAX_SUGGEST_INPUT_CHARS = 256

/** The length pre-filter and input cap matter: this runs on caller-controlled names. */
export function suggestClosestMethod(method: string, candidates: string[]): string | null {
  if (!candidates.length || !method || method.length > MAX_SUGGEST_INPUT_CHARS) return null

  const normalized = method.toLowerCase()
  const threshold = Math.max(2, Math.floor(method.length * 0.4))
  let best: { name: string; score: number } | null = null

  for (const candidate of candidates) {
    if (Math.abs(candidate.length - normalized.length) > threshold) continue
    const score = levenshteinDistance(normalized, candidate.toLowerCase())
    if (score <= threshold && (!best || score < best.score)) {
      best = { name: candidate, score }
      if (score === 0) break
    }
  }

  return best?.name ?? null
}
