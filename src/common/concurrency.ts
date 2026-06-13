// Bounded-concurrency map: at most `limit` of `fn` in flight, preserving order and Promise.all error semantics.
export async function mapLimit<T, R>(items: readonly T[], limit: number, fn: (item: T, index: number) => Promise<R>): Promise<R[]> {
  const results: R[] = new Array(items.length)
  if (items.length === 0) return results
  const workers = Math.max(1, Math.min(limit, items.length))
  let cursor = 0
  const runWorker = async (): Promise<void> => {
    while (cursor < items.length) {
      const index = cursor++
      results[index] = await fn(items[index], index)
    }
  }
  await Promise.all(Array.from({ length: workers }, () => runWorker()))
  return results
}
