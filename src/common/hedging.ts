export interface HedgingOptions {
  enabled?: boolean
  copies?: number
  delayMs?: number
}

export async function hedge<T>(fn: (attempt: number, signal: AbortSignal) => Promise<T>, opts: HedgingOptions): Promise<T> {
  const enabled = opts.enabled !== false
  const copies = Math.max(1, opts.copies ?? 2)
  const delayMs = Math.max(0, opts.delayMs ?? 50)

  if (!enabled || copies === 1) {
    const controller = new AbortController()
    return await fn(1, controller.signal)
  }

  const controllers: AbortController[] = []
  const promises: Promise<T>[] = []
  const { promise: outer, resolve, reject } = Promise.withResolvers<T>()
  let resolved = false
  let rejectedCount = 0

  const fireOne = (i: number) => {
    const ctrl = new AbortController()
    controllers.push(ctrl)
    const p = fn(i + 1, ctrl.signal)
    promises.push(p)
    p.then((v) => {
      if (resolved) return
      resolved = true
      for (const c of controllers) if (c !== ctrl) try { c.abort() } catch {}
      resolve(v)
    }).catch((err) => {
      rejectedCount++
      if (resolved) return
      if (rejectedCount >= copies) reject(err)
    })
  }

  fireOne(0)
  for (let i = 1; i < copies; i++) {
    setTimeout(() => { if (!resolved) fireOne(i) }, delayMs * i)
  }

  return outer
}
