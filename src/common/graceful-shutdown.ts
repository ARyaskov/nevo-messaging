export type ShutdownHook = () => Promise<void> | void

export class GracefulShutdown {
  private readonly hooks: { name: string; fn: ShutdownHook }[] = []
  private readonly inflight = new Set<symbol>()
  private shuttingDown = false
  private resolveDrain?: () => void

  isShuttingDown(): boolean { return this.shuttingDown }

  register(name: string, fn: ShutdownHook): void {
    this.hooks.push({ name, fn })
  }

  trackInflight<T>(task: Promise<T>): Promise<T> {
    // Track even tasks started during the drain window: returning early here
    // would leave them untracked, so drain/timeout could tear them down
    // mid-flight. Once tracked, completing the last one still resolves drain.
    const id = Symbol()
    this.inflight.add(id)
    return task.finally(() => {
      this.inflight.delete(id)
      // Gate on an active drain (resolveDrain set), not on shuttingDown: drain()
      // is public and may be awaited on its own without shutdown() having flipped
      // shuttingDown. A present resolver means some drain() is waiting, so the
      // last inflight task completing should resolve it via completion rather
      // than forcing it to wait out the timeout.
      if (this.inflight.size === 0 && this.resolveDrain) {
        this.resolveDrain()
      }
    })
  }

  async drain(timeoutMs = 30_000): Promise<void> {
    if (this.inflight.size === 0) return
    const { promise, resolve } = Promise.withResolvers<void>()
    this.resolveDrain = resolve
    const timer = setTimeout(resolve, timeoutMs)
    try {
      await promise
    } finally {
      clearTimeout(timer)
      // Drop the settled resolver so a later drain() never reuses it.
      this.resolveDrain = undefined
    }
    if (this.inflight.size > 0) {
      console.warn(
        `[GracefulShutdown] drain timed out after ${timeoutMs}ms with ${this.inflight.size} inflight task(s) remaining`
      )
    }
  }

  async shutdown(timeoutMs = 30_000): Promise<void> {
    if (this.shuttingDown) return
    this.shuttingDown = true
    await this.drain(timeoutMs)
    for (let i = this.hooks.length - 1; i >= 0; i--) {
      try {
        await this.hooks[i].fn()
      } catch (err) {
        console.error(`[GracefulShutdown] hook ${this.hooks[i].name} failed`, err)
      }
    }
  }
}
