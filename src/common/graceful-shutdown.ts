export type ShutdownHook = () => Promise<void> | void

export class GracefulShutdown {
  private readonly hooks: { name: string; fn: ShutdownHook }[] = []
  private readonly inflight = new Set<symbol>()
  private shuttingDown = false
  private resolveDrain?: () => void

  isShuttingDown(): boolean {
    return this.shuttingDown
  }

  register(name: string, fn: ShutdownHook): void {
    this.hooks.push({ name, fn })
  }

  /**
   * Track in-flight work so a drain can await it. Accepts an already-started
   * promise (tracked as-is) or a thunk, which is preferred: the task only
   * starts if it can also be tracked, so a spawn racing shutdown never
   * initiates a dispatch that the drain then misses.
   */
  trackInflight<T>(task: Promise<T> | (() => Promise<T>)): Promise<T> {
    if (typeof task === "function") {
      // Once shutdown started, no new work may be started: it would escape the drain.
      if (this.shuttingDown) throw new Error("[GracefulShutdown] cannot track in-flight work during shutdown")
      return this.trackInflight(task())
    }
    const id = Symbol()
    this.inflight.add(id)
    return task.finally(() => {
      this.inflight.delete(id)
      // Gate on an active drain (resolveDrain set), not on shuttingDown.
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
      console.warn(`[GracefulShutdown] drain timed out after ${timeoutMs}ms with ${this.inflight.size} inflight task(s) remaining`)
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
