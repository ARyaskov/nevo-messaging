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
    if (this.shuttingDown) return task
    const id = Symbol()
    this.inflight.add(id)
    return task.finally(() => {
      this.inflight.delete(id)
      if (this.shuttingDown && this.inflight.size === 0 && this.resolveDrain) {
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
