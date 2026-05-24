# Graceful shutdown

`GracefulShutdown` is a small class that tracks in-flight work and runs registered hooks in order on shutdown.

## Real API

```ts
class GracefulShutdown {
  isShuttingDown(): boolean
  register(name: string, fn: () => Promise<void> | void): void
  trackInflight<T>(task: Promise<T>): Promise<T>
  drain(timeoutMs?: number): Promise<void>          // wait for in-flight to finish
  shutdown(timeoutMs?: number): Promise<void>       // drain + run hooks
}
```

Both `drain` and `shutdown` accept a per-call timeout (default `30_000`). There is no static config object — the timeout is passed at call time.

## Wiring

```ts
import { GracefulShutdown } from "@riaskov/nevo-messaging"

const shutdown = new GracefulShutdown()

// Register hooks (run in registration order during shutdown())
shutdown.register("stop subscriptions", async () => { await sub.unsubscribe() })
shutdown.register("close nats client",  async () => { await client.close() })
shutdown.register("flush outbox",       async () => { await outbox.flushOnce() })
shutdown.register("close db",           async () => { await pool.end() })

// Track each handler invocation so drain() waits for them
async function onMessage(msg) {
  await shutdown.trackInflight(handleMessage(msg))
}

// Hook SIGTERM
for (const sig of ["SIGTERM", "SIGINT"] as const) {
  process.on(sig, async () => {
    if (shutdown.isShuttingDown()) return
    await shutdown.shutdown(30_000)
    process.exit(0)
  })
}
```

## What `shutdown()` does

1. Marks the shutdown state (so `isShuttingDown()` returns `true`)
2. `drain(timeoutMs)` — waits for tracked in-flight tasks to complete; rejects if the timeout elapses
3. Runs each registered hook in the order they were registered

If a hook throws, the others still run. The thrown error is logged and `shutdown()` resolves normally (so the process can exit cleanly).

## Drain timeout

The default 30 s suits container orchestrators with `terminationGracePeriodSeconds: 45`. If your handlers can take longer, pass a higher value:

```ts
await shutdown.shutdown(60_000)
```

If the timeout elapses, the in-flight tasks continue to run but `drain` resolves so subsequent hooks can fire.

## Liveness vs. readiness during shutdown

Pair with the [health registry](./health-checks.md):

- Liveness — leave it `ok`. The process is still running.
- Readiness — return `down` once `shutdown.isShuttingDown()` returns `true`, so the orchestrator stops sending new traffic.

```ts
healthRegistry.register("not-draining", () => ({
  status: shutdown.isShuttingDown() ? "down" : "ok",
  message: shutdown.isShuttingDown() ? "draining" : "ready"
}), { kind: "readiness" })
```

## What is NOT provided

- **No global config option** on `create*Microservice` for graceful shutdown — instantiate the class yourself and hook signals.
- **No `preStop` lifecycle slot.** Use the hook order: register your `preStop` work first so it runs before transport teardown.
- **No `forceExitMs` killer.** If hooks hang past your timeout, you must call `process.exit(1)` from a separate watchdog.

## Recipe with a watchdog

```ts
process.on("SIGTERM", async () => {
  const killer = setTimeout(() => process.exit(1), 60_000)
  killer.unref()

  await shutdown.shutdown(45_000)
  clearTimeout(killer)
  process.exit(0)
})
```

The `killer.unref()` makes sure the watchdog doesn't keep the loop alive — `process.exit()` runs whichever path completes first.

## See also

- [health-checks.md](./health-checks.md) — readiness during shutdown
- [outbox.md](./outbox.md) — `outbox.stop()` is a good hook
- [environment.md](./environment.md) — `NODE_ENV` etc.
