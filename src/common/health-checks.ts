import type { HealthCheckFn } from "./health"

export function pgPing(client: { query: (sql: string) => Promise<unknown> }, opts?: { sql?: string }): HealthCheckFn {
  const sql = opts?.sql ?? "SELECT 1"
  return async () => {
    try {
      await client.query(sql)
      return { status: "ok" }
    } catch (err: any) {
      return { status: "down", message: err?.message ?? "pg ping failed" }
    }
  }
}

export function redisPing(client: { ping: () => Promise<string> }): HealthCheckFn {
  return async () => {
    try {
      const r = await client.ping()
      if (typeof r === "string" && r.toUpperCase() === "PONG") return { status: "ok" }
      return { status: "down", message: `unexpected reply: ${r}` }
    } catch (err: any) {
      return { status: "down", message: err?.message ?? "redis ping failed" }
    }
  }
}

export function kafkaAdminPing(admin: { describeCluster: () => Promise<{ brokers: unknown[] }> }): HealthCheckFn {
  return async () => {
    try {
      const info = await admin.describeCluster()
      const brokers = Array.isArray(info?.brokers) ? info.brokers : []
      if (brokers.length > 0) return { status: "ok", message: `${brokers.length} broker(s)` }
      return { status: "down", message: "no brokers" }
    } catch (err: any) {
      return { status: "down", message: err?.message ?? "kafka ping failed" }
    }
  }
}

export function natsPing(nc: { request: (subj: string, data: Uint8Array, opts?: { timeout?: number }) => Promise<any>; isClosed?: () => boolean }): HealthCheckFn {
  return async () => {
    try {
      if (typeof nc.isClosed === "function" && nc.isClosed()) return { status: "down", message: "connection closed" }
      const out = await nc.request("$SYS.REQ.SERVER.PING", new Uint8Array(), { timeout: 1000 }).catch(() => null)
      if (out === null) return { status: "ok", message: "nats connected (no PING reply)" }
      return { status: "ok" }
    } catch (err: any) {
      return { status: "down", message: err?.message ?? "nats ping failed" }
    }
  }
}

export function httpPing(url: string, opts?: { timeoutMs?: number; expectedStatus?: number; fetchImpl?: typeof fetch }): HealthCheckFn {
  const timeoutMs = opts?.timeoutMs ?? 2000
  const expected = opts?.expectedStatus ?? 200
  const fetchImpl = opts?.fetchImpl ?? globalThis.fetch
  return async () => {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    try {
      const res = await fetchImpl(url, { signal: controller.signal })
      if (res.status === expected) return { status: "ok" }
      return { status: "down", message: `http ${res.status}` }
    } catch (err: any) {
      return { status: "down", message: err?.message ?? "http ping failed" }
    } finally {
      clearTimeout(timer)
    }
  }
}

export function memoryUsagePing(thresholdMb: number = 1024): HealthCheckFn {
  return async () => {
    const used = process.memoryUsage()
    const heapUsedMb = used.heapUsed / 1024 / 1024
    if (heapUsedMb > thresholdMb) {
      return { status: "down", message: `heap ${heapUsedMb.toFixed(0)}MB > ${thresholdMb}MB` }
    }
    return { status: "ok", message: `${heapUsedMb.toFixed(0)}MB` }
  }
}

export function eventLoopLagPing(thresholdMs: number = 100): HealthCheckFn {
  return async () => {
    const start = performance.now()
    await new Promise((r) => setImmediate(r))
    const lag = performance.now() - start
    if (lag > thresholdMs) return { status: "down", message: `lag ${lag.toFixed(0)}ms > ${thresholdMs}ms` }
    return { status: "ok", message: `${lag.toFixed(1)}ms` }
  }
}
