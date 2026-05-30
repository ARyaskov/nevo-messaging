import { Worker } from "node:worker_threads"
import * as path from "node:path"
import * as os from "node:os"
import { getDefaultLogger, NevoLogger } from "./logger"

const WORKER_INLINE_SOURCE = `
const { parentPort } = require("node:worker_threads")
const { gzipSync, gunzipSync, deflateSync, inflateSync } = require("node:zlib")
const zlib = require("node:zlib")

parentPort.on("message", (msg) => {
  // Destructure outside the try so 'id' is in scope for the catch — a decompress
  // that overflows maxOutputLength throws here, and the reply must carry the id.
  const { id, op, data, encoding, level, maxOutputBytes } = msg
  try {
    let result
    if (op === "compress") {
      if (encoding === "gzip") result = gzipSync(data, { level })
      else if (encoding === "deflate") result = deflateSync(data, { level })
      else if (encoding === "zstd" && typeof zlib.zstdCompressSync === "function") result = zlib.zstdCompressSync(data)
      else result = data
    } else {
      const limit = maxOutputBytes != null ? { maxOutputLength: maxOutputBytes } : undefined
      if (encoding === "gzip") result = gunzipSync(data, limit)
      else if (encoding === "deflate") result = inflateSync(data, limit)
      else if (encoding === "zstd" && typeof zlib.zstdDecompressSync === "function") result = zlib.zstdDecompressSync(data, limit)
      else result = data
    }
    parentPort.postMessage({ id, ok: true, data: result }, [result.buffer])
  } catch (err) {
    parentPort.postMessage({ id, ok: false, err: err && err.message ? err.message : String(err), code: err && err.code ? err.code : undefined })
  }
})
`

interface PendingJob {
  resolve: (data: Uint8Array) => void
  reject: (err: Error) => void
}

interface PooledWorker {
  worker: Worker
  busy: boolean
}

let pool: PooledWorker[] | null = null
const pendingByWorker = new Map<Worker, Map<number, PendingJob>>()
let jobCounter = 0

function getPool(size: number): PooledWorker[] {
  if (pool) return pool
  pool = []
  for (let i = 0; i < size; i++) {
    const w = new Worker(WORKER_INLINE_SOURCE, { eval: true })
    pool.push({ worker: w, busy: false })
    const pending = new Map<number, PendingJob>()
    pendingByWorker.set(w, pending)
    w.on("message", (msg: any) => {
      const job = pending.get(msg.id)
      if (!job) return
      pending.delete(msg.id)
      if (msg.ok) job.resolve(new Uint8Array(msg.data))
      else {
        const err = new Error(msg.err) as Error & { code?: string }
        if (msg.code) err.code = msg.code
        job.reject(err)
      }
    })
    w.unref()
  }
  return pool
}

function pickWorker(): PooledWorker | null {
  if (!pool) return null
  for (const w of pool) if (!w.busy) return w
  return pool[Math.floor(Math.random() * pool.length)]
}

export interface CompressionWorkerOptions {
  enabled?: boolean
  poolSize?: number
  threshold?: number
  logger?: NevoLogger
}

let logger: NevoLogger | null = null
let cfg: CompressionWorkerOptions = {}

export function configureCompressionWorker(opts: CompressionWorkerOptions): void {
  cfg = opts
  logger = opts.logger ?? getDefaultLogger().child({ component: "compression-worker" })
  if (opts.enabled) getPool(opts.poolSize ?? Math.max(1, Math.min(4, os.cpus().length - 1)))
}

export function isCompressionWorkerEnabled(): boolean {
  return cfg.enabled === true && !!pool
}

export function compressionWorkerThreshold(): number {
  return cfg.threshold ?? 64 * 1024
}

export async function workerCompress(data: Uint8Array, encoding: "gzip" | "deflate" | "zstd", level?: number): Promise<Uint8Array> {
  const pooled = pickWorker()
  if (!pooled) throw new Error("Compression worker pool not initialized; call configureCompressionWorker first")
  const id = ++jobCounter
  const pending = pendingByWorker.get(pooled.worker)!
  const { promise, resolve, reject } = Promise.withResolvers<Uint8Array>()
  pending.set(id, { resolve, reject })
  pooled.busy = true
  try {
    pooled.worker.postMessage({ id, op: "compress", data, encoding, level }, [data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer])
  } catch (err) {
    pending.delete(id)
    pooled.busy = false
    throw err
  }
  // Return the chained promise so the caller's await/catch also covers the busy
  // reset — a bare `promise.finally(...)` would orphan its rejection and surface
  // as an unhandledRejection when a job rejects (e.g. a decompression-bomb cap).
  return promise.finally(() => { pooled.busy = false })
}

export async function workerDecompress(data: Uint8Array, encoding: "gzip" | "deflate" | "zstd", maxOutputBytes?: number): Promise<Uint8Array> {
  const pooled = pickWorker()
  if (!pooled) throw new Error("Compression worker pool not initialized; call configureCompressionWorker first")
  const id = ++jobCounter
  const pending = pendingByWorker.get(pooled.worker)!
  const { promise, resolve, reject } = Promise.withResolvers<Uint8Array>()
  pending.set(id, { resolve, reject })
  pooled.busy = true
  try {
    pooled.worker.postMessage({ id, op: "decompress", data, encoding, maxOutputBytes }, [data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer])
  } catch (err) {
    pending.delete(id)
    pooled.busy = false
    throw err
  }
  return promise.finally(() => { pooled.busy = false })
}

export async function shutdownCompressionWorker(): Promise<void> {
  if (!pool) return
  for (const w of pool) {
    try { await w.worker.terminate() } catch {}
  }
  pool = null
  pendingByWorker.clear()
}
