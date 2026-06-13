import { Worker } from "node:worker_threads"
import * as os from "node:os"
import { NevoLogger } from "./logger"

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
}

let pool: PooledWorker[] | null = null
const pendingByWorker = new Map<Worker, Map<number, PendingJob>>()
let jobCounter = 0

function getPool(size: number): PooledWorker[] {
  if (!pool) pool = []
  while (pool.length < size) {
    const w = new Worker(WORKER_INLINE_SOURCE, { eval: true })
    pool.push({ worker: w })
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
    const rejectPending = (err: Error) => {
      const jobs = pendingByWorker.get(w)
      if (jobs) {
        for (const job of jobs.values()) job.reject(err)
        jobs.clear()
      }
      pendingByWorker.delete(w)
      if (pool) pool = pool.filter((entry) => entry.worker !== w)
      cfg.logger?.warn({ event: "compression.worker.failed", err: err.message }, "Compression worker exited with pending jobs")
    }
    w.on("error", (err) => rejectPending(err instanceof Error ? err : new Error(String(err))))
    w.on("exit", (code) => {
      if (pendingByWorker.has(w)) {
        rejectPending(new Error(`Compression worker exited with code ${code}`))
      }
    })
    w.unref()
  }
  return pool
}

function pickWorker(): PooledWorker | null {
  if ((!pool || pool.length === 0) && cfg.enabled) {
    getPool(cfg.poolSize ?? Math.max(1, Math.min(4, os.cpus().length - 1)))
  }
  if (!pool || pool.length === 0) return null
  let selected = pool[0]
  let selectedJobs = pendingByWorker.get(selected.worker)?.size ?? 0
  for (let i = 1; i < pool.length; i++) {
    const count = pendingByWorker.get(pool[i].worker)?.size ?? 0
    if (count < selectedJobs) {
      selected = pool[i]
      selectedJobs = count
    }
  }
  return selected
}

export interface CompressionWorkerOptions {
  enabled?: boolean
  poolSize?: number
  threshold?: number
  logger?: NevoLogger
}

let cfg: CompressionWorkerOptions = {}

export function configureCompressionWorker(opts: CompressionWorkerOptions): void {
  cfg = opts
  if (opts.enabled) getPool(opts.poolSize ?? Math.max(1, Math.min(4, os.cpus().length - 1)))
}

export function isCompressionWorkerEnabled(): boolean {
  return cfg.enabled === true && !!pool
}

export function compressionWorkerThreshold(): number {
  return cfg.threshold ?? 64 * 1024
}

function transferableCopy(data: Uint8Array): Uint8Array {
  const copy = new Uint8Array(data.byteLength)
  copy.set(data)
  return copy
}

export async function workerCompress(data: Uint8Array, encoding: "gzip" | "deflate" | "zstd", level?: number): Promise<Uint8Array> {
  const pooled = pickWorker()
  if (!pooled) throw new Error("Compression worker pool not initialized; call configureCompressionWorker first")
  const id = ++jobCounter
  const pending = pendingByWorker.get(pooled.worker)!
  const { promise, resolve, reject } = Promise.withResolvers<Uint8Array>()
  pending.set(id, { resolve, reject })
  const transferable = transferableCopy(data)
  try {
    pooled.worker.postMessage({ id, op: "compress", data: transferable, encoding, level }, [transferable.buffer as ArrayBuffer])
  } catch (err) {
    pending.delete(id)
    throw err
  }
  return promise
}

export async function workerDecompress(data: Uint8Array, encoding: "gzip" | "deflate" | "zstd", maxOutputBytes?: number): Promise<Uint8Array> {
  const pooled = pickWorker()
  if (!pooled) throw new Error("Compression worker pool not initialized; call configureCompressionWorker first")
  const id = ++jobCounter
  const pending = pendingByWorker.get(pooled.worker)!
  const { promise, resolve, reject } = Promise.withResolvers<Uint8Array>()
  pending.set(id, { resolve, reject })
  const transferable = transferableCopy(data)
  try {
    pooled.worker.postMessage({ id, op: "decompress", data: transferable, encoding, maxOutputBytes }, [transferable.buffer as ArrayBuffer])
  } catch (err) {
    pending.delete(id)
    throw err
  }
  return promise
}

export async function shutdownCompressionWorker(): Promise<void> {
  if (!pool) return
  const workers = pool
  pool = null
  for (const entry of workers) {
    const pending = pendingByWorker.get(entry.worker)
    if (pending) {
      const err = new Error("Compression worker pool is shutting down")
      for (const job of pending.values()) job.reject(err)
      pending.clear()
      pendingByWorker.delete(entry.worker)
    }
    try {
      await entry.worker.terminate()
    } catch {}
  }
  pendingByWorker.clear()
}
