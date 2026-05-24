import {
  gzipSync,
  gunzipSync,
  deflateSync,
  inflateSync,
  constants as zlibConstants
} from "node:zlib"
import { gzip, gunzip, deflate, inflate } from "node:zlib"
import { promisify } from "node:util"
import type { CompressionOptions } from "./types"

type Algorithm = "gzip" | "deflate" | "zstd"
export type CompressionEncoding = Algorithm | "identity"

export interface ResolvedCompressionOptions {
  enabled: boolean
  algorithm: Algorithm
  threshold: number
  level: number
  async: boolean
}

export function resolveCompressionOptions(opts?: CompressionOptions): ResolvedCompressionOptions {
  return {
    enabled: opts?.enabled === true,
    algorithm: (opts?.algorithm as Algorithm) ?? "gzip",
    threshold: opts?.threshold ?? 1024,
    level: opts?.level ?? 6,
    async: opts?.async !== false
  }
}

const gzipAsync = promisify(gzip)
const gunzipAsync = promisify(gunzip)
const deflateAsync = promisify(deflate)
const inflateAsync = promisify(inflate)

let zstdSync: { compress: (b: Uint8Array, opts?: object) => Buffer; decompress: (b: Uint8Array) => Buffer } | null = null
let zstdAsync: { compress: (b: Uint8Array, opts?: object) => Promise<Buffer>; decompress: (b: Uint8Array) => Promise<Buffer> } | null = null

function tryLoadZstd(): boolean {
  if (zstdSync && zstdAsync) return true
  try {
    const napi: any = require("@napi-rs/zstd")
    if (napi && typeof napi.compressSync === "function") {
      zstdSync = {
        compress: (b) => Buffer.from(napi.compressSync(Buffer.from(b))),
        decompress: (b) => Buffer.from(napi.decompressSync(Buffer.from(b)))
      }
      zstdAsync = {
        compress: async (b) => Buffer.from(await napi.compress(Buffer.from(b))),
        decompress: async (b) => Buffer.from(await napi.decompress(Buffer.from(b)))
      }
      return true
    }
  } catch {}
  try {
    const zlib = require("node:zlib") as typeof import("node:zlib")
    const z: any = zlib
    if (typeof z.zstdCompressSync === "function") {
      zstdSync = {
        compress: (b, opts) => z.zstdCompressSync(b, opts),
        decompress: (b) => z.zstdDecompressSync(b)
      }
      zstdAsync = {
        compress: promisify(z.zstdCompress),
        decompress: promisify(z.zstdDecompress)
      }
      return true
    }
  } catch {}
  return false
}

export function maybeCompress(buf: Uint8Array, opts: ResolvedCompressionOptions): { data: Uint8Array; encoding: CompressionEncoding } {
  if (!opts.enabled || buf.byteLength < opts.threshold) {
    return { data: buf, encoding: "identity" }
  }
  if (opts.algorithm === "zstd" && tryLoadZstd()) {
    return { data: zstdSync!.compress(buf, { params: { 100: opts.level } }), encoding: "zstd" }
  }
  if (opts.algorithm === "gzip" || opts.algorithm === "zstd") {
    return { data: gzipSync(buf, { level: opts.level }), encoding: "gzip" }
  }
  return { data: deflateSync(buf, { level: opts.level }), encoding: "deflate" }
}

export async function maybeCompressAsync(buf: Uint8Array, opts: ResolvedCompressionOptions): Promise<{ data: Uint8Array; encoding: CompressionEncoding }> {
  if (!opts.enabled || buf.byteLength < opts.threshold) {
    return { data: buf, encoding: "identity" }
  }
  try {
    const { isCompressionWorkerEnabled, compressionWorkerThreshold, workerCompress } = require("./compression-worker") as typeof import("./compression-worker")
    if (isCompressionWorkerEnabled() && buf.byteLength >= compressionWorkerThreshold()) {
      const algo: "gzip" | "deflate" | "zstd" = opts.algorithm === "zstd" && tryLoadZstd() ? "zstd" : opts.algorithm === "deflate" ? "deflate" : "gzip"
      const data = await workerCompress(buf, algo, opts.level)
      return { data, encoding: algo }
    }
  } catch {}
  if (opts.algorithm === "zstd" && tryLoadZstd()) {
    const data = await zstdAsync!.compress(buf, { params: { 100: opts.level } })
    return { data, encoding: "zstd" }
  }
  if (opts.algorithm === "gzip" || opts.algorithm === "zstd") {
    const data = await gzipAsync(buf, { level: opts.level })
    return { data, encoding: "gzip" }
  }
  const data = await deflateAsync(buf, { level: opts.level })
  return { data, encoding: "deflate" }
}

export function maybeDecompress(buf: Uint8Array, encoding?: string): Uint8Array {
  if (!encoding || encoding === "identity") return buf
  if (encoding === "gzip") return gunzipSync(buf)
  if (encoding === "deflate") return inflateSync(buf)
  if (encoding === "zstd" && tryLoadZstd()) return zstdSync!.decompress(buf)
  return buf
}

export async function maybeDecompressAsync(buf: Uint8Array, encoding?: string): Promise<Uint8Array> {
  if (!encoding || encoding === "identity") return buf
  if (encoding === "gzip") return gunzipAsync(buf)
  if (encoding === "deflate") return inflateAsync(buf)
  if (encoding === "zstd" && tryLoadZstd()) return zstdAsync!.decompress(buf)
  return buf
}

void zlibConstants
