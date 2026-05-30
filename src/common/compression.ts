import {
  gzipSync,
  gunzipSync,
  deflateSync,
  inflateSync,
  constants as zlibConstants
} from "node:zlib"
import { gzip, gunzip, deflate, inflate } from "node:zlib"
import * as nodeZlib from "node:zlib"
import { createRequire } from "node:module"
import { promisify } from "node:util"

const nodeRequire = createRequire(__filename)
import type { CompressionOptions } from "./types"
import { PayloadTooLargeError } from "./errors"

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
    const z: any = nodeZlib
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
    const { isCompressionWorkerEnabled, compressionWorkerThreshold, workerCompress } = nodeRequire("./compression-worker") as typeof import("./compression-worker")
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

// Inbound buffers at/above this size decompress off the event loop (worker pool
// or libuv) via maybeDecompressAsync; smaller ones inflate synchronously so the
// common path never pays for an async hop.
export const ASYNC_DECOMPRESS_THRESHOLD = 64 * 1024

export function shouldDecompressAsync(byteLength: number, encoding?: string): boolean {
  if (!encoding || encoding === "identity") return false
  return byteLength >= ASYNC_DECOMPRESS_THRESHOLD
}

function zlibLimitOptions(maxOutputBytes?: number): { maxOutputLength?: number } {
  // zlib's gunzip/inflate throw ERR_BUFFER_TOO_LARGE the moment the inflated
  // output would exceed maxOutputLength, so a bomb is stopped before the full
  // payload is ever allocated.
  return maxOutputBytes !== undefined ? { maxOutputLength: maxOutputBytes } : {}
}

function capCheck(out: Uint8Array, maxOutputBytes?: number): Uint8Array {
  if (maxOutputBytes !== undefined && out.byteLength > maxOutputBytes) {
    throw new PayloadTooLargeError(out.byteLength, maxOutputBytes)
  }
  return out
}

function isBufferTooLarge(err: unknown): boolean {
  return err instanceof PayloadTooLargeError || (err as { code?: string } | null)?.code === "ERR_BUFFER_TOO_LARGE"
}

function translateDecompressError(err: unknown, maxOutputBytes?: number): Error {
  // zlib does not report the would-be size, so surface the cap it hit. Either
  // way the receiver sees PAYLOAD_TOO_LARGE rather than a raw RangeError.
  if (isBufferTooLarge(err)) {
    return err instanceof PayloadTooLargeError ? err : new PayloadTooLargeError(maxOutputBytes ?? 0, maxOutputBytes ?? 0)
  }
  return err instanceof Error ? err : new Error(String(err))
}

// Reads the (optional) Frame_Content_Size from a zstd frame header. zstd has no
// streaming output cap like zlib's maxOutputLength, so we reject up front when
// the declared size already exceeds the limit. Returns null when the frame
// omits the size (legal for streamed frames) — capCheck is the backstop there.
function zstdContentSize(buf: Uint8Array): number | null {
  if (buf.byteLength < 5) return null
  if (buf[0] !== 0x28 || buf[1] !== 0xb5 || buf[2] !== 0x2f || buf[3] !== 0xfd) return null
  const fhd = buf[4]
  const fcsFlag = (fhd >> 6) & 0x3
  const singleSegment = (fhd >> 5) & 0x1
  const dictIdFlag = fhd & 0x3
  let offset = 5
  if (!singleSegment) offset += 1
  offset += dictIdFlag === 3 ? 4 : dictIdFlag
  const fcsSize = fcsFlag === 0 ? (singleSegment ? 1 : 0) : fcsFlag === 1 ? 2 : fcsFlag === 2 ? 4 : 8
  if (fcsSize === 0 || buf.byteLength < offset + fcsSize) return null
  let size = 0
  for (let i = 0; i < fcsSize; i++) size += buf[offset + i] * 2 ** (8 * i)
  if (fcsSize === 2) size += 256
  return size
}

function assertZstdWithinCap(buf: Uint8Array, maxOutputBytes?: number): void {
  if (maxOutputBytes === undefined) return
  const size = zstdContentSize(buf)
  if (size !== null && size > maxOutputBytes) throw new PayloadTooLargeError(size, maxOutputBytes)
}

export function maybeDecompress(buf: Uint8Array, encoding?: string, maxOutputBytes?: number): Uint8Array {
  if (!encoding || encoding === "identity") return buf
  try {
    if (encoding === "gzip") return capCheck(gunzipSync(buf, zlibLimitOptions(maxOutputBytes)), maxOutputBytes)
    if (encoding === "deflate") return capCheck(inflateSync(buf, zlibLimitOptions(maxOutputBytes)), maxOutputBytes)
    if (encoding === "zstd" && tryLoadZstd()) {
      assertZstdWithinCap(buf, maxOutputBytes)
      return capCheck(zstdSync!.decompress(buf), maxOutputBytes)
    }
  } catch (err) {
    throw translateDecompressError(err, maxOutputBytes)
  }
  return buf
}

export async function maybeDecompressAsync(buf: Uint8Array, encoding?: string, maxOutputBytes?: number): Promise<Uint8Array> {
  if (!encoding || encoding === "identity") return buf
  // Large gzip/deflate buffers offload to the worker pool (mirrors the compress
  // path). The worker enforces maxOutputBytes itself, so a bomb is rejected
  // there without ever allocating on the main thread.
  if (encoding === "gzip" || encoding === "deflate") {
    const offloaded = await tryWorkerDecompress(buf, encoding, maxOutputBytes)
    if (offloaded) return offloaded
  }
  try {
    if (encoding === "gzip") return capCheck(await gunzipAsync(buf, zlibLimitOptions(maxOutputBytes)), maxOutputBytes)
    if (encoding === "deflate") return capCheck(await inflateAsync(buf, zlibLimitOptions(maxOutputBytes)), maxOutputBytes)
    if (encoding === "zstd" && tryLoadZstd()) {
      assertZstdWithinCap(buf, maxOutputBytes)
      return capCheck(await zstdAsync!.decompress(buf), maxOutputBytes)
    }
  } catch (err) {
    throw translateDecompressError(err, maxOutputBytes)
  }
  return buf
}

// Returns the decompressed bytes when the worker pool handled the buffer, or
// null to fall back to inline decompression (pool disabled, below threshold, or
// a transient worker failure). A size-cap violation in the worker propagates as
// PAYLOAD_TOO_LARGE — it must NOT fall through and re-inflate inline.
async function tryWorkerDecompress(buf: Uint8Array, encoding: "gzip" | "deflate", maxOutputBytes?: number): Promise<Uint8Array | null> {
  let mod: typeof import("./compression-worker")
  try {
    mod = nodeRequire("./compression-worker") as typeof import("./compression-worker")
  } catch {
    return null
  }
  if (!mod.isCompressionWorkerEnabled() || buf.byteLength < mod.compressionWorkerThreshold()) return null
  try {
    return capCheck(await mod.workerDecompress(buf, encoding, maxOutputBytes), maxOutputBytes)
  } catch (err) {
    if (isBufferTooLarge(err)) throw translateDecompressError(err, maxOutputBytes)
    return null
  }
}

void zlibConstants
