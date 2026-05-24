import type { Codec, CodecName } from "./codec"
import { MessagingError } from "./errors"
import { ErrorCode } from "./error-code"

type FastJsonStringifyModule = (schema: object) => (input: unknown) => string

interface FastJsonCodecOptions {
  schema: object
  name?: string
}

let modCache: FastJsonStringifyModule | null = null

function loadModule(): FastJsonStringifyModule {
  if (modCache) return modCache
  try {
    const m = require("fast-json-stringify") as FastJsonStringifyModule | { default: FastJsonStringifyModule }
    modCache = typeof m === "function" ? m : (m as { default: FastJsonStringifyModule }).default
    return modCache
  } catch {
    throw new MessagingError(ErrorCode.INTERNAL, {
      message: 'Missing optional dependency "fast-json-stringify". Install it to use FastJsonStringifyCodec.'
    })
  }
}

export class FastJsonStringifyCodec implements Codec {
  readonly name: CodecName
  readonly contentType = "application/json"
  private readonly stringify: (input: unknown) => string

  constructor(opts: FastJsonCodecOptions) {
    const factory = loadModule()
    this.stringify = factory(opts.schema)
    this.name = opts.name ?? "fast-json-stringify"
  }

  encode(value: unknown): Uint8Array {
    const str = this.stringify(value)
    const byteLen = Buffer.byteLength(str, "utf8")
    const buf = Buffer.allocUnsafe(byteLen)
    buf.write(str, 0, byteLen, "utf8")
    return buf
  }

  decode<T = unknown>(data: Uint8Array | string): T {
    const str = typeof data === "string" ? data : Buffer.from(data).toString("utf8")
    try {
      return JSON.parse(str) as T
    } catch (err: any) {
      throw new MessagingError(ErrorCode.PARSE_ERROR, { message: `FastJsonStringify decode error: ${err.message}` })
    }
  }
}
