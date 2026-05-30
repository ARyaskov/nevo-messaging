import { createRequire } from "node:module"
import { bigIntReplacer, makeBigIntReviver } from "./bigint.utils"
import { MessagingError } from "./errors"
import { ErrorCode } from "./error-code"

const nodeRequire = createRequire(__filename)

const legacyReviver = makeBigIntReviver({ acceptLegacy: true })

export type CodecName = "msgpack" | "json" | "json-fast" | string

export interface Codec {
  readonly name: CodecName
  readonly contentType: string
  encode(value: unknown): Uint8Array
  decode<T = unknown>(data: Uint8Array | string): T
}

const textDecoder = new TextDecoder()
const textEncoder = new TextEncoder()

export function getSharedTextDecoder() { return textDecoder }
export function getSharedTextEncoder() { return textEncoder }

export class JsonCodec implements Codec {
  readonly name: CodecName = "json"
  readonly contentType = "application/json"

  encode(value: unknown): Uint8Array {
    const str = JSON.stringify(value, bigIntReplacer)
    const byteLen = Buffer.byteLength(str, "utf8")
    const buf = Buffer.allocUnsafe(byteLen)
    buf.write(str, 0, byteLen, "utf8")
    return buf
  }

  decode<T = unknown>(data: Uint8Array | string): T {
    const str = typeof data === "string" ? data : textDecoder.decode(data)
    try {
      return JSON.parse(str, legacyReviver) as T
    } catch (err: any) {
      throw new MessagingError(ErrorCode.PARSE_ERROR, { message: `JSON parse error: ${err.message}` })
    }
  }
}

export class JsonCodecFast implements Codec {
  readonly name: CodecName = "json-fast"
  readonly contentType = "application/json"

  encode(value: unknown): Uint8Array {
    const str = JSON.stringify(value)
    const byteLen = Buffer.byteLength(str, "utf8")
    const buf = Buffer.allocUnsafe(byteLen)
    buf.write(str, 0, byteLen, "utf8")
    return buf
  }

  decode<T = unknown>(data: Uint8Array | string): T {
    const str = typeof data === "string" ? data : textDecoder.decode(data)
    try {
      return JSON.parse(str) as T
    } catch (err: any) {
      throw new MessagingError(ErrorCode.PARSE_ERROR, { message: `JSON parse error: ${err.message}` })
    }
  }
}

export class MessagePackCodec implements Codec {
  readonly name: CodecName = "msgpack"
  readonly contentType = "application/msgpack"
  private encoder?: { encode(v: unknown): Uint8Array }
  private decoder?: { decode(b: Uint8Array): unknown }

  private ensureLoaded(): void {
    if (this.encoder && this.decoder) return
    const loaded = getOrCreateSharedMsgpack()
    this.encoder = loaded.encoder
    this.decoder = loaded.decoder
  }

  encode(value: unknown): Uint8Array {
    this.ensureLoaded()
    try {
      const out = this.encoder!.encode(value)
      if (out.byteOffset !== 0 || out.byteLength !== out.buffer.byteLength) {
        const copy = new Uint8Array(out.byteLength)
        copy.set(out)
        return copy
      }
      return out
    } catch (err: any) {
      throw new MessagingError(ErrorCode.PARSE_ERROR, { message: `MessagePack encode error: ${err.message}` })
    }
  }

  decode<T = unknown>(data: Uint8Array | string): T {
    this.ensureLoaded()
    if (typeof data === "string") {
      data = Buffer.from(data, "binary")
    }
    try {
      return this.decoder!.decode(data) as T
    } catch (err: any) {
      throw new MessagingError(ErrorCode.PARSE_ERROR, { message: `MessagePack decode error: ${err.message}` })
    }
  }
}

let sharedMsgpack: { encoder: { encode(v: unknown): Uint8Array }; decoder: { decode(b: Uint8Array): unknown } } | null = null

function getOrCreateSharedMsgpack(): { encoder: { encode(v: unknown): Uint8Array }; decoder: { decode(b: Uint8Array): unknown } } {
  if (sharedMsgpack) return sharedMsgpack
  try {
    const mp = nodeRequire("@msgpack/msgpack") as typeof import("@msgpack/msgpack")
    const EncoderCtor = (mp as unknown as { Encoder?: new (opts: object) => { encode(v: unknown): Uint8Array } }).Encoder
    const DecoderCtor = (mp as unknown as { Decoder?: new (opts: object) => { decode(b: Uint8Array): unknown } }).Decoder
    const opts = { useBigInt64: true }
    sharedMsgpack = {
      encoder: EncoderCtor ? new EncoderCtor(opts) : { encode: (v) => mp.encode(v, opts) },
      decoder: DecoderCtor ? new DecoderCtor(opts) : { decode: (b) => mp.decode(b, opts) }
    }
    return sharedMsgpack
  } catch {
    throw new MessagingError(ErrorCode.INTERNAL, {
      message: 'Missing optional dependency "@msgpack/msgpack". Install it to use MessagePack codec, or pass codec: new JsonCodec().'
    })
  }
}

const registry = new Map<CodecName, Codec>()

const sharedJsonCodec = new JsonCodec()
const sharedJsonFastCodec = new JsonCodecFast()
let sharedMsgpackCodec: MessagePackCodec | null = null

export function registerCodec(codec: Codec): void {
  registry.set(codec.name, codec)
}

export function getCodec(name: CodecName): Codec {
  if (name === "json") return sharedJsonCodec
  if (name === "json-fast") return sharedJsonFastCodec
  if (name === "msgpack") {
    if (!sharedMsgpackCodec) sharedMsgpackCodec = new MessagePackCodec()
    return sharedMsgpackCodec
  }
  const c = registry.get(name)
  if (!c) throw new MessagingError(ErrorCode.INTERNAL, { message: `Codec "${name}" is not registered` })
  return c
}

let defaultCodec: Codec | null = null

export function getDefaultCodec(): Codec {
  if (!defaultCodec) {
    try {
      if (!sharedMsgpackCodec) sharedMsgpackCodec = new MessagePackCodec()
      sharedMsgpackCodec.encode({ probe: 1 })
      defaultCodec = sharedMsgpackCodec
    } catch {
      defaultCodec = sharedJsonCodec
    }
  }
  return defaultCodec
}

export function setDefaultCodec(codec: Codec): void {
  defaultCodec = codec
  registerCodec(codec)
}

registerCodec(sharedJsonCodec)
registerCodec(sharedJsonFastCodec)
try {
  if (!sharedMsgpackCodec) sharedMsgpackCodec = new MessagePackCodec()
  registerCodec(sharedMsgpackCodec)
} catch {
  // msgpack optional, skip
}
