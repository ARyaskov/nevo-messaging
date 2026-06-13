export const BIGINT_SENTINEL = "@@nevo:bigint:"
const LEGACY_BIGINT_RE = /^(\d+)n$/

export interface BigIntSerializable {
  [key: string]: any
}

export const bigIntReplacer = function (_key: string, value: unknown): unknown {
  if (typeof value === "bigint") return `${BIGINT_SENTINEL}${value.toString()}`
  return value
}

const MAX_WIRE_DEPTH = 512

/**
 * Normalise values to one wire model shared by JSON and MessagePack codecs.
 * It intentionally follows JSON's undefined semantics and represents Date and
 * arbitrary-size BigInt without relying on codec-specific extensions.
 */
export function normalizeWireValue(value: unknown): unknown {
  const seen = new WeakSet<object>()

  const visit = (input: unknown, depth: number, inArray: boolean): unknown => {
    if (input === undefined || typeof input === "function" || typeof input === "symbol") {
      return inArray ? null : undefined
    }
    if (typeof input === "bigint") return `${BIGINT_SENTINEL}${input.toString()}`
    if (input === null || typeof input !== "object") return input
    if (input instanceof Date) return input.toISOString()
    if (depth >= MAX_WIRE_DEPTH) {
      throw new RangeError(`normalizeWireValue: maximum nesting depth (${MAX_WIRE_DEPTH}) exceeded`)
    }
    if (seen.has(input)) throw new TypeError("normalizeWireValue: circular reference detected")
    seen.add(input)
    try {
      if (Array.isArray(input)) {
        return input.map((item) => visit(item, depth + 1, true))
      }
      if (ArrayBuffer.isView(input) || input instanceof ArrayBuffer) return input
      const out: Record<string, unknown> = {}
      for (const [key, item] of Object.entries(input as Record<string, unknown>)) {
        const normalized = visit(item, depth + 1, false)
        if (normalized !== undefined) setRebuiltKey(out, key, normalized)
      }
      return out
    } finally {
      seen.delete(input)
    }
  }

  return visit(value, 0, true)
}

export function makeBigIntReviver(opts?: { acceptLegacy?: boolean }): (key: string, value: unknown) => unknown {
  const acceptLegacy = opts?.acceptLegacy === true
  return function (_key, value) {
    if (typeof value !== "string") return value
    if (value.length > BIGINT_SENTINEL.length && value.startsWith(BIGINT_SENTINEL)) {
      const digits = value.slice(BIGINT_SENTINEL.length)
      if (/^-?\d+$/.test(digits)) return BigInt(digits)
      return value
    }
    if (acceptLegacy) {
      const m = LEGACY_BIGINT_RE.exec(value)
      if (m) return BigInt(m[1])
    }
    return value
  }
}

const defaultReviver = makeBigIntReviver()
const legacyReviver = makeBigIntReviver({ acceptLegacy: true })

const MAX_BIGINT_DEPTH = 512

// Assigning a "__proto__" key from untrusted input would mutate the prototype
// of the rebuilt object instead of creating a data property.
function setRebuiltKey(target: Record<string, unknown>, key: string, value: unknown): void {
  if (key === "__proto__") {
    Object.defineProperty(target, key, { value, enumerable: true, writable: true, configurable: true })
  } else {
    target[key] = value
  }
}

function serializeBigIntInner(obj: any, depth: number, seen: WeakSet<object>): any {
  if (obj === null || obj === undefined) return obj
  if (typeof obj === "bigint") return `${BIGINT_SENTINEL}${obj.toString()}`
  if (typeof obj !== "object") return obj

  if (depth >= MAX_BIGINT_DEPTH) {
    throw new RangeError(`serializeBigInt: maximum nesting depth (${MAX_BIGINT_DEPTH}) exceeded`)
  }
  if (seen.has(obj)) {
    throw new TypeError("serializeBigInt: circular reference detected")
  }
  seen.add(obj)
  try {
    if (Array.isArray(obj)) return obj.map((v) => serializeBigIntInner(v, depth + 1, seen))
    const serialized: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(obj)) setRebuiltKey(serialized, key, serializeBigIntInner(value, depth + 1, seen))
    return serialized
  } finally {
    seen.delete(obj)
  }
}

export function serializeBigInt(obj: any): any {
  return serializeBigIntInner(obj, 0, new WeakSet<object>())
}

function deserializeBigIntInner(obj: any, options: { acceptLegacy?: boolean } | undefined, depth: number, seen: WeakSet<object>): any {
  if (obj === null || obj === undefined) return obj

  if (typeof obj === "string") {
    if (obj.startsWith(BIGINT_SENTINEL)) {
      const digits = obj.slice(BIGINT_SENTINEL.length)
      if (/^-?\d+$/.test(digits)) return BigInt(digits)
    }
    if (options?.acceptLegacy) {
      const m = LEGACY_BIGINT_RE.exec(obj)
      if (m) return BigInt(m[1])
    }
    return obj
  }

  if (typeof obj !== "object") return obj

  if (depth >= MAX_BIGINT_DEPTH) {
    throw new RangeError(`deserializeBigInt: maximum nesting depth (${MAX_BIGINT_DEPTH}) exceeded`)
  }
  if (seen.has(obj)) {
    throw new TypeError("deserializeBigInt: circular reference detected")
  }
  seen.add(obj)
  try {
    if (Array.isArray(obj)) return obj.map((v) => deserializeBigIntInner(v, options, depth + 1, seen))
    const deserialized: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(obj)) setRebuiltKey(deserialized, key, deserializeBigIntInner(value, options, depth + 1, seen))
    return deserialized
  } finally {
    seen.delete(obj)
  }
}

export function deserializeBigInt(obj: any, options?: { acceptLegacy?: boolean }): any {
  return deserializeBigIntInner(obj, options, 0, new WeakSet<object>())
}

export function stringifyWithBigInt(obj: unknown): string {
  return JSON.stringify(obj, bigIntReplacer)
}

export function parseWithBigInt(str: string, options?: { acceptLegacy?: boolean }): any {
  return JSON.parse(str, options?.acceptLegacy ? legacyReviver : defaultReviver)
}
