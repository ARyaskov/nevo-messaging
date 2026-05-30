export const BIGINT_SENTINEL = "@@nevo:bigint:"
const LEGACY_BIGINT_RE = /^(\d+)n$/

export interface BigIntSerializable {
  [key: string]: any
}

export const bigIntReplacer = function (_key: string, value: unknown): unknown {
  if (typeof value === "bigint") return `${BIGINT_SENTINEL}${value.toString()}`
  return value
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

/**
 * Maximum nesting depth walked by serializeBigInt/deserializeBigInt. Without a
 * cap, a deeply-nested (or maliciously crafted) payload could blow the call
 * stack; with a WeakSet seen-guard, cyclic input is also caught. Real-world
 * messaging payloads are nowhere near this deep.
 */
const MAX_BIGINT_DEPTH = 512

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
    for (const [key, value] of Object.entries(obj)) serialized[key] = serializeBigIntInner(value, depth + 1, seen)
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
    for (const [key, value] of Object.entries(obj)) deserialized[key] = deserializeBigIntInner(value, options, depth + 1, seen)
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
