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

export function serializeBigInt(obj: any): any {
  if (obj === null || obj === undefined) return obj
  if (typeof obj === "bigint") return `${BIGINT_SENTINEL}${obj.toString()}`
  if (Array.isArray(obj)) return obj.map(serializeBigInt)
  if (typeof obj === "object") {
    const serialized: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(obj)) serialized[key] = serializeBigInt(value)
    return serialized
  }
  return obj
}

export function deserializeBigInt(obj: any, options?: { acceptLegacy?: boolean }): any {
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

  if (Array.isArray(obj)) return obj.map((v) => deserializeBigInt(v, options))

  if (typeof obj === "object") {
    const deserialized: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(obj)) deserialized[key] = deserializeBigInt(value, options)
    return deserialized
  }

  return obj
}

export function stringifyWithBigInt(obj: unknown): string {
  return JSON.stringify(obj, bigIntReplacer)
}

export function parseWithBigInt(str: string, options?: { acceptLegacy?: boolean }): any {
  return JSON.parse(str, options?.acceptLegacy ? legacyReviver : defaultReviver)
}
