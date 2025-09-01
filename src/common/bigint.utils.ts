export interface BigIntSerializable {
  [key: string]: any
}

export function serializeBigInt(obj: any): any {
  if (obj === null || obj === undefined) {
    return obj
  }

  if (typeof obj === "bigint") {
    return `${obj.toString()}n`
  }

  if (Array.isArray(obj)) {
    return obj.map(serializeBigInt)
  }

  if (typeof obj === "object") {
    const serialized: any = {}
    for (const [key, value] of Object.entries(obj)) {
      serialized[key] = serializeBigInt(value)
    }
    return serialized
  }

  return obj
}

export function deserializeBigInt(obj: any): any {
  if (obj === null || obj === undefined) {
    return obj
  }

  if (typeof obj === "string" && /^\d+n$/.test(obj)) {
    return BigInt(obj.slice(0, -1))
  }

  if (Array.isArray(obj)) {
    return obj.map(deserializeBigInt)
  }

  if (typeof obj === "object") {
    const deserialized: any = {}
    for (const [key, value] of Object.entries(obj)) {
      deserialized[key] = deserializeBigInt(value)
    }
    return deserialized
  }

  return obj
}

export function stringifyWithBigInt(obj: any): string {
  return JSON.stringify(serializeBigInt(obj))
}

export function parseWithBigInt(str: string): any {
  return deserializeBigInt(JSON.parse(str))
}
