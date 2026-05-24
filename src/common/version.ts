export interface ParsedMethod {
  name: string
  version: string | null
}

export const DEFAULT_METHOD_VERSION = "v1"

export function parseMethod(method: string): ParsedMethod {
  const at = method.lastIndexOf("@")
  if (at < 0) return { name: method, version: null }
  return { name: method.slice(0, at), version: method.slice(at + 1) }
}

export function formatMethod(name: string, version: string | null | undefined): string {
  if (!version) return name
  return `${name}@${version}`
}

export function isVersionCompatible(want: string | null, have: string | null): boolean {
  if (!want) return true
  if (!have) return want === DEFAULT_METHOD_VERSION
  return want === have
}
