// PII / secret redaction for logs, audit entries, and DLQ payloads.
// NOTE: `jsonByteSize` must mirror `_redact`'s per-type representation; keep them in sync.

// Any key CONTAINING one of these tokens (case-insensitive) is redacted.
const REDACT_SUBSTRING = /password|secret|token|key|auth|cookie|credential/i

// Exact (case-insensitive) sensitive key names; also the source of truth for `pinoRedactPaths`.
export const REDACT_KEY_NAMES: readonly string[] = [
  "password",
  "passwd",
  "pwd",
  "secret",
  "client_secret",
  "token",
  "access_token",
  "refresh_token",
  "id_token",
  "authorization",
  "auth",
  "bearer",
  "apikey",
  "api_key",
  "x-api-key",
  "accesskey",
  "access_key",
  "privatekey",
  "private_key",
  "cookie",
  "set-cookie",
  "credential",
  "credentials",
  "ssn",
  "cvv"
]

const REDACT_KEYS = new Set(REDACT_KEY_NAMES.map((k) => k.toLowerCase()))

const REDACTED = "[REDACTED]"
const CIRCULAR = "[Circular]"

function isSensitiveKey(key: string, extra: Set<string> | null): boolean {
  const lower = key.toLowerCase()
  if (REDACT_KEYS.has(lower)) return true
  if (extra && extra.has(lower)) return true
  return REDACT_SUBSTRING.test(lower)
}

function toExtra(customKeys?: string[]): Set<string> | null {
  return customKeys && customKeys.length ? new Set(customKeys.map((k) => k.toLowerCase())) : null
}

function isBinary(v: object): v is ArrayBufferView {
  return ArrayBuffer.isView(v)
}

function binarySummary(v: ArrayBufferView): string {
  return `[Buffer ${v.byteLength}B]`
}

export function redactObject<T>(value: T, customKeys?: string[]): T {
  return _redact(value, toExtra(customKeys), []) as T
}

// `ancestors` tracks the active root-to-`v` path so only genuine back-references become "[Circular]".
function _redact(v: unknown, extra: Set<string> | null, ancestors: object[]): unknown {
  if (v === null || typeof v !== "object") return v

  if (v instanceof Date || v instanceof RegExp) return v
  if (isBinary(v)) return binarySummary(v)

  if (ancestors.includes(v)) return CIRCULAR
  ancestors.push(v)
  try {
    if (Array.isArray(v)) {
      return v.map((item) => _redact(item, extra, ancestors))
    }
    if (v instanceof Set) {
      return Array.from(v, (item) => _redact(item, extra, ancestors))
    }
    if (v instanceof Map) {
      const out: Record<string, unknown> = {}
      for (const [k, val] of v) {
        const key = typeof k === "string" ? k : String(k)
        out[key] = isSensitiveKey(key, extra) ? REDACTED : _redact(val, extra, ancestors)
      }
      return out
    }
    const out: Record<string, unknown> = {}
    for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
      out[k] = isSensitiveKey(k, extra) ? REDACTED : _redact(val, extra, ancestors)
    }
    return out
  } finally {
    ancestors.pop()
  }
}

// Estimate UTF-8 byte length of `JSON.stringify(redactObject(value))`, bailing once `limit` is exceeded and without allocating.
export function jsonByteSize(value: unknown, limit = Number.POSITIVE_INFINITY, customKeys?: string[]): number {
  const extra = toExtra(customKeys)
  const ancestors: object[] = []
  let bytes = 0

  const quoted = (s: string): number => Buffer.byteLength(s, "utf8") + 2

  const walk = (v: unknown): void => {
    if (bytes > limit) return
    if (v === null || v === undefined) {
      bytes += 4
      return
    }
    const t = typeof v
    if (t === "string") {
      bytes += quoted(v as string)
      return
    }
    if (t === "number") {
      bytes += Number.isFinite(v as number) ? String(v).length : 4
      return
    }
    if (t === "boolean") {
      bytes += v ? 4 : 5
      return
    }
    if (t !== "object") {
      bytes += 4
      return
    } // bigint/symbol/function — approximate

    const obj = v as object
    if (obj instanceof Date) {
      bytes += 26
      return
    } // ISO string + quotes
    if (obj instanceof RegExp) {
      bytes += 2
      return
    }
    if (isBinary(obj)) {
      bytes += quoted(binarySummary(obj))
      return
    }
    if (ancestors.includes(obj)) {
      bytes += quoted(CIRCULAR)
      return
    }

    ancestors.push(obj)
    if (Array.isArray(obj)) {
      bytes += 2
      for (let i = 0; i < obj.length && bytes <= limit; i++) {
        if (i > 0) bytes += 1
        walk(obj[i])
      }
    } else if (obj instanceof Set) {
      bytes += 2
      let first = true
      for (const item of obj) {
        if (bytes > limit) break
        if (!first) bytes += 1
        first = false
        walk(item)
      }
    } else if (obj instanceof Map) {
      bytes += 2
      let first = true
      for (const [k, val] of obj) {
        if (bytes > limit) break
        const key = typeof k === "string" ? k : String(k)
        if (!first) bytes += 1
        first = false
        bytes += quoted(key) + 1
        if (isSensitiveKey(key, extra)) bytes += quoted(REDACTED)
        else walk(val)
      }
    } else {
      bytes += 2
      let first = true
      for (const [k, val] of Object.entries(obj as Record<string, unknown>)) {
        if (bytes > limit) break
        if (val === undefined) continue // JSON omits undefined object members
        if (!first) bytes += 1
        first = false
        bytes += quoted(k) + 1
        if (isSensitiveKey(k, extra)) bytes += quoted(REDACTED)
        else walk(val)
      }
    }
    ancestors.pop()
  }

  walk(value)
  return bytes
}

// Derive bounded-depth pino/fast-redact paths. Logger inputs are also
// recursively redacted before pino sees them; these paths are defense in depth.
export function pinoRedactPaths(maxDepth = 8): string[] {
  const paths: string[] = []
  for (const name of REDACT_KEY_NAMES) {
    for (let depth = 0; depth <= maxDepth; depth++) {
      if (/^[A-Za-z0-9_]+$/.test(name)) {
        paths.push(`${depth === 0 ? "" : "*.".repeat(depth)}${name}`)
      } else {
        const prefix = depth === 0 ? "" : `${"*.".repeat(depth - 1)}*`
        paths.push(`${prefix}["${name}"]`)
      }
    }
  }
  return paths
}
