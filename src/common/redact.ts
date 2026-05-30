// PII / secret redaction for logs, audit entries, DLQ payloads, and anywhere
// user data is persisted or shipped off-box.
//
// `redactObject` deep-clones a value, replacing the values of sensitive keys
// with "[REDACTED]" and representing non-plain objects faithfully (binaries are
// summarized, Map/Set are unfolded, Date/RegExp pass through). `jsonByteSize` is
// its size-estimating twin used by the audit hot path to drop oversized payloads
// BEFORE paying for a full redaction pass — keep the two in sync when changing
// how a type is represented.

// Any key CONTAINING one of these tokens (case-insensitive) is redacted. Catches
// camelCase / snake_case / prefixed variants such as `userPassword`,
// `db_password_enc`, `x-api-key`, `refreshToken`, `oauth`.
const REDACT_SUBSTRING = /password|secret|token|key|auth|cookie|credential/i

// Exact (case-insensitive) sensitive key names. Covers the common ones the
// substring pattern above misses (passwd, pwd, ssn, cvv, bearer) plus the
// canonical headers/fields, and doubles as the single source of truth for the
// pino redact paths in logger.ts (see `pinoRedactPaths`).
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

// `ancestors` is the current path from the root to `v` (NOT every object ever
// seen). We add `v` before recursing into its children and remove it after, so a
// value that appears in two sibling branches is redacted normally and only a
// genuine back-reference onto the active path is flagged "[Circular]".
function _redact(v: unknown, extra: Set<string> | null, ancestors: object[]): unknown {
  if (v === null || typeof v !== "object") return v

  // Non-plain objects: represent faithfully instead of walking their internals.
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

// Estimate the UTF-8 byte length of `JSON.stringify(redactObject(value))` while
// (a) bailing out as soon as `limit` is exceeded and (b) never allocating the
// redacted clone or the JSON string. The audit log uses this to short-circuit
// oversized payloads on the hot path before committing to deep redaction. The
// size model mirrors `_redact`: sensitive values collapse to "[REDACTED]",
// binaries to "[Buffer NB]", and back-references to "[Circular]".
export function jsonByteSize(value: unknown, limit = Number.POSITIVE_INFINITY, customKeys?: string[]): number {
  const extra = toExtra(customKeys)
  const ancestors: object[] = []
  let bytes = 0

  const quoted = (s: string): number => Buffer.byteLength(s, "utf8") + 2

  const walk = (v: unknown): void => {
    if (bytes > limit) return
    if (v === null || v === undefined) { bytes += 4; return } // "null"
    const t = typeof v
    if (t === "string") { bytes += quoted(v as string); return }
    if (t === "number") { bytes += Number.isFinite(v as number) ? String(v).length : 4; return }
    if (t === "boolean") { bytes += v ? 4 : 5; return }
    if (t !== "object") { bytes += 4; return } // bigint/symbol/function — approximate

    const obj = v as object
    if (obj instanceof Date) { bytes += 26; return } // "2024-01-01T00:00:00.000Z" + quotes
    if (obj instanceof RegExp) { bytes += 2; return } // {}
    if (isBinary(obj)) { bytes += quoted(binarySummary(obj)); return }
    if (ancestors.includes(obj)) { bytes += quoted(CIRCULAR); return }

    ancestors.push(obj)
    if (Array.isArray(obj)) {
      bytes += 2 // []
      for (let i = 0; i < obj.length && bytes <= limit; i++) {
        if (i > 0) bytes += 1 // comma
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
        bytes += quoted(key) + 1 // "key":
        if (isSensitiveKey(key, extra)) bytes += quoted(REDACTED)
        else walk(val)
      }
    } else {
      bytes += 2 // {}
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

// Derive pino/fast-redact paths from REDACT_KEY_NAMES so the structured logger
// and the runtime redactor share one source of truth. fast-redact has no
// recursive descent and is case-sensitive, so this covers each known key name at
// the top level and one level deep only; `redactObject` additionally does
// substring + case-insensitive matching that pino cannot express. Keys with
// non-identifier characters (e.g. "x-api-key") use bracket notation, which bare
// dotted paths reject.
export function pinoRedactPaths(): string[] {
  const paths: string[] = []
  for (const name of REDACT_KEY_NAMES) {
    if (/^[A-Za-z0-9_]+$/.test(name)) {
      paths.push(name, `*.${name}`)
    } else {
      paths.push(`["${name}"]`, `*["${name}"]`)
    }
  }
  return paths
}
