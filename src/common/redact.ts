const DEFAULT_REDACT_KEYS = new Set([
  "password",
  "passwd",
  "secret",
  "token",
  "authorization",
  "apikey",
  "api_key",
  "accesskey",
  "access_key",
  "privatekey",
  "private_key",
  "cookie",
  "set-cookie",
  "ssn"
])

const REDACTED = "[REDACTED]"

export function redactObject<T>(value: T, customKeys?: string[]): T {
  const extra = customKeys ? new Set(customKeys.map((k) => k.toLowerCase())) : null
  return _redact(value, extra) as T
}

function _redact(v: unknown, extra: Set<string> | null, seen = new WeakSet()): unknown {
  if (v === null || v === undefined) return v
  if (typeof v !== "object") return v
  if (seen.has(v as object)) return "[Circular]"
  seen.add(v as object)

  if (Array.isArray(v)) {
    return v.map((item) => _redact(item, extra, seen))
  }

  const out: Record<string, unknown> = {}
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
    const lower = k.toLowerCase()
    if (DEFAULT_REDACT_KEYS.has(lower) || extra?.has(lower)) {
      out[k] = REDACTED
    } else {
      out[k] = _redact(val, extra, seen)
    }
  }
  return out
}
