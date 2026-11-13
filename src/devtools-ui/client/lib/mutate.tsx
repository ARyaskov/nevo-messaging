import { useState } from "react"

const STORAGE_KEY = "nevo-devtools-token"

/**
 * Token for the DevTools mutating endpoints (POST /api/config, POST /api/replay).
 * The server requires it whenever a token is configured — always on a
 * non-loopback bind — so the dashboard stores it locally and sends the
 * `x-nevo-devtools-token` header.
 */
export function getDevToolsToken(): string {
  try {
    return globalThis.localStorage?.getItem(STORAGE_KEY) ?? ""
  } catch {
    return ""
  }
}

export function setDevToolsToken(token: string): void {
  try {
    if (token) globalThis.localStorage?.setItem(STORAGE_KEY, token)
    else globalThis.localStorage?.removeItem(STORAGE_KEY)
  } catch {
    // Storage unavailable (private mode): keep the in-memory value only.
  }
}

/** Headers for a mutating POST; includes the token when one is stored. */
export function mutatingHeaders(): Record<string, string> {
  const headers: Record<string, string> = { "content-type": "application/json" }
  const token = getDevToolsToken()
  return token ? { ...headers, "x-nevo-devtools-token": token } : headers
}

const inputStyle = {
  padding: "6px 10px",
  background: "var(--panel-2)",
  color: "var(--text)",
  border: "1px solid var(--border)",
  borderRadius: 6,
  width: 280
} as const

/** Shared token input used by the mutating panels. */
export function TokenInput() {
  const [token, setToken] = useState(getDevToolsToken())
  return (
    <label style={{ display: "flex", flexDirection: "column", gap: 4, marginBottom: 12 }}>
      <span className="nv-muted" style={{ fontSize: 12 }}>
        API token (required on non-loopback binds)
      </span>
      <input
        type="password"
        value={token}
        placeholder="leave empty on loopback"
        onChange={(e) => {
          setToken(e.target.value)
          setDevToolsToken(e.target.value)
        }}
        style={inputStyle}
      />
    </label>
  )
}
