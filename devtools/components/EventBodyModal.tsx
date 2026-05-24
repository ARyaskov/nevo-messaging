"use client"

import { useEffect, useState } from "react"
import type { DevToolsEvent } from "@riaskov/nevo-messaging"

interface Props {
  event: DevToolsEvent | null
  onClose: () => void
}

/**
 * Read-only modal that pretty-prints a single `DevToolsEvent` and lets the
 * operator copy the JSON to clipboard.
 *
 * Closes on:
 *   - clicking the backdrop
 *   - clicking the × button
 *   - pressing Escape
 *
 * The body itself is rendered into a textarea (rather than <pre>) so that
 * native text-selection, scrolling, find-in-page, and right-click "save as"
 * all work out of the box on every platform.
 */
export function EventBodyModal({ event, onClose }: Props) {
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    if (!event) return
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose() }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [event, onClose])

  // Reset the "Copied!" badge whenever a different event is opened.
  useEffect(() => { setCopied(false) }, [event])

  if (!event) return null

  const body = JSON.stringify(event, bigintReplacer, 2)
  const title = [event.type, event.service, event.method].filter(Boolean).join(" · ") || "Event body"

  const handleCopy = async () => {
    try {
      if (typeof navigator !== "undefined" && navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(body)
      } else {
        // Older browsers / non-secure contexts: fallback via a temporary textarea.
        const ta = document.createElement("textarea")
        ta.value = body
        ta.setAttribute("readonly", "")
        ta.style.position = "fixed"
        ta.style.top = "-9999px"
        document.body.appendChild(ta)
        ta.select()
        try { document.execCommand("copy") } catch {}
        document.body.removeChild(ta)
      }
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      // Best-effort; if clipboard write fails, the textarea is still selectable.
    }
  }

  return (
    <div
      className="nv-modal-backdrop"
      role="dialog"
      aria-modal="true"
      aria-label="Event body"
      onClick={onClose}
    >
      <div className="nv-modal" onClick={(e) => e.stopPropagation()}>
        <div className="nv-modal-header">
          <div className="nv-modal-title nv-mono">{title}</div>
          <div className="nv-row" style={{ gap: 8 }}>
            <button
              type="button"
              className="nv-btn"
              onClick={handleCopy}
              aria-live="polite"
            >
              {copied ? "Copied!" : "Copy"}
            </button>
            <button
              type="button"
              className="nv-btn-icon"
              onClick={onClose}
              aria-label="Close"
              title="Close (Esc)"
            >
              ×
            </button>
          </div>
        </div>
        <textarea
          className="nv-modal-body nv-mono"
          value={body}
          readOnly
          spellCheck={false}
          // `onFocus → select` so the operator can Ctrl+A → Ctrl+C as a backup.
          onFocus={(e) => e.currentTarget.select()}
        />
      </div>
    </div>
  )
}

/**
 * `JSON.stringify` choke on BigInt by default. Events flowing through nevo
 * routinely carry bigint timestamps and IDs, so coerce them to a tagged
 * string representation that matches the framework's wire convention.
 */
function bigintReplacer(_key: string, value: unknown): unknown {
  if (typeof value === "bigint") return `${value.toString()}n`
  return value
}
