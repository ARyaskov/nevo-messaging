import { useCallback, useEffect, useState } from "react"

export interface Location {
  pathname: string
  search: URLSearchParams
}

function currentLocation(): Location {
  return { pathname: window.location.pathname, search: new URLSearchParams(window.location.search) }
}

export function navigate(to: string): void {
  window.history.pushState({}, "", to)
  window.dispatchEvent(new PopStateEvent("popstate"))
}

/** True for plain left-clicks on same-origin links that should stay in the SPA. */
function isInternalNavigation(event: MouseEvent, anchor: HTMLAnchorElement): boolean {
  if (event.defaultPrevented || event.button !== 0) return false
  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return false
  if (anchor.target && anchor.target !== "_self") return false
  if (anchor.hasAttribute("download")) return false
  if (anchor.origin !== window.location.origin) return false
  return true
}

/**
 * Path-based routing over the History API.
 *
 * A document-level click interceptor is what lets every view keep using plain
 * `<a href="/services/user">` markup — no link component, no rewritten hrefs.
 * The server serves index.html for unknown paths, so deep links and reloads work.
 */
export function useLocation(): Location {
  const [location, setLocation] = useState<Location>(currentLocation)

  const sync = useCallback(() => setLocation(currentLocation()), [])

  useEffect(() => {
    const onClick = (event: MouseEvent) => {
      const anchor = (event.target as Element | null)?.closest?.("a")
      if (!anchor || !isInternalNavigation(event, anchor as HTMLAnchorElement)) return
      event.preventDefault()
      const href = (anchor as HTMLAnchorElement).href
      if (href !== window.location.href) {
        window.history.pushState({}, "", href)
        sync()
      }
    }

    document.addEventListener("click", onClick)
    window.addEventListener("popstate", sync)
    return () => {
      document.removeEventListener("click", onClick)
      window.removeEventListener("popstate", sync)
    }
  }, [sync])

  useEffect(() => {
    window.scrollTo(0, 0)
  }, [location.pathname])

  return location
}
