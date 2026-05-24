import type { Metadata } from "next"
import type { ReactNode } from "react"
import "./globals.css"

export const metadata: Metadata = {
  title: "Nevo Messaging DevTools",
  description: "Live dashboard for @riaskov/nevo-messaging services, methods, ACL, errors and RPS"
}

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <header className="nv-header">
          <div className="nv-brand">
            <span className="nv-logo">⚡</span>
            <span>Nevo Messaging DevTools</span>
          </div>
          <nav>
            <a href="/">Overview</a>
            <a href="/services">Services</a>
            <a href="/methods">Methods</a>
            <a href="/errors">Errors</a>
            <a href="/acl">ACL</a>
            <a href="/circuits">Circuits</a>
            <a href="/traces">Traces</a>
            <a href="/trace">Trace</a>
            <a href="/replay">Replay</a>
            <a href="/config">Config</a>
          </nav>
        </header>
        <main className="nv-main">{children}</main>
      </body>
    </html>
  )
}
