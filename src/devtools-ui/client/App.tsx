import type { ReactNode } from "react"
import { useLocation } from "./router"
import { useRegistry, useSnapshot, type Loadable } from "./lib/use-initial-data"
import { AclInspector } from "./components/AclInspector"
import { CircuitDashboard } from "./components/CircuitDashboard"
import { ConfigEditor } from "./components/ConfigEditor"
import { ErrorsTimeline } from "./components/ErrorsTimeline"
import { LiveDashboard } from "./components/LiveDashboard"
import { MethodsLeaderboard } from "./components/MethodsLeaderboard"
import { ReplayConsole } from "./components/ReplayConsole"
import { ServiceDetail } from "./components/ServiceDetail"
import { ServicesList } from "./components/ServicesList"
import { TraceViewer } from "./components/TraceViewer"
import { TracesView } from "./components/TracesView"

const NAV: { href: string; label: string }[] = [
  { href: "/", label: "Overview" },
  { href: "/services", label: "Services" },
  { href: "/methods", label: "Methods" },
  { href: "/errors", label: "Errors" },
  { href: "/acl", label: "ACL" },
  { href: "/circuits", label: "Circuits" },
  { href: "/traces", label: "Traces" },
  { href: "/trace", label: "Trace" },
  { href: "/replay", label: "Replay" },
  { href: "/config", label: "Config" }
]

/** Renders once the initial snapshot has landed; views need it as `initialEvents`. */
function Loaded<T>({ state, children }: { state: Loadable<T>; children: (data: T) => ReactNode }): ReactNode {
  if (state.error) return <p className="nv-muted">Failed to load: {state.error}</p>
  if (state.data === null) return <p className="nv-muted">Loading…</p>
  return children(state.data)
}

function OverviewPage() {
  const events = useSnapshot(500)
  return (
    <>
      <h1 className="nv-h1">Overview</h1>
      <Loaded state={events}>{(data) => <LiveDashboard initialEvents={data} />}</Loaded>
    </>
  )
}

function ServicesPage() {
  const events = useSnapshot(2000)
  const registry = useRegistry()
  return (
    <>
      <h1 className="nv-h1">Services</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Registered services across all wired controllers. Stats are derived from the last 2 000 events in the bus (≈last few minutes of traffic).
      </p>
      <Loaded state={events}>
        {(evts) => <Loaded state={registry}>{(reg) => <ServicesList initialEvents={evts} initialServices={reg.services} />}</Loaded>}
      </Loaded>
    </>
  )
}

function ServiceDetailPage({ name }: { name: string }) {
  const events = useSnapshot(2000)
  const registry = useRegistry()
  return (
    <>
      <p className="nv-muted">
        <a href="/services">← All services</a>
      </p>
      <Loaded state={events}>
        {(evts) => (
          <Loaded state={registry}>
            {(reg) => {
              const info = reg.services.find((s) => s.serviceName === name)
              if (!info && !evts.some((e) => e.service === name)) {
                return <p className="nv-muted">No service named &quot;{name}&quot; has registered or produced traffic yet.</p>
              }
              return (
                <ServiceDetail
                  serviceName={name}
                  initialEvents={evts}
                  initialServiceInfo={info}
                  initialCircuits={reg.circuits.filter((c) => c.service === name)}
                />
              )
            }}
          </Loaded>
        )}
      </Loaded>
    </>
  )
}

function MethodsPage({ service, method }: { service?: string; method?: string }) {
  const events = useSnapshot(2000)
  return (
    <>
      <h1 className="nv-h1">Methods</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Top-N rankings across the last 2 000 events. Statistics include p50/p95/p99 latency and error rate per (service, method).
      </p>
      <Loaded state={events}>{(data) => <MethodsLeaderboard initialEvents={data} highlightService={service} highlightMethod={method} />}</Loaded>
    </>
  )
}

function ErrorsPage() {
  const events = useSnapshot(2000)
  return (
    <>
      <h1 className="nv-h1">Errors</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Latest failed responses across all services. Filter by service or method, group by error code.
      </p>
      <Loaded state={events}>{(data) => <ErrorsTimeline initialEvents={data} />}</Loaded>
    </>
  )
}

function AclPage() {
  const registry = useRegistry()
  return (
    <>
      <h1 className="nv-h1">ACL Inspector</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Per-service access-control rules. Use the simulator at the bottom to test how a hypothetical caller would be evaluated against a
        service&apos;s rules.
      </p>
      <Loaded state={registry}>{(reg) => <AclInspector initialServices={reg.services} />}</Loaded>
    </>
  )
}

function CircuitsPage() {
  const events = useSnapshot(2000)
  const registry = useRegistry()
  return (
    <>
      <h1 className="nv-h1">Circuit Breakers</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Live state of every client-side circuit breaker. Open circuits short-circuit the next request and return <code>ErrorCode.CIRCUIT_OPEN</code>{" "}
        until the reset window elapses.
      </p>
      <Loaded state={events}>
        {(evts) => <Loaded state={registry}>{(reg) => <CircuitDashboard initialEvents={evts} initialCircuits={reg.circuits} />}</Loaded>}
      </Loaded>
    </>
  )
}

function TracesPage({ chain }: { chain?: string }) {
  const events = useSnapshot(5000)
  return (
    <>
      <h1 className="nv-h1">Traces</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Each row groups every envelope that belongs to one logical fan-out (request → downstream → downstream → response). The framework propagates a{" "}
        <code>chainId</code> via <code>AsyncLocalStorage</code>, so any service that calls another through a Nevo client inherits the same chain
        automatically.
      </p>
      <Loaded state={events}>{(data) => <TracesView initialEvents={data} initialChainId={chain} />}</Loaded>
    </>
  )
}

function TracePage({ uuid }: { uuid?: string }) {
  const events = useSnapshot(2000)
  return (
    <>
      <h1 className="nv-h1">Trace</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Reconstructs an end-to-end trace by joining client + server events with the same <code>uuid</code>. Pair this with W3C{" "}
        <code>traceparent</code> in <code>meta.trace</code> for a fully distributed view.
      </p>
      <Loaded state={events}>{(data) => <TraceViewer initialEvents={data} initialUuid={uuid} />}</Loaded>
    </>
  )
}

function ReplayPage() {
  const registry = useRegistry()
  return (
    <>
      <h1 className="nv-h1">Replay traffic</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Replay a query/emit against a service. The replay is recorded in the DevToolsBus as a <code>custom</code> event with{" "}
        <code>extra.kind = &quot;replay-requested&quot;</code>; a connected gateway can pick it up and actually fire the request.
      </p>
      <Loaded state={registry}>
        {(reg) => <ReplayConsole services={reg.services.map((s) => ({ name: s.serviceName, methods: s.methods.map((m) => m.signalName) }))} />}
      </Loaded>
    </>
  )
}

function ConfigPage() {
  const registry = useRegistry()
  return (
    <>
      <h1 className="nv-h1">Live config</h1>
      <p className="nv-muted" style={{ marginBottom: 16 }}>
        Edit ACL of a registered service at runtime. Changes update the in-process <code>DevToolsRegistry</code> entry, which the signal-router
        decorator reads on the next request.
      </p>
      <Loaded state={registry}>{(reg) => <ConfigEditor services={reg.services} />}</Loaded>
    </>
  )
}

function NotFound({ pathname }: { pathname: string }) {
  return (
    <>
      <h1 className="nv-h1">Not found</h1>
      <p className="nv-muted">
        Nothing is routed at <code>{pathname}</code>. <a href="/">Back to overview</a>.
      </p>
    </>
  )
}

function renderRoute(pathname: string, search: URLSearchParams): ReactNode {
  const serviceMatch = /^\/services\/(.+)$/.exec(pathname)
  if (serviceMatch) return <ServiceDetailPage name={decodeURIComponent(serviceMatch[1]!)} />

  const param = (key: string) => search.get(key) ?? undefined

  switch (pathname.replace(/\/+$/, "") || "/") {
    case "/":
      return <OverviewPage />
    case "/services":
      return <ServicesPage />
    case "/methods":
      return <MethodsPage service={param("service")} method={param("method")} />
    case "/errors":
      return <ErrorsPage />
    case "/acl":
      return <AclPage />
    case "/circuits":
      return <CircuitsPage />
    case "/traces":
      return <TracesPage chain={param("chain")} />
    case "/trace":
      return <TracePage uuid={param("uuid")} />
    case "/replay":
      return <ReplayPage />
    case "/config":
      return <ConfigPage />
    default:
      return <NotFound pathname={pathname} />
  }
}

export function App() {
  const { pathname, search } = useLocation()
  const active = pathname.replace(/\/+$/, "") || "/"

  return (
    <>
      <header className="nv-header">
        <div className="nv-brand">
          <span className="nv-logo">⚡</span>
          <span>Nevo Messaging DevTools</span>
        </div>
        <nav>
          {NAV.map((item) => (
            <a
              key={item.href}
              href={item.href}
              aria-current={item.href === active || (item.href === "/services" && active.startsWith("/services/")) ? "page" : undefined}
            >
              {item.label}
            </a>
          ))}
        </nav>
      </header>
      {/* Keyed so switching routes remounts views instead of reusing their state. */}
      <main className="nv-main" key={`${pathname}?${search.toString()}`}>
        {renderRoute(pathname, search)}
      </main>
    </>
  )
}
