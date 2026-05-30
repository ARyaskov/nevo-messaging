import { AsyncLocalStorage } from "node:async_hooks"
import { uuidv7 } from "./uuid"

/**
 * Per-call context propagated implicitly across async boundaries so an inbound
 * handler and any outbound calls it makes share the same "chain id".
 *
 *   ┌──────────┐  query     ┌──────────┐  query     ┌──────────┐
 *   │ frontend │ ─────────► │   user   │ ─────────► │ contract │
 *   │          │            │ handler  │            │ handler  │
 *   │ chain=X  │            │ chain=X  │            │ chain=X  │
 *   └──────────┘            └──────────┘            └──────────┘
 *
 * Every Nevo client picks up the active chain id when building outbound meta;
 * every signal-router establishes the chain id from inbound meta before
 * running the user handler. The result is a per-conversation correlation key
 * that lets DevTools reconstruct the full request fan-out without requiring a
 * full OpenTelemetry deployment.
 */
export interface ChainContext {
  chainId: string
  /**
   * The envelope uuid of the inbound message that triggered this handler, if
   * any. Outbound calls inside the handler use this as a "parent" pointer so
   * the DevTools view can render a tree rather than a flat list.
   */
  parentUuid?: string
}

const als = new AsyncLocalStorage<ChainContext>()

/**
 * Upper bound on an inbound chain id we are willing to honor. A UUIDv7 is 36
 * chars; 64 leaves room for prefixed / trace-style correlation ids from other
 * systems while still capping memory and log-injection blast radius.
 */
const MAX_CHAIN_ID_LEN = 64

/**
 * Characters allowed in an inbound chain id. Permissive enough for UUIDs,
 * OpenTelemetry trace ids and common prefixed ids (`-` `_` `.` `:`), but no
 * whitespace, control chars or other content an attacker could smuggle into
 * logs / dashboards.
 */
const CHAIN_ID_RE = /^[A-Za-z0-9._:-]+$/

/** True when an inbound chain id is a sane, bounded token we can trust. */
export function isValidChainId(id: string): boolean {
  return id.length > 0 && id.length <= MAX_CHAIN_ID_LEN && CHAIN_ID_RE.test(id)
}

/**
 * Generate a fresh chain identifier. UUIDv7 is used so chain ids embed a
 * timestamp and sort naturally by start time when grouped in the dashboard.
 */
export function newChainId(): string {
  return uuidv7()
}

/** Return the active ChainContext, or undefined when no handler is running. */
export function getCurrentChainContext(): ChainContext | undefined {
  return als.getStore()
}

/** Convenience: just the chain id (omits parent-uuid plumbing). */
export function getCurrentChainId(): string | undefined {
  return als.getStore()?.chainId
}

/**
 * Run `fn` within a chain context. Every `await`-descendant of `fn`
 * (handlers, microtasks, timers via async hooks) sees this context.
 */
export function runInChain<T>(ctx: ChainContext, fn: () => T): T {
  return als.run(ctx, fn)
}

/**
 * Establish a chain id for a freshly-arrived inbound message:
 *   1. Honor the chain id sent by the caller, if present.
 *   2. Otherwise inherit from the current ALS context (rare — usually only
 *      relevant in tests that nest handlers in the same process).
 *   3. Otherwise mint a new chain id (this is the entry-point of a chain).
 */
export function resolveInboundChainId(metaChainId: unknown): string {
  // The inbound chain id is attacker-controlled, so only honor it when it is a
  // sane, bounded token (see isValidChainId). An over-long or junk value is
  // discarded in favor of inheriting from ALS or minting a fresh id.
  if (typeof metaChainId === "string" && isValidChainId(metaChainId)) return metaChainId
  const current = getCurrentChainId()
  if (current) return current
  return newChainId()
}

/**
 * Pick a chain id for an outbound envelope:
 *   1. Explicit caller override wins (rare — usually you let the runtime pick).
 *   2. Inherit from the ALS context if we're inside a handler (this is the
 *      common path that links A → B → C).
 *   3. Mint a new chain id when no caller context exists (start of a chain).
 */
export function resolveOutboundChainId(explicit?: string): string {
  if (typeof explicit === "string" && explicit.length > 0) return explicit
  const current = getCurrentChainId()
  if (current) return current
  return newChainId()
}

/**
 * Reveal the underlying AsyncLocalStorage so unit tests / advanced callers
 * can `als.disable()` or compose with their own propagation.
 */
export function getChainStorage(): AsyncLocalStorage<ChainContext> {
  return als
}
