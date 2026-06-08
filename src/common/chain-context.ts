import { AsyncLocalStorage } from "node:async_hooks"
import { uuidv7 } from "./uuid"

/** Per-call context propagated across async boundaries to share a chain id. */
export interface ChainContext {
  chainId: string
  /** Envelope uuid of the inbound message that triggered this handler, if any. */
  parentUuid?: string
}

const als = new AsyncLocalStorage<ChainContext>()

const MAX_CHAIN_ID_LEN = 64

const CHAIN_ID_RE = /^[A-Za-z0-9._:-]+$/

/** True when an inbound chain id is a sane, bounded token we can trust. */
export function isValidChainId(id: string): boolean {
  return id.length > 0 && id.length <= MAX_CHAIN_ID_LEN && CHAIN_ID_RE.test(id)
}

/** Generate a fresh chain identifier (UUIDv7). */
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

/** Run `fn` within a chain context; every async descendant inherits it. */
export function runInChain<T>(ctx: ChainContext, fn: () => T): T {
  return als.run(ctx, fn)
}

/** Establish a chain id for an inbound message: honor caller, else inherit, else mint. */
export function resolveInboundChainId(metaChainId: unknown): string {
  // Inbound id is attacker-controlled; honor only a valid, bounded token.
  if (typeof metaChainId === "string" && isValidChainId(metaChainId)) return metaChainId
  const current = getCurrentChainId()
  if (current) return current
  return newChainId()
}

/** Pick a chain id for an outbound envelope: explicit override, else inherit, else mint. */
export function resolveOutboundChainId(explicit?: string): string {
  if (typeof explicit === "string" && explicit.length > 0) return explicit
  const current = getCurrentChainId()
  if (current) return current
  return newChainId()
}

/** Reveal the underlying AsyncLocalStorage for tests / advanced callers. */
export function getChainStorage(): AsyncLocalStorage<ChainContext> {
  return als
}
