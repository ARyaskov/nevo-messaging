export interface CqrsLikeBus {
  execute?(command: unknown): Promise<unknown>
  publish?(event: unknown): Promise<void> | void
}

/** Disposer returned by attach* — call to restore the original bus method. */
export type CqrsDetach = () => void

const ATTACHED_COMMAND = Symbol.for("nevo.cqrs.attachedCommand")
const ATTACHED_EVENT = Symbol.for("nevo.cqrs.attachedEvent")

const NOOP_DETACH: CqrsDetach = () => {}

export interface CqrsBridgeOptions {
  service: string
  client: { query: (svc: string, method: string, params: unknown) => Promise<unknown>; emit: (svc: string, method: string, params: unknown) => Promise<void> }
  remoteCommands?: string[]
  remoteEvents?: string[]
  commandKey?: (cmd: unknown) => string
  eventKey?: (evt: unknown) => string
}

export class CqrsBridge {
  constructor(private readonly opts: CqrsBridgeOptions) {}

  private cmdName(cmd: unknown): string {
    if (this.opts.commandKey) return this.opts.commandKey(cmd)
    return (cmd as any)?.constructor?.name ?? "UnknownCommand"
  }

  private evtName(evt: unknown): string {
    if (this.opts.eventKey) return this.opts.eventKey(evt)
    return (evt as any)?.constructor?.name ?? "UnknownEvent"
  }

  async executeRemote(command: unknown): Promise<unknown> {
    const name = this.cmdName(command)
    return this.opts.client.query(this.opts.service, name, command)
  }

  async publishRemote(event: unknown): Promise<void> {
    const name = this.evtName(event)
    await this.opts.client.emit(this.opts.service, name, event)
  }

  shouldForwardCommand(command: unknown): boolean {
    if (!this.opts.remoteCommands) return false
    return this.opts.remoteCommands.includes(this.cmdName(command))
  }

  shouldForwardEvent(event: unknown): boolean {
    if (!this.opts.remoteEvents) return false
    return this.opts.remoteEvents.includes(this.evtName(event))
  }

  attachToCommandBus(bus: CqrsLikeBus): CqrsDetach {
    if (!bus.execute) return NOOP_DETACH
    // Idempotent: a second attach must not double-wrap execute().
    if ((bus as any)[ATTACHED_COMMAND]) return NOOP_DETACH

    // Keep the *exact* original reference so the disposer restores it verbatim;
    // invoke it via `.call(bus, …)` to preserve the receiver for method-style
    // buses.
    const original = bus.execute
    const wrapped: NonNullable<CqrsLikeBus["execute"]> = (command: unknown) => {
      if (this.shouldForwardCommand(command)) return this.executeRemote(command)
      return original.call(bus, command)
    }
    bus.execute = wrapped
    ;(bus as any)[ATTACHED_COMMAND] = true

    return () => {
      // Only restore if our wrapper is still the active one (avoid clobbering a
      // later wrapper installed on top of ours).
      if (bus.execute === wrapped) bus.execute = original
      delete (bus as any)[ATTACHED_COMMAND]
    }
  }

  attachToEventBus(bus: CqrsLikeBus): CqrsDetach {
    if (!bus.publish) return NOOP_DETACH
    // Idempotent: a second attach must not double-wrap publish().
    if ((bus as any)[ATTACHED_EVENT]) return NOOP_DETACH

    // Keep the *exact* original reference so the disposer can restore it
    // verbatim (a `.bind()` copy would not be reference-equal to what the
    // caller installed). The wrapper invokes it via `.call(bus, …)` so the
    // receiver is still preserved when the underlying publish is a method.
    const original = bus.publish
    // Not declared `async`: the non-forwarded path returns `original(event)`
    // verbatim, preserving whether the underlying publish is sync (void) or
    // async (Promise) rather than silently turning a sync publish into a
    // fire-and-forget async call.
    const wrapped: NonNullable<CqrsLikeBus["publish"]> = (event: unknown) => {
      if (this.shouldForwardEvent(event)) return this.publishRemote(event)
      return original.call(bus, event)
    }
    bus.publish = wrapped
    ;(bus as any)[ATTACHED_EVENT] = true

    return () => {
      if (bus.publish === wrapped) bus.publish = original
      delete (bus as any)[ATTACHED_EVENT]
    }
  }
}
