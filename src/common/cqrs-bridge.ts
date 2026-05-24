export interface CqrsLikeBus {
  execute?(command: unknown): Promise<unknown>
  publish?(event: unknown): Promise<void> | void
}

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

  attachToCommandBus(bus: CqrsLikeBus): void {
    if (!bus.execute) return
    const original = bus.execute.bind(bus)
    bus.execute = async (command: unknown) => {
      if (this.shouldForwardCommand(command)) return this.executeRemote(command)
      return original(command)
    }
  }

  attachToEventBus(bus: CqrsLikeBus): void {
    if (!bus.publish) return
    const original = bus.publish.bind(bus)
    bus.publish = async (event: unknown) => {
      if (this.shouldForwardEvent(event)) {
        await this.publishRemote(event)
        return
      }
      return original(event)
    }
  }
}
