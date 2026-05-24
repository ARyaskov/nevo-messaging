import { isProduction } from "./env"

export type NevoLogLevel = "trace" | "debug" | "info" | "warn" | "error" | "fatal"

export interface NevoLogger {
  trace(obj: object | string, msg?: string): void
  debug(obj: object | string, msg?: string): void
  info(obj: object | string, msg?: string): void
  warn(obj: object | string, msg?: string): void
  error(obj: object | string, msg?: string): void
  fatal(obj: object | string, msg?: string): void
  child(bindings: Record<string, unknown>): NevoLogger
  setLevel?(level: NevoLogLevel): void
  isLevelEnabled?(level: NevoLogLevel): boolean
}

export interface NevoLoggerOptions {
  level?: NevoLogLevel
  name?: string
  redact?: string[]
  pretty?: boolean
  base?: Record<string, unknown>
}

class ConsoleLogger implements NevoLogger {
  private readonly bindings: Record<string, unknown>
  private level: NevoLogLevel

  constructor(opts: NevoLoggerOptions = {}) {
    this.level = opts.level || (isProduction() ? "info" : "debug")
    this.bindings = { ...(opts.base || {}) }
    if (opts.name) this.bindings["name"] = opts.name
  }

  private static readonly ORDER: NevoLogLevel[] = ["trace", "debug", "info", "warn", "error", "fatal"]

  private shouldLog(level: NevoLogLevel): boolean {
    return ConsoleLogger.ORDER.indexOf(level) >= ConsoleLogger.ORDER.indexOf(this.level)
  }

  isLevelEnabled(level: NevoLogLevel): boolean {
    return this.shouldLog(level)
  }

  private emit(level: NevoLogLevel, obj: object | string, msg?: string): void {
    if (!this.shouldLog(level)) return
    const payload = typeof obj === "string" ? { msg: obj } : { ...obj, ...(msg ? { msg } : {}) }
    const merged = { level, time: Date.now(), ...this.bindings, ...payload }
    const stream = level === "error" || level === "fatal" ? console.error : level === "warn" ? console.warn : console.log
    stream(JSON.stringify(merged))
  }

  trace(o: object | string, m?: string) { this.emit("trace", o, m) }
  debug(o: object | string, m?: string) { this.emit("debug", o, m) }
  info(o: object | string, m?: string) { this.emit("info", o, m) }
  warn(o: object | string, m?: string) { this.emit("warn", o, m) }
  error(o: object | string, m?: string) { this.emit("error", o, m) }
  fatal(o: object | string, m?: string) { this.emit("fatal", o, m) }

  child(bindings: Record<string, unknown>): NevoLogger {
    const c = new ConsoleLogger({ level: this.level })
    Object.assign((c as any).bindings, this.bindings, bindings)
    return c
  }

  setLevel(level: NevoLogLevel) { this.level = level }
}

function tryPino(opts: NevoLoggerOptions): NevoLogger | null {
  try {
    const pino = require("pino")
    const pinoLogger = pino({
      name: opts.name,
      level: opts.level || (isProduction() ? "info" : "debug"),
      base: opts.base,
      redact: opts.redact,
      ...(opts.pretty && !isProduction()
        ? { transport: { target: "pino-pretty", options: { colorize: true, translateTime: "SYS:standard" } } }
        : {})
    })
    return wrapPino(pinoLogger)
  } catch {
    return null
  }
}

function wrapPino(pino: any): NevoLogger {
  return {
    trace: (o, m) => pino.trace(o, m),
    debug: (o, m) => pino.debug(o, m),
    info: (o, m) => pino.info(o, m),
    warn: (o, m) => pino.warn(o, m),
    error: (o, m) => pino.error(o, m),
    fatal: (o, m) => pino.fatal(o, m),
    child: (b) => wrapPino(pino.child(b)),
    setLevel: (lvl) => { pino.level = lvl },
    isLevelEnabled: (lvl) => typeof pino.isLevelEnabled === "function" ? pino.isLevelEnabled(lvl) : true
  }
}

let defaultLogger: NevoLogger | null = null

export function createLogger(opts: NevoLoggerOptions = {}): NevoLogger {
  const pinoLogger = tryPino({
    level: opts.level,
    name: opts.name || "nevo",
    base: opts.base,
    redact: opts.redact ?? ["*.auth.token", "*.password", "*.secret", "meta.auth.token"],
    pretty: opts.pretty ?? !isProduction()
  })
  return pinoLogger || new ConsoleLogger(opts)
}

export function getDefaultLogger(): NevoLogger {
  if (!defaultLogger) {
    defaultLogger = createLogger({ name: "nevo" })
  }
  return defaultLogger
}

export function setDefaultLogger(logger: NevoLogger): void {
  defaultLogger = logger
}
