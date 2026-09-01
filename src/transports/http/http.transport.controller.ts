import {
  BadRequestException,
  Body,
  CanActivate,
  Controller,
  ExecutionContext,
  ForbiddenException,
  Inject,
  Injectable,
  Optional,
  Post,
  Query,
  ServiceUnavailableException,
  Sse,
  UseGuards
} from "@nestjs/common"
import { Observable, Subject } from "rxjs"
import { map } from "rxjs/operators"
import {
  DEFAULT_BROADCAST_TOPIC,
  DEFAULT_DISCOVERY_TOPIC,
  DEFAULT_SUBSCRIPTION_SUFFIX,
  DEFAULT_MAX_PAYLOAD_BYTES,
  enforcePayloadLimit,
  getCodec,
  stringifyWithBigInt
} from "../../common"

export const HTTP_SSE_BROKER_TOKEN = "NEVO_HTTP_SSE_BROKER"
export const HTTP_TRANSPORT_OPTIONS_TOKEN = "NEVO_HTTP_TRANSPORT_OPTIONS"

export const DEFAULT_MAX_SSE_CHANNELS = 1024

export interface HttpTransportControllerOptions {
  authorize?: (req: unknown) => boolean | Promise<boolean>
  /** Allow unauthenticated access. Only safe on a trusted network. */
  insecure?: boolean
  maxPayloadBytes?: number
}

export const createHttpTransportOptionsProvider = (options: HttpTransportControllerOptions) => ({
  provide: HTTP_TRANSPORT_OPTIONS_TOKEN,
  useValue: options
})

/** Fails closed: the endpoints behind this guard are message-injection surfaces. */
@Injectable()
export class HttpTransportAuthGuard implements CanActivate {
  constructor(@Optional() @Inject(HTTP_TRANSPORT_OPTIONS_TOKEN) private readonly options?: HttpTransportControllerOptions) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const authorize = this.options?.authorize
    if (authorize) {
      return (await authorize(context.switchToHttp().getRequest())) === true
    }
    if (this.options?.insecure === true) return true
    throw new ForbiddenException(
      "Nevo HTTP transport endpoints are unauthorized: register createHttpTransportOptionsProvider({ authorize }) " +
        "to authenticate callers, or { insecure: true } to explicitly allow unauthenticated access on a trusted network."
    )
  }
}

function decodeBinaryPayload(payload: unknown, maxPayloadBytes: number): any {
  if (!(payload instanceof Uint8Array)) return payload
  // Cap before the codec can pre-allocate from a crafted header.
  enforcePayloadLimit(payload, maxPayloadBytes)
  try {
    return getCodec("msgpack").decode(payload)
  } catch {
    return getCodec("json").decode(payload)
  }
}

const SSE_HEARTBEAT = ""
const SSE_HEARTBEAT_MS = 15_000

interface BrokerChannel {
  subject: Subject<string>
  subscribers: number
}

export interface HttpSseBrokerOptions {
  maxChannels?: number
}

/** Channel names come from request input, so channels are reference-counted. */
@Injectable()
export class HttpSseBroker {
  private readonly channels = new Map<string, BrokerChannel>()
  private readonly maxChannels: number
  private heartbeat?: NodeJS.Timeout

  constructor(@Optional() opts?: HttpSseBrokerOptions) {
    this.maxChannels = Math.max(1, opts?.maxChannels ?? DEFAULT_MAX_SSE_CHANNELS)
    this.heartbeat = setInterval(() => {
      for (const channel of this.channels.values()) {
        if (channel.subscribers > 0) channel.subject.next(SSE_HEARTBEAT)
      }
    }, SSE_HEARTBEAT_MS)
    if (typeof this.heartbeat.unref === "function") this.heartbeat.unref()
  }

  stream(channel: string): Observable<{ data: string }> {
    this.assertCapacity(channel)
    return new Observable<string>((subscriber) => {
      const entry = this.acquire(channel)
      const inner = entry.subject.subscribe(subscriber)
      return () => {
        inner.unsubscribe()
        this.release(channel)
      }
    }).pipe(map((data) => ({ data })))
  }

  publish(channel: string, payload: unknown): boolean {
    const entry = this.channels.get(channel)
    if (!entry || entry.subscribers === 0) return false
    entry.subject.next(stringifyWithBigInt(payload))
    return true
  }

  channelCount(): number {
    return this.channels.size
  }

  onModuleDestroy(): void {
    if (this.heartbeat) clearInterval(this.heartbeat)
    this.heartbeat = undefined
    for (const entry of this.channels.values()) entry.subject.complete()
    this.channels.clear()
  }

  private assertCapacity(channel: string): void {
    if (this.channels.has(channel)) return
    if (this.channels.size >= this.maxChannels) {
      throw new ServiceUnavailableException(`SSE channel limit reached (${this.maxChannels}); refusing to open "${channel}"`)
    }
  }

  private acquire(channel: string): BrokerChannel {
    let entry = this.channels.get(channel)
    if (!entry) {
      this.assertCapacity(channel)
      entry = { subject: new Subject<string>(), subscribers: 0 }
      this.channels.set(channel, entry)
    }
    entry.subscribers++
    return entry
  }

  private release(channel: string): void {
    const entry = this.channels.get(channel)
    if (!entry) return
    entry.subscribers--
    if (entry.subscribers <= 0) {
      this.channels.delete(channel)
      entry.subject.complete()
    }
  }
}

export const createHttpSseBrokerProvider = (options?: HttpSseBrokerOptions) => ({
  provide: HTTP_SSE_BROKER_TOKEN,
  useFactory: () => new HttpSseBroker(options)
})

let fallbackBroker: HttpSseBroker | null = null

function getFallbackBroker(): HttpSseBroker {
  if (!fallbackBroker) fallbackBroker = new HttpSseBroker()
  return fallbackBroker
}

export function resetHttpSseFallbackBroker(): void {
  if (!fallbackBroker) return
  fallbackBroker.onModuleDestroy()
  fallbackBroker = null
}

@Controller()
@UseGuards(HttpTransportAuthGuard)
export class HttpTransportController {
  private readonly broker: HttpSseBroker
  private readonly ownsBroker: boolean
  private readonly maxPayloadBytes: number

  constructor(
    @Optional() @Inject(HTTP_SSE_BROKER_TOKEN) broker?: HttpSseBroker,
    @Optional() @Inject(HTTP_TRANSPORT_OPTIONS_TOKEN) options?: HttpTransportControllerOptions
  ) {
    this.broker = broker ?? getFallbackBroker()
    this.ownsBroker = !broker
    this.maxPayloadBytes = options?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES
  }

  onModuleDestroy(): void {
    if (this.ownsBroker) resetHttpSseFallbackBroker()
  }

  @Sse(`/${DEFAULT_DISCOVERY_TOPIC}`)
  streamDiscovery(): Observable<{ data: string }> {
    return this.broker.stream(DEFAULT_DISCOVERY_TOPIC)
  }

  @Post(`/${DEFAULT_DISCOVERY_TOPIC}`)
  publishDiscovery(@Body() payload: any) {
    const delivered = this.broker.publish(DEFAULT_DISCOVERY_TOPIC, decodeBinaryPayload(payload, this.maxPayloadBytes))
    return { ok: true, delivered }
  }

  @Sse(`/__nevo/subscribe`)
  streamSubscription(@Query("service") service: string): Observable<{ data: string }> {
    if (typeof service !== "string" || service.length === 0) {
      throw new BadRequestException('Query parameter "service" must be a non-empty string')
    }
    const channel = `${service.toLowerCase()}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    return this.broker.stream(channel)
  }

  @Post(`/__nevo/publish`)
  publishSubscription(@Body() payload: any) {
    const body = decodeBinaryPayload(payload, this.maxPayloadBytes)
    const serviceName = body?.serviceName ?? body?.meta?.headers?.["nevo-service"]
    if (serviceName === undefined || serviceName === null) return { ok: false, delivered: false }
    if (typeof serviceName !== "string" || serviceName.length === 0) {
      throw new BadRequestException('"serviceName" must be a non-empty string')
    }
    const channel = `${serviceName.toLowerCase()}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    const delivered = this.broker.publish(channel, body)
    return { ok: true, delivered }
  }

  @Sse(`/${DEFAULT_BROADCAST_TOPIC}`)
  streamBroadcast(): Observable<{ data: string }> {
    return this.broker.stream(DEFAULT_BROADCAST_TOPIC)
  }

  @Post(`/${DEFAULT_BROADCAST_TOPIC}`)
  publishBroadcast(@Body() payload: any) {
    const delivered = this.broker.publish(DEFAULT_BROADCAST_TOPIC, decodeBinaryPayload(payload, this.maxPayloadBytes))
    return { ok: true, delivered }
  }
}
