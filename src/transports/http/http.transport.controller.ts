import {
  BadRequestException,
  Body,
  CanActivate,
  Controller,
  ExecutionContext,
  Inject,
  Injectable,
  Optional,
  Post,
  Query,
  Sse,
  UseGuards
} from "@nestjs/common"
import { Observable, Subject } from "rxjs"
import { map } from "rxjs/operators"
import { DEFAULT_BROADCAST_TOPIC, DEFAULT_DISCOVERY_TOPIC, DEFAULT_SUBSCRIPTION_SUFFIX, getCodec, stringifyWithBigInt } from "../../common"

export const HTTP_SSE_BROKER_TOKEN = "NEVO_HTTP_SSE_BROKER"
export const HTTP_TRANSPORT_OPTIONS_TOKEN = "NEVO_HTTP_TRANSPORT_OPTIONS"

export interface HttpTransportControllerOptions {
  authorize?: (req: unknown) => boolean | Promise<boolean>
}

export const createHttpTransportOptionsProvider = (options: HttpTransportControllerOptions) => ({
  provide: HTTP_TRANSPORT_OPTIONS_TOKEN,
  useValue: options
})

@Injectable()
export class HttpTransportAuthGuard implements CanActivate {
  constructor(@Optional() @Inject(HTTP_TRANSPORT_OPTIONS_TOKEN) private readonly options?: HttpTransportControllerOptions) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const authorize = this.options?.authorize
    if (!authorize) return true
    return (await authorize(context.switchToHttp().getRequest())) === true
  }
}

function decodeBinaryPayload(payload: unknown): any {
  if (!(payload instanceof Uint8Array)) return payload
  try {
    return getCodec("msgpack").decode(payload)
  } catch {
    return getCodec("json").decode(payload)
  }
}

// Sentinel pushed periodically so clients can tell a live-but-idle stream from a
// dead (half-open) connection. Empty-data SSE frames are ignored by the client parser.
const SSE_HEARTBEAT = ""
const SSE_HEARTBEAT_MS = 15_000

@Injectable()
export class HttpSseBroker {
  private readonly channels = new Map<string, Subject<string>>()
  private heartbeat?: NodeJS.Timeout

  constructor() {
    this.heartbeat = setInterval(() => {
      for (const subject of this.channels.values()) subject.next(SSE_HEARTBEAT)
    }, SSE_HEARTBEAT_MS)
    if (typeof this.heartbeat.unref === "function") this.heartbeat.unref()
  }

  stream(channel: string): Observable<{ data: string }> {
    return this.getChannel(channel)
      .asObservable()
      .pipe(map((data) => ({ data })))
  }

  publish(channel: string, payload: unknown) {
    this.getChannel(channel).next(stringifyWithBigInt(payload))
  }

  onModuleDestroy(): void {
    if (this.heartbeat) clearInterval(this.heartbeat)
    this.heartbeat = undefined
  }

  private getChannel(channel: string): Subject<string> {
    let subject = this.channels.get(channel)
    if (!subject) {
      subject = new Subject<string>()
      this.channels.set(channel, subject)
    }
    return subject
  }
}

export const createHttpSseBrokerProvider = () => ({
  provide: HTTP_SSE_BROKER_TOKEN,
  useClass: HttpSseBroker
})

@Controller()
@UseGuards(HttpTransportAuthGuard)
export class HttpTransportController {
  constructor(@Optional() @Inject(HTTP_SSE_BROKER_TOKEN) private readonly broker: HttpSseBroker = new HttpSseBroker()) {}

  @Sse(`/${DEFAULT_DISCOVERY_TOPIC}`)
  streamDiscovery(): Observable<{ data: string }> {
    return this.broker.stream(DEFAULT_DISCOVERY_TOPIC)
  }

  @Post(`/${DEFAULT_DISCOVERY_TOPIC}`)
  publishDiscovery(@Body() payload: any) {
    this.broker.publish(DEFAULT_DISCOVERY_TOPIC, decodeBinaryPayload(payload))
    return { ok: true }
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
    const body = decodeBinaryPayload(payload)
    const serviceName = body?.serviceName ?? body?.meta?.headers?.["nevo-service"]
    if (serviceName === undefined || serviceName === null) return { ok: false }
    if (typeof serviceName !== "string" || serviceName.length === 0) {
      throw new BadRequestException('"serviceName" must be a non-empty string')
    }
    const channel = `${serviceName.toLowerCase()}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    this.broker.publish(channel, body)
    return { ok: true }
  }

  @Sse(`/${DEFAULT_BROADCAST_TOPIC}`)
  streamBroadcast(): Observable<{ data: string }> {
    return this.broker.stream(DEFAULT_BROADCAST_TOPIC)
  }

  @Post(`/${DEFAULT_BROADCAST_TOPIC}`)
  publishBroadcast(@Body() payload: any) {
    this.broker.publish(DEFAULT_BROADCAST_TOPIC, decodeBinaryPayload(payload))
    return { ok: true }
  }
}
