import { Body, Controller, Inject, Injectable, Optional, Post, Query, Sse } from "@nestjs/common"
import { Observable, Subject } from "rxjs"
import { map } from "rxjs/operators"
import { DEFAULT_BROADCAST_TOPIC, DEFAULT_DISCOVERY_TOPIC, DEFAULT_SUBSCRIPTION_SUFFIX, stringifyWithBigInt } from "../../common"

export const HTTP_SSE_BROKER_TOKEN = "NEVO_HTTP_SSE_BROKER"

@Injectable()
export class HttpSseBroker {
  private readonly channels = new Map<string, Subject<string>>()

  stream(channel: string): Observable<{ data: string }> {
    return this.getChannel(channel).asObservable().pipe(map((data) => ({ data })))
  }

  publish(channel: string, payload: unknown) {
    this.getChannel(channel).next(stringifyWithBigInt(payload))
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
export class HttpTransportController {
  constructor(@Optional() @Inject(HTTP_SSE_BROKER_TOKEN) private readonly broker: HttpSseBroker = new HttpSseBroker()) {}

  @Sse(`/${DEFAULT_DISCOVERY_TOPIC}`)
  streamDiscovery(): Observable<{ data: string }> {
    return this.broker.stream(DEFAULT_DISCOVERY_TOPIC)
  }

  @Post(`/${DEFAULT_DISCOVERY_TOPIC}`)
  publishDiscovery(@Body() payload: any) {
    this.broker.publish(DEFAULT_DISCOVERY_TOPIC, payload)
    return { ok: true }
  }

  @Sse(`/__nevo/subscribe`)
  streamSubscription(@Query("service") service: string): Observable<{ data: string }> {
    const channel = `${service.toLowerCase()}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    return this.broker.stream(channel)
  }

  @Post(`/__nevo/publish`)
  publishSubscription(@Body() payload: any) {
    const serviceName = payload?.serviceName ?? payload?.meta?.headers?.["nevo-service"]
    if (!serviceName) return { ok: false }
    const channel = `${serviceName.toLowerCase()}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    this.broker.publish(channel, payload)
    return { ok: true }
  }

  @Sse(`/${DEFAULT_BROADCAST_TOPIC}`)
  streamBroadcast(): Observable<{ data: string }> {
    return this.broker.stream(DEFAULT_BROADCAST_TOPIC)
  }

  @Post(`/${DEFAULT_BROADCAST_TOPIC}`)
  publishBroadcast(@Body() payload: any) {
    this.broker.publish(DEFAULT_BROADCAST_TOPIC, payload)
    return { ok: true }
  }
}
