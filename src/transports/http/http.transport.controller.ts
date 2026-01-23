import { Body, Controller, Post, Query, Sse } from "@nestjs/common"
import { Observable, Subject } from "rxjs"
import { map } from "rxjs/operators"
import { DEFAULT_BROADCAST_TOPIC, DEFAULT_DISCOVERY_TOPIC, DEFAULT_SUBSCRIPTION_SUFFIX, stringifyWithBigInt } from "../../common"

type Channel = string

class HttpSseBroker {
  private readonly channels = new Map<Channel, Subject<string>>()

  stream(channel: Channel): Observable<{ data: string }> {
    const subject = this.getChannel(channel)
    return subject.asObservable().pipe(map((data) => ({ data })))
  }

  publish(channel: Channel, payload: unknown) {
    const subject = this.getChannel(channel)
    subject.next(stringifyWithBigInt(payload))
  }

  private getChannel(channel: Channel): Subject<string> {
    let subject = this.channels.get(channel)
    if (!subject) {
      subject = new Subject<string>()
      this.channels.set(channel, subject)
    }
    return subject
  }
}

const broker = new HttpSseBroker()

@Controller()
export class HttpTransportController {
  @Sse(`/${DEFAULT_DISCOVERY_TOPIC}`)
  streamDiscovery(): Observable<{ data: string }> {
    return broker.stream(DEFAULT_DISCOVERY_TOPIC)
  }

  @Post(`/${DEFAULT_DISCOVERY_TOPIC}`)
  publishDiscovery(@Body() payload: any) {
    broker.publish(DEFAULT_DISCOVERY_TOPIC, payload)
    return { ok: true }
  }

  @Sse(`/__nevo/subscribe`)
  streamSubscription(@Query("service") service: string): Observable<{ data: string }> {
    const channel = `${service.toLowerCase()}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    return broker.stream(channel)
  }

  @Post(`/__nevo/publish`)
  publishSubscription(@Body() payload: any) {
    const serviceName = payload?.serviceName
    if (!serviceName) {
      return { ok: false }
    }
    const channel = `${serviceName.toLowerCase()}${DEFAULT_SUBSCRIPTION_SUFFIX}`
    broker.publish(channel, payload)
    return { ok: true }
  }

  @Sse(`/${DEFAULT_BROADCAST_TOPIC}`)
  streamBroadcast(): Observable<{ data: string }> {
    return broker.stream(DEFAULT_BROADCAST_TOPIC)
  }

  @Post(`/${DEFAULT_BROADCAST_TOPIC}`)
  publishBroadcast(@Body() payload: any) {
    broker.publish(DEFAULT_BROADCAST_TOPIC, payload)
    return { ok: true }
  }
}
