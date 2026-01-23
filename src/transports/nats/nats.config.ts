import { NevoNatsClient, NevoNatsClientOptions } from "./nevo-nats.client"

export interface NatsClientFactoryOptions extends NevoNatsClientOptions {
  clientIdPrefix: string
}

export const createNevoNatsClient = (serviceNames: string[], options: NatsClientFactoryOptions) => {
  return {
    provide: "NEVO_NATS_CLIENT",
    useFactory: async () => {
      return NevoNatsClient.create(serviceNames, {
        ...options,
        serviceName: options.serviceName || options.clientIdPrefix
      })
    }
  }
}
