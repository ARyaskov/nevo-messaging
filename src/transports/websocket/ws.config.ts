import { NevoWsClient, NevoWsClientOptions } from "./nevo-ws.client"

export const NEVO_WS_CLIENT_TOKEN = "NEVO_WS_CLIENT"

export interface WsClientFactoryOptions extends NevoWsClientOptions {
  clientIdPrefix: string
}

export const createNevoWsClient = (serviceUrls: Record<string, string>, options: WsClientFactoryOptions) => {
  return {
    provide: NEVO_WS_CLIENT_TOKEN,
    useFactory: async () => {
      return new NevoWsClient(serviceUrls, {
        ...options,
        serviceName: options.serviceName || options.clientIdPrefix
      })
    }
  }
}
