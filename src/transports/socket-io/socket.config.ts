import { NevoSocketClient, NevoSocketClientOptions } from "./nevo-socket.client"

export interface SocketClientFactoryOptions extends NevoSocketClientOptions {
  clientIdPrefix: string
}

export const createNevoSocketClient = (serviceUrls: Record<string, string>, options: SocketClientFactoryOptions) => {
  return {
    provide: "NEVO_SOCKET_CLIENT",
    useFactory: async () => {
      return new NevoSocketClient(serviceUrls, {
        ...options,
        serviceName: options.serviceName || options.clientIdPrefix
      })
    }
  }
}
