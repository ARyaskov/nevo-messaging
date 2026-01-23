import { NevoHttpClient, NevoHttpClientOptions } from "./nevo-http.client"

export interface HttpClientFactoryOptions extends NevoHttpClientOptions {
  clientIdPrefix: string
}

export const createNevoHttpClient = (serviceUrls: Record<string, string>, options: HttpClientFactoryOptions) => {
  return {
    provide: "NEVO_HTTP_CLIENT",
    useFactory: async () => {
      return new NevoHttpClient(serviceUrls, {
        ...options,
        serviceName: options.serviceName || options.clientIdPrefix
      })
    }
  }
}
