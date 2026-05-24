import { NevoHttpClient, NevoHttpClientOptions } from "./nevo-http.client"

export const NEVO_HTTP_CLIENT_TOKEN = "NEVO_HTTP_CLIENT"

export interface HttpClientFactoryOptions extends NevoHttpClientOptions {
  clientIdPrefix: string
}

export const createNevoHttpClient = (serviceUrls: Record<string, string>, options: HttpClientFactoryOptions) => {
  return {
    provide: NEVO_HTTP_CLIENT_TOKEN,
    useFactory: async () => {
      return new NevoHttpClient(serviceUrls, {
        ...options,
        serviceName: options.serviceName || options.clientIdPrefix
      })
    }
  }
}
