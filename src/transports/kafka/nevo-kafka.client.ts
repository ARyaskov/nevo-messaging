import { ClientKafka } from "@nestjs/microservices"
import { lastValueFrom, timeout, TimeoutError } from "rxjs"
import { MessagingError, ErrorCode, stringifyWithBigInt } from "../../common"
import { randomUUID } from "node:crypto"

export interface NevoKafkaClientOptions {
  timeoutMs?: number
  debug?: boolean
}

export class NevoKafkaClient {
  private readonly kafkaClient: ClientKafka
  private readonly serviceNames: string[]
  private readonly timeoutMs: number
  private readonly debug: boolean

  constructor(kafkaClient: ClientKafka, serviceNames: string[], options?: NevoKafkaClientOptions) {
    this.kafkaClient = kafkaClient
    this.serviceNames = serviceNames.map((name) => name.toLowerCase())
    this.timeoutMs = options?.timeoutMs || 20000
    this.debug = options?.debug || false

    this.serviceNames.forEach((serviceName) => {
      const topicName = `${serviceName}-events`
      const replyTopicName = `${topicName}.reply`

      this.kafkaClient.subscribeToResponseOf(topicName)
      this.kafkaClient.subscribeToResponseOf(replyTopicName)
    })
  }

  private createMessagePayload(method: string, params: any) {
    const uuid = randomUUID()
    return {
      key: uuid,
      value: stringifyWithBigInt({ uuid, method, params })
    }
  }

  async query<T = any>(serviceName: string, method: string, params: any): Promise<T> {
    const normalizedServiceName = serviceName.toLowerCase()

    if (!this.serviceNames.includes(normalizedServiceName)) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Service "${serviceName}" is not registered in nevo kafka client`,
        availableServices: this.serviceNames
      })
    }

    const topicName = `${normalizedServiceName}-events`
    const payload = this.createMessagePayload(method, params)

    if (this.debug) {
      console.log(`[NevoKafkaClient] Sending query to ${topicName}:`, { method, params })
    }

    try {
      const response = await lastValueFrom(this.kafkaClient.send<any>(topicName, payload).pipe(timeout(this.timeoutMs)))

      if (response?.params?.result === "error" && response?.params?.error) {
        const errorData = response.params.error
        const error = new MessagingError(errorData.code, errorData.details, errorData.service || serviceName)

        if (process.env["MODE"] !== "production" && errorData.stack) {
          error.stack = errorData.stack
        }

        throw error
      }

      return response?.params?.result as T
    } catch (error) {
      if (error instanceof TimeoutError) {
        console.error(`Kafka request timed out after ${this.timeoutMs}ms`)
        throw new MessagingError(ErrorCode.UNKNOWN, {
          message: `Request to ${serviceName}.${method} timed out after ${this.timeoutMs}ms`
        })
      }
      throw error
    }
  }

  async emit(serviceName: string, method: string, params: any): Promise<void> {
    const normalizedServiceName = serviceName.toLowerCase()

    if (!this.serviceNames.includes(normalizedServiceName)) {
      throw new MessagingError(ErrorCode.UNKNOWN, {
        message: `Service "${serviceName}" is not registered in nevo kafka client`,
        availableServices: this.serviceNames
      })
    }

    const topicName = `${normalizedServiceName}-events`
    const payload = this.createMessagePayload(method, params)

    if (this.debug) {
      console.log(`[NevoKafkaClient] Emitting to ${topicName}:`, { method, params })
    }

    try {
      this.kafkaClient.emit(topicName, payload)
    } catch (error) {
      console.error(`Failed to emit event to ${serviceName}.${method}:`, error)
      throw error
    }
  }

  getAvailableServices(): string[] {
    return [...this.serviceNames]
  }
}
