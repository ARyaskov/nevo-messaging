import { Transport } from "@nestjs/microservices"
import { Partitioners } from "kafkajs"
import { NevoKafkaClient } from "./nevo-kafka.client"
import { DEFAULT_BROADCAST_TOPIC, DEFAULT_DISCOVERY_TOPIC, DEFAULT_SUBSCRIPTION_SUFFIX } from "../../common"

export interface KafkaClientFactoryOptions {
  clientIdPrefix: string
  groupIdPrefix?: string
  serviceName?: string
  sessionTimeout?: number
  allowAutoTopicCreation?: boolean
  retryAttempts?: number
  brokerRetryTimeout?: number
  kafkaHost?: string
  kafkaPort?: string
  debug?: boolean
  authToken?: string
  discovery?: {
    enabled?: boolean
    heartbeatIntervalMs?: number
    ttlMs?: number
  }
}

async function createKafkaTopics(serviceNames: string[], options: KafkaClientFactoryOptions): Promise<void> {
  const host = process.env[options.kafkaHost || "KAFKA_HOST"] || "localhost"
  const port = parseInt(process.env[options.kafkaPort || "KAFKA_PORT"] || "9092")

  const { Kafka } = await import("kafkajs")
  const kafka = new Kafka({
    clientId: `${options.clientIdPrefix}-admin`,
    brokers: [`${host}:${port}`],
    connectionTimeout: 10000,
    requestTimeout: 30000,
    retry: {
      retries: 5,
      initialRetryTime: 300,
      maxRetryTime: 30000
    }
  })

  const admin = kafka.admin()

  try {
    await admin.connect()
    console.log(`[KafkaAdmin] Connected to Kafka broker at ${host}:${port}`)

    const existingTopics = await admin.listTopics()
    console.log(`[KafkaAdmin] Existing topics:`, existingTopics)

    const topicsToCreate = serviceNames.flatMap((serviceName) => {
      const normalizedServiceName = serviceName.toLowerCase()
      const eventsTopic = `${normalizedServiceName}-events`
      const replyTopic = `${eventsTopic}.reply`
      const subscriptionTopic = `${normalizedServiceName}${DEFAULT_SUBSCRIPTION_SUFFIX}`

      const topics = []

      if (!existingTopics.includes(eventsTopic)) {
        topics.push({
          topic: eventsTopic,
          numPartitions: 3,
          replicationFactor: 1,
          configEntries: [
            { name: "cleanup.policy", value: "delete" },
            { name: "retention.ms", value: "86400000" }, // 24 hours
            { name: "max.message.bytes", value: "1000012" }
          ]
        })
      }

      if (!existingTopics.includes(replyTopic)) {
        topics.push({
          topic: replyTopic,
          numPartitions: 3,
          replicationFactor: 1,
          configEntries: [
            { name: "cleanup.policy", value: "delete" },
            { name: "retention.ms", value: "3600000" }, // 1 hour for reply topics
            { name: "max.message.bytes", value: "1000012" }
          ]
        })
      }

      if (!existingTopics.includes(subscriptionTopic)) {
        topics.push({
          topic: subscriptionTopic,
          numPartitions: 3,
          replicationFactor: 1,
          configEntries: [
            { name: "cleanup.policy", value: "delete" },
            { name: "retention.ms", value: "86400000" },
            { name: "max.message.bytes", value: "1000012" }
          ]
        })
      }

      return topics
    })

    if (!existingTopics.includes(DEFAULT_BROADCAST_TOPIC)) {
      topicsToCreate.push({
        topic: DEFAULT_BROADCAST_TOPIC,
        numPartitions: 3,
        replicationFactor: 1,
        configEntries: [
          { name: "cleanup.policy", value: "delete" },
          { name: "retention.ms", value: "86400000" },
          { name: "max.message.bytes", value: "1000012" }
        ]
      })
    }

    if (!existingTopics.includes(DEFAULT_DISCOVERY_TOPIC)) {
      topicsToCreate.push({
        topic: DEFAULT_DISCOVERY_TOPIC,
        numPartitions: 1,
        replicationFactor: 1,
        configEntries: [
          { name: "cleanup.policy", value: "compact" },
          { name: "retention.ms", value: "86400000" }
        ]
      })
    }

    if (topicsToCreate.length > 0) {
      console.log(
        `[KafkaAdmin] Creating ${topicsToCreate.length} topics:`,
        topicsToCreate.map((t) => t.topic)
      )

      await admin.createTopics({
        topics: topicsToCreate,
        waitForLeaders: true,
        timeout: 30000
      })

      console.log(
        `[KafkaAdmin] Successfully created topics:`,
        topicsToCreate.map((t) => t.topic)
      )

      // Wait a bit for topics to be fully initialized
      await new Promise((resolve) => setTimeout(resolve, 2000))

      // Verify topics were created
      const updatedTopics = await admin.listTopics()
      const createdTopics = topicsToCreate.map((t) => t.topic)
      const missingTopics = createdTopics.filter((topic) => !updatedTopics.includes(topic))

      if (missingTopics.length > 0) {
        console.error(`[KafkaAdmin] Failed to create topics:`, missingTopics)
        throw new Error(`Failed to create topics: ${missingTopics.join(", ")}`)
      }

      console.log(`[KafkaAdmin] Verified all topics created successfully`)
    } else {
      console.log(`[KafkaAdmin] All topics already exist, skipping creation`)
    }
  } catch (error: any) {
    console.error(`[KafkaAdmin] Error managing topics:`, error.message)

    // If it's a topic exists error, that's actually OK
    if (error.message && error.message.includes("already exists")) {
      console.log(`[KafkaAdmin] Topics already exist, continuing...`)
    } else {
      throw error
    }
  } finally {
    try {
      await admin.disconnect()
      console.log(`[KafkaAdmin] Disconnected from Kafka`)
    } catch (disconnectError) {
      console.warn(`[KafkaAdmin] Warning during disconnect:`, disconnectError)
    }
  }
}

export const createNevoKafkaClient = (serviceNames: string[], options: KafkaClientFactoryOptions) => {
  const defaultOptions = {
    groupIdPrefix: options.clientIdPrefix,
    sessionTimeout: 15000,
    allowAutoTopicCreation: true,
    retryAttempts: 3,
    brokerRetryTimeout: 1000,
    debug: false,
    timeoutMs: 20000,
    discovery: {
      enabled: true,
      heartbeatIntervalMs: 5000,
      ttlMs: 15000
    }
  }

  const mergedOptions = { ...defaultOptions, ...options }

  return {
    provide: "NEVO_KAFKA_CLIENT",
    useFactory: async () => {
      const host = process.env[mergedOptions.kafkaHost || "KAFKA_HOST"] || "localhost"
      const port = parseInt(process.env[mergedOptions.kafkaPort || "KAFKA_PORT"] || "9092")

      console.log(`[NevoKafkaClient] Initializing for services: ${serviceNames.join(", ")}`)

      let topicCreationAttempts = 0
      const maxTopicCreationAttempts = 3

      while (topicCreationAttempts < maxTopicCreationAttempts) {
        try {
          await createKafkaTopics(serviceNames, { ...mergedOptions, kafkaHost: "KAFKA_HOST", kafkaPort: "KAFKA_PORT" })
          break
        } catch (error: any) {
          topicCreationAttempts++
          console.warn(`[NevoKafkaClient] Topic creation attempt ${topicCreationAttempts} failed:`, error.message)

          if (topicCreationAttempts >= maxTopicCreationAttempts) {
            console.error(`[NevoKafkaClient] Failed to create topics after ${maxTopicCreationAttempts} attempts`)
            throw error
          }

          // Wait before retrying
          await new Promise((resolve) => setTimeout(resolve, 2000 * topicCreationAttempts))
        }
      }

      const kafkaClientConfig = {
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: `${mergedOptions.clientIdPrefix}-nevo`,
            brokers: [`${host}:${port}`],
            connectionTimeout: 10000,
            requestTimeout: 30000,
            retry: {
              retries: mergedOptions.retryAttempts,
              initialRetryTime: 300,
              maxRetryTime: mergedOptions.brokerRetryTimeout
            }
          },
          consumer: {
            groupId: `${mergedOptions.groupIdPrefix}-nevo-consumer`,
            allowAutoTopicCreation: mergedOptions.allowAutoTopicCreation,
            sessionTimeout: mergedOptions.sessionTimeout,
            maxWaitTimeInMs: 5000,
            heartbeatInterval: 3000
          },
          producer: {
            createPartitioner: Partitioners.DefaultPartitioner,
            allowAutoTopicCreation: mergedOptions.allowAutoTopicCreation,
            idempotent: true,
            maxInFlightRequests: 1
          }
        }
      }

      const { ClientKafka } = await import("@nestjs/microservices")
      const kafkaClient = new ClientKafka(kafkaClientConfig.options)

      console.log(`[NevoKafkaClient] Created client for services: ${serviceNames.join(", ")}`)

      return new NevoKafkaClient(kafkaClient, serviceNames, {
        timeoutMs: mergedOptions.timeoutMs,
        debug: mergedOptions.debug,
        serviceName: mergedOptions.serviceName || mergedOptions.clientIdPrefix,
        authToken: mergedOptions.authToken,
        brokers: [`${host}:${port}`],
        discovery: mergedOptions.discovery
      })
    }
  }
}
