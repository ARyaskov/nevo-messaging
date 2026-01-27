type KafkaModule = typeof import("kafkajs")
type NatsModule = typeof import("nats")
type SocketIoModule = typeof import("socket.io")
type SocketIoClientModule = typeof import("socket.io-client")

let kafkaModule: KafkaModule | null = null
let natsModule: NatsModule | null = null
let socketIoModule: SocketIoModule | null = null
let socketIoClientModule: SocketIoClientModule | null = null

function isModuleNotFound(error: any, name: string): boolean {
  if (!error) {
    return false
  }
  if (error.code === "MODULE_NOT_FOUND") {
    return error.message?.includes(`'${name}'`) || error.message?.includes(`\"${name}\"`) || error.message?.includes(name)
  }
  return false
}

function requireOptional<T>(name: string): T {
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    return require(name) as T
  } catch (error: any) {
    if (isModuleNotFound(error, name)) {
      const err = new Error(`Missing optional dependency "${name}". Install it to use this transport.`)
      ;(err as any).cause = error
      throw err
    }
    throw error
  }
}

export function getKafkaModule(): KafkaModule {
  if (!kafkaModule) {
    kafkaModule = requireOptional<KafkaModule>("kafkajs")
  }
  return kafkaModule
}

export function getNatsModule(): NatsModule {
  if (!natsModule) {
    natsModule = requireOptional<NatsModule>("nats")
  }
  return natsModule
}

export function getSocketIoModule(): SocketIoModule {
  if (!socketIoModule) {
    socketIoModule = requireOptional<SocketIoModule>("socket.io")
  }
  return socketIoModule
}

export function getSocketIoClientModule(): SocketIoClientModule {
  if (!socketIoClientModule) {
    socketIoClientModule = requireOptional<SocketIoClientModule>("socket.io-client")
  }
  return socketIoClientModule
}
