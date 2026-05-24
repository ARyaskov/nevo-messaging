type KafkaModule = typeof import("kafkajs")
type NatsCoreModule = typeof import("@nats-io/nats-core")
type NatsTransportModule = typeof import("@nats-io/transport-node")
type NatsJetStreamModule = typeof import("@nats-io/jetstream")
type SocketIoModule = typeof import("socket.io")
type SocketIoClientModule = typeof import("socket.io-client")

// Public surface returned by `getNatsModule()`.
// nats.js v3 split the legacy `nats` package into multiple scoped packages.
// The merged namespace exposes both `connect` (transport-node) and
// `headers` / codec helpers (nats-core) so callers can keep destructuring
// as before.
type NatsModule = NatsCoreModule & NatsTransportModule

let kafkaModule: KafkaModule | null = null
let natsCoreModule: NatsCoreModule | null = null
let natsTransportModule: NatsTransportModule | null = null
let natsMergedModule: NatsModule | null = null
let natsJetStreamModule: NatsJetStreamModule | null = null
let socketIoModule: SocketIoModule | null = null
let socketIoClientModule: SocketIoClientModule | null = null

function isModuleNotFound(error: unknown, name: string): boolean {
  if (!error || typeof error !== "object") return false
  const e = error as { code?: string; message?: string }
  if (e.code !== "MODULE_NOT_FOUND") return false
  const m = e.message ?? ""
  return m.includes(`'${name}'`) || m.includes(`"${name}"`) || m.includes(name)
}

function requireOptional<T>(name: string): T {
  try {
    return require(name) as T
  } catch (error: unknown) {
    if (isModuleNotFound(error, name)) {
      throw new Error(`Missing optional dependency "${name}". Install it to use this transport.`, { cause: error })
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
  if (natsMergedModule) return natsMergedModule
  if (!natsTransportModule) {
    natsTransportModule = requireOptional<NatsTransportModule>("@nats-io/transport-node")
  }
  if (!natsCoreModule) {
    // nats-core is a transitive dep of transport-node and is always installed alongside.
    try {
      natsCoreModule = require("@nats-io/nats-core") as NatsCoreModule
    } catch {
      throw new Error('Missing "@nats-io/nats-core" — it should be installed as a peer of "@nats-io/transport-node".')
    }
  }
  natsMergedModule = Object.assign({} as NatsModule, natsCoreModule, natsTransportModule)
  return natsMergedModule
}

export function getJetStreamModule(): NatsJetStreamModule {
  if (!natsJetStreamModule) {
    natsJetStreamModule = requireOptional<NatsJetStreamModule>("@nats-io/jetstream")
  }
  return natsJetStreamModule
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
