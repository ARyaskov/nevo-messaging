import type { ServiceMethodHandler, ServiceMethodMapping } from "./types"
import { DEFAULT_METHOD_VERSION } from "./version"
import { getDefaultLogger } from "./logger"

export const NEVO_CONTRACT_METHOD = "nevo.contract"
export const CONTRACT_PROTOCOL_VERSION = "1"

export interface ContractMethodDescriptor {
  signalName: string
  version: string
  paramsSchema?: SchemaDescriptor | null
  resultSchema?: SchemaDescriptor | null
}

export interface SchemaDescriptor {
  kind: "zod" | "class-validator" | "unknown" | "json-schema"
  shape?: unknown
  className?: string
  raw?: string
}

export interface ServiceContract {
  protocol: typeof CONTRACT_PROTOCOL_VERSION
  serviceName: string
  serviceVersion?: string
  instanceId?: string
  capabilities?: string[]
  generatedAt: number
  methods: ContractMethodDescriptor[]
}

/**
 * Pulls the kind discriminator + def object out of any zod schema instance.
 * Supports both zod 4 (`schema._zod.def`, kind = lowercase string like "string", "object")
 * and zod 3 (`schema._def.typeName`, kind = "ZodString", "ZodObject"...).
 *
 * Returns null for non-zod values so callers can fall through to other detectors.
 */
function readZodInfo(schema: unknown): { kind: string; def: any; raw: string } | null {
  if (!schema || typeof schema !== "object") return null
  const anyS = schema as any
  // zod 4: canonical internal accessor
  if (anyS._zod?.def && typeof anyS._zod.def.type === "string") {
    return { kind: anyS._zod.def.type, def: anyS._zod.def, raw: anyS._zod.def.type }
  }
  // zod 3: legacy accessor, kept as a fallback so callers still using v3 work
  if (anyS._def && typeof anyS._def.typeName === "string") {
    const lower = String(anyS._def.typeName).replace(/^Zod/, "").toLowerCase()
    return { kind: lower, def: anyS._def, raw: anyS._def.typeName }
  }
  return null
}

export function describeSchema(schema: unknown): SchemaDescriptor | null {
  if (!schema) return null

  if (typeof schema === "object" && schema !== null) {
    const zod = readZodInfo(schema)
    if (zod) {
      return { kind: "zod", shape: serializeZod(schema), raw: zod.raw }
    }
    const anyS = schema as any
    if (typeof anyS.toJSON === "function") {
      try {
        return { kind: "json-schema", shape: anyS.toJSON() }
      } catch {}
    }
  }
  if (typeof schema === "function") {
    return { kind: "class-validator", className: (schema as { name: string }).name }
  }
  return { kind: "unknown" }
}

function serializeZod(zodSchema: unknown, depth = 0): unknown {
  if (!zodSchema || depth > 6) return null
  const info = readZodInfo(zodSchema)
  if (!info) return null
  const { kind, def } = info
  switch (kind) {
    case "string":
      return { type: "string" }
    case "number":
      return { type: "number" }
    case "bigint":
      return { type: "bigint" }
    case "boolean":
      return { type: "boolean" }
    case "date":
      return { type: "date" }
    case "literal": {
      // zod 4 stores `def.values` (array, supports z.literal([a, b])).
      // zod 3 stores a single `def.value`.
      if (Array.isArray(def.values)) {
        return {
          type: "literal",
          value: def.values.length === 1 ? def.values[0] : def.values
        }
      }
      return { type: "literal", value: def.value }
    }
    case "enum": {
      // zod 4: `def.entries` is a record { name: value }.
      // zod 3: `def.values` is an array. Some intermediate shapes use `def.options`.
      let values: unknown[] = []
      if (def.entries && typeof def.entries === "object") {
        values = Object.values(def.entries)
      } else if (Array.isArray(def.values)) {
        values = def.values
      } else if (Array.isArray(def.options)) {
        values = def.options
      }
      return { type: "enum", values }
    }
    case "array": {
      // zod 4: `def.element`. zod 3: `def.type`.
      const inner = def.element ?? def.type
      return { type: "array", items: serializeZod(inner, depth + 1) }
    }
    case "optional":
      return { type: "optional", inner: serializeZod(def.innerType, depth + 1) }
    case "nullable":
      return { type: "nullable", inner: serializeZod(def.innerType, depth + 1) }
    case "object": {
      // zod 4: `def.shape` is a plain object. zod 3: `def.shape()` is a getter.
      const shape = typeof def.shape === "function" ? def.shape() : def.shape
      const out: Record<string, unknown> = {}
      if (shape && typeof shape === "object") {
        for (const [k, v] of Object.entries(shape)) {
          out[k] = serializeZod(v, depth + 1)
        }
      }
      return { type: "object", fields: out }
    }
    case "union":
      return { type: "union", options: unionOptions(def).map((o) => serializeZod(o, depth + 1)) }
    case "discriminatedunion":
      // Same wire shape as a plain union; carry the discriminator property name so
      // consumers can document it if they choose to.
      return { type: "union", options: unionOptions(def).map((o) => serializeZod(o, depth + 1)), discriminator: def.discriminator }
    case "record": {
      // zod 4: `def.valueType` still exists; some builds use `def.element` for value.
      const valueType = def.valueType ?? def.element
      return { type: "record", valueType: serializeZod(valueType, depth + 1) }
    }
    case "tuple": {
      // zod 3 & 4: `def.items` is the array of positional element schemas;
      // `def.rest`, when present, types the variadic tail.
      const items = (Array.isArray(def.items) ? def.items : []).map((it: unknown) => serializeZod(it, depth + 1))
      const node: Record<string, unknown> = { type: "tuple", items }
      if (def.rest) node.rest = serializeZod(def.rest, depth + 1)
      return node
    }
    case "intersection":
      return { type: "intersection", left: serializeZod(def.left, depth + 1), right: serializeZod(def.right, depth + 1) }
    case "map":
      return { type: "map", keyType: serializeZod(def.keyType, depth + 1), valueType: serializeZod(def.valueType, depth + 1) }
    case "set":
      return { type: "set", valueType: serializeZod(def.valueType, depth + 1) }
    case "default":
      // `.default()` keeps the inner shape but makes the field omittable on input;
      // the object serializer treats a "default" node as optional.
      return { type: "default", inner: serializeZod(def.innerType, depth + 1) }
    case "effects":
      // zod 3 refine/transform/preprocess wrapper — `def.schema` is the base schema.
      return { type: "effects", inner: serializeZod(def.schema ?? def.innerType ?? def.in, depth + 1) }
    case "pipe":
    case "pipeline":
      // zod 3 `ZodPipeline` / zod 4 `ZodPipe`. The wire payload is the input side.
      return { type: "pipe", inner: serializeZod(def.in ?? def.out, depth + 1) }
    case "branded":
    case "readonly":
    case "catch":
      // Transparent wrappers with no effect on the wire shape — unwrap them.
      return serializeZod(def.type ?? def.innerType, depth + 1)
    default:
      getDefaultLogger().warn({ event: "contract.zod.unhandled", kind }, "Unhandled zod type during contract serialization; emitting kind only")
      return { type: kind }
  }
}

function unionOptions(def: any): unknown[] {
  const opts = def.options
  if (Array.isArray(opts)) return opts
  if (opts && typeof opts.values === "function") return [...opts.values()]
  return []
}

export function buildContract(
  serviceName: string,
  methodRegistry: ServiceMethodMapping,
  opts?: { instanceId?: string; serviceVersion?: string; capabilities?: string[] }
): ServiceContract {
  const methods: ContractMethodDescriptor[] = []
  for (const [signalName, handler] of Object.entries(methodRegistry)) {
    if (signalName.startsWith("nevo.")) continue
    const h = handler as ServiceMethodHandler
    methods.push({
      signalName,
      version: h.version || DEFAULT_METHOD_VERSION,
      paramsSchema: describeSchema(h.schema ?? (h.options as any)?.schema),
      resultSchema: describeSchema(h.resultSchema ?? (h.options as any)?.resultSchema)
    })
  }
  return {
    protocol: CONTRACT_PROTOCOL_VERSION,
    serviceName,
    serviceVersion: opts?.serviceVersion,
    instanceId: opts?.instanceId,
    capabilities: opts?.capabilities,
    generatedAt: Date.now(),
    methods: methods.sort((a, b) => a.signalName.localeCompare(b.signalName))
  }
}
