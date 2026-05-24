import type { ServiceContract, ContractMethodDescriptor, SchemaDescriptor } from "./contract"

function schemaToJsonSchema(schema?: SchemaDescriptor | null): Record<string, unknown> {
  if (!schema) return { type: "object" }
  if (schema.kind === "json-schema") return (schema.shape as Record<string, unknown>) ?? { type: "object" }
  if (schema.kind === "zod") return zodShapeToJsonSchema(schema.shape)
  if (schema.kind === "class-validator") return { type: "object", title: schema.className }
  return { type: "object" }
}

function zodShapeToJsonSchema(node: any, depth = 0): Record<string, unknown> {
  if (!node || depth > 10) return { type: "object" }
  switch (node.type) {
    case "string": return { type: "string" }
    case "number": return { type: "number" }
    case "bigint": return { type: "string", format: "bigint" }
    case "boolean": return { type: "boolean" }
    case "date": return { type: "string", format: "date-time" }
    case "literal": return { const: node.value }
    case "enum": return { enum: node.values }
    case "array": return { type: "array", items: zodShapeToJsonSchema(node.items, depth + 1) }
    case "optional": return zodShapeToJsonSchema(node.inner, depth + 1)
    case "nullable": return { ...zodShapeToJsonSchema(node.inner, depth + 1), nullable: true }
    case "union": return { oneOf: (node.options ?? []).map((o: any) => zodShapeToJsonSchema(o, depth + 1)) }
    case "record": return { type: "object", additionalProperties: zodShapeToJsonSchema(node.valueType, depth + 1) }
    case "object": {
      const properties: Record<string, unknown> = {}
      const required: string[] = []
      for (const [k, v] of Object.entries(node.fields ?? {})) {
        properties[k] = zodShapeToJsonSchema(v, depth + 1)
        if ((v as any)?.type !== "optional") required.push(k)
      }
      return { type: "object", properties, required: required.length ? required : undefined }
    }
    default: return {}
  }
}

export interface OpenApiGenOptions {
  title?: string
  version?: string
  description?: string
  baseUrl?: string
}

export function contractToOpenApi(contract: ServiceContract, opts: OpenApiGenOptions = {}): unknown {
  const paths: Record<string, unknown> = {}
  for (const m of contract.methods) {
    paths[`/${contract.serviceName.toLowerCase()}-events/${encodeURIComponent(m.signalName)}`] = {
      post: {
        operationId: m.signalName,
        summary: `${m.signalName}@${m.version}`,
        requestBody: {
          required: true,
          content: {
            "application/json": {
              schema: {
                type: "object",
                required: ["uuid", "method", "params"],
                properties: {
                  uuid: { type: "string" },
                  method: { type: "string", const: m.signalName },
                  params: schemaToJsonSchema(m.paramsSchema)
                }
              }
            }
          }
        },
        responses: {
          "200": {
            description: "OK",
            content: { "application/json": { schema: { type: "object", properties: { uuid: { type: "string" }, method: { type: "string" }, params: { type: "object", properties: { result: schemaToJsonSchema(m.resultSchema) } } } } } }
          }
        }
      }
    }
  }

  return {
    openapi: "3.0.3",
    info: {
      title: opts.title ?? `${contract.serviceName} API`,
      version: opts.version ?? contract.serviceVersion ?? "1.0.0",
      description: opts.description ?? `Auto-generated from nevo-messaging contract (${contract.protocol})`
    },
    servers: opts.baseUrl ? [{ url: opts.baseUrl }] : undefined,
    paths
  }
}

export function contractToAsyncApi(contract: ServiceContract): unknown {
  const channels: Record<string, unknown> = {}
  for (const m of contract.methods) {
    channels[`${contract.serviceName.toLowerCase()}-events`] = channels[`${contract.serviceName.toLowerCase()}-events`] ?? {
      publish: {
        operationId: m.signalName,
        message: {
          name: m.signalName,
          contentType: "application/msgpack",
          payload: {
            type: "object",
            properties: {
              uuid: { type: "string" },
              method: { type: "string", const: m.signalName },
              params: schemaToJsonSchema(m.paramsSchema)
            }
          }
        }
      }
    }
  }

  return {
    asyncapi: "2.6.0",
    info: {
      title: `${contract.serviceName} AsyncAPI`,
      version: contract.serviceVersion ?? "1.0.0"
    },
    channels
  }
}
