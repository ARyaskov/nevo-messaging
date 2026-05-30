import { test } from "node:test"
import assert from "node:assert/strict"
import { z } from "zod"
import { buildContract, describeSchema, NEVO_CONTRACT_METHOD, type ServiceContract } from "../src/common/contract"
import { contractsEqual } from "../src/common/contract-poller"
import { contractToOpenApi } from "../src/common/openapi-gen"
import { generateContractModule } from "../src/cli/generate"
import type { ServiceMethodMapping } from "../src/common/types"

// Pulls the JSON Schema generated for a single-method contract's `params` field
// out of the rendered OpenAPI document.
function paramsJsonSchema(contract: ServiceContract): any {
  const doc = contractToOpenApi(contract) as any
  const path = Object.values(doc.paths)[0] as any
  return path.post.requestBody.content["application/json"].schema.properties.params
}

test("buildContract skips nevo.* methods", () => {
  const reg: ServiceMethodMapping = {
    "user.getById": { serviceMethod: "getById", version: "v1" },
    "user.delete": { serviceMethod: "delete", version: "v2" },
    [NEVO_CONTRACT_METHOD]: { serviceMethod: "x" }
  }
  const c = buildContract("user", reg, { instanceId: "abc", serviceVersion: "1.2.3", capabilities: ["a"] })
  assert.equal(c.serviceName, "user")
  assert.equal(c.serviceVersion, "1.2.3")
  assert.equal(c.instanceId, "abc")
  assert.deepEqual(c.capabilities, ["a"])
  assert.equal(c.methods.length, 2)
  assert.deepEqual(c.methods.map((m) => m.signalName), ["user.delete", "user.getById"])
  assert.equal(c.methods.find((m) => m.signalName === "user.delete")?.version, "v2")
})

test("describeSchema detects zod v3 shape via _def.typeName", () => {
  const zodLike = { _def: { typeName: "ZodObject", shape: () => ({ id: { _def: { typeName: "ZodNumber" } } }) } }
  const d = describeSchema(zodLike)
  assert.equal(d?.kind, "zod")
  const shape = d?.shape as any
  assert.equal(shape.type, "object")
  assert.deepEqual(shape.fields.id, { type: "number" })
})

test("describeSchema detects zod v4 shape via _zod.def.type", () => {
  // v4 stores the discriminator in `_zod.def.type` (lowercase), `def.shape` is
  // a plain object, arrays use `def.element`, literals use `def.values` (array).
  const v4 = {
    _zod: {
      def: {
        type: "object",
        shape: {
          id:   { _zod: { def: { type: "bigint" } } },
          tags: { _zod: { def: { type: "array", element: { _zod: { def: { type: "string" } } } } } },
          tier: { _zod: { def: { type: "literal", values: ["free"] } } },
          tail: { _zod: { def: { type: "optional", innerType: { _zod: { def: { type: "number" } } } } } }
        }
      }
    }
  }
  const d = describeSchema(v4)
  assert.equal(d?.kind, "zod")
  const shape = d?.shape as any
  assert.equal(shape.type, "object")
  assert.deepEqual(shape.fields.id,   { type: "bigint" })
  assert.deepEqual(shape.fields.tags, { type: "array", items: { type: "string" } })
  assert.deepEqual(shape.fields.tier, { type: "literal", value: "free" })
  assert.deepEqual(shape.fields.tail, { type: "optional", inner: { type: "number" } })
})

test("describeSchema enum v4 uses def.entries", () => {
  const v4Enum = { _zod: { def: { type: "enum", entries: { A: "a", B: "b" } } } }
  const d = describeSchema(v4Enum)
  const shape = d?.shape as any
  assert.equal(shape.type, "enum")
  assert.deepEqual([...shape.values].sort(), ["a", "b"])
})

test("describeSchema detects class-validator class", () => {
  class CreateUserDto {}
  const d = describeSchema(CreateUserDto)
  assert.equal(d?.kind, "class-validator")
  assert.equal(d?.className, "CreateUserDto")
})

test("describeSchema returns null for nullish", () => {
  assert.equal(describeSchema(null), null)
  assert.equal(describeSchema(undefined), null)
})

// ---------------------------------------------------------------------------
// Schema drift detection — `contractsEqual` must look at the schema shape, not
// just the version tag, or a breaking param change without a version bump goes
// unnoticed and `nevo.contract.changed` never fires.
// ---------------------------------------------------------------------------

test("contractsEqual detects a param-schema change without a version bump", () => {
  const reg = (schema: unknown): ServiceMethodMapping => ({ "user.create": { serviceMethod: "create", version: "v1", schema } })
  const base = buildContract("user", reg(z.object({ name: z.string() })))

  // Field retyped (string -> number), same version.
  assert.equal(contractsEqual(base, buildContract("user", reg(z.object({ name: z.number() })))), false)
  // Field added, same version.
  assert.equal(contractsEqual(base, buildContract("user", reg(z.object({ name: z.string(), age: z.number() })))), false)
  // Field removed, same version.
  assert.equal(contractsEqual(base, buildContract("user", reg(z.object({})))), false)
  // Identical schema, same version -> equal.
  assert.equal(contractsEqual(base, buildContract("user", reg(z.object({ name: z.string() })))), true)
})

test("contractsEqual detects a result-schema change without a version bump", () => {
  const reg = (resultSchema: unknown): ServiceMethodMapping => ({
    "user.get": { serviceMethod: "get", version: "v1", schema: z.object({ id: z.bigint() }), resultSchema }
  })
  const a = buildContract("user", reg(z.object({ name: z.string() })))
  const b = buildContract("user", reg(z.object({ name: z.number() })))
  assert.equal(contractsEqual(a, b), false)
})

test("contractsEqual ignores object field ordering", () => {
  const a = buildContract("user", { "user.create": { serviceMethod: "create", version: "v1", schema: z.object({ name: z.string(), age: z.number() }) } })
  const b = buildContract("user", { "user.create": { serviceMethod: "create", version: "v1", schema: z.object({ age: z.number(), name: z.string() }) } })
  assert.equal(contractsEqual(a, b), true)
})

// ---------------------------------------------------------------------------
// Result schemas are captured (previously always null).
// ---------------------------------------------------------------------------

test("buildContract populates resultSchema from the handler", () => {
  const reg: ServiceMethodMapping = {
    "user.get": { serviceMethod: "get", version: "v1", schema: z.object({ id: z.bigint() }), resultSchema: z.object({ name: z.string() }) }
  }
  const m = buildContract("user", reg).methods[0]
  assert.equal(m.resultSchema?.kind, "zod")
  assert.deepEqual((m.resultSchema?.shape as any).fields.name, { type: "string" })
})

test("buildContract reads resultSchema from signal options too", () => {
  const reg: ServiceMethodMapping = {
    "user.get": { serviceMethod: "get", version: "v1", options: { resultSchema: z.object({ ok: z.boolean() }) } }
  }
  assert.equal(buildContract("user", reg).methods[0].resultSchema?.kind, "zod")
})

test("buildContract leaves resultSchema null when none is declared", () => {
  const reg: ServiceMethodMapping = { "user.get": { serviceMethod: "get", version: "v1" } }
  assert.equal(buildContract("user", reg).methods[0].resultSchema, null)
})

// ---------------------------------------------------------------------------
// serializeZod / JSON-schema / TS no longer collapse rich zod types to `{}`.
// ---------------------------------------------------------------------------

test("describeSchema serializes tuple/intersection/map/set/default structurally", () => {
  assert.deepEqual(describeSchema(z.tuple([z.string(), z.number()]))?.shape, {
    type: "tuple",
    items: [{ type: "string" }, { type: "number" }]
  })
  assert.deepEqual(describeSchema(z.intersection(z.object({ a: z.string() }), z.object({ b: z.number() })))?.shape, {
    type: "intersection",
    left: { type: "object", fields: { a: { type: "string" } } },
    right: { type: "object", fields: { b: { type: "number" } } }
  })
  assert.deepEqual(describeSchema(z.map(z.string(), z.number()))?.shape, { type: "map", keyType: { type: "string" }, valueType: { type: "number" } })
  assert.deepEqual(describeSchema(z.set(z.string()))?.shape, { type: "set", valueType: { type: "string" } })
  assert.deepEqual(describeSchema(z.string().default("x"))?.shape, { type: "default", inner: { type: "string" } })
})

test("contractToOpenApi renders a tuple as a proper array schema (not {})", () => {
  const c = buildContract("svc", { "svc.m": { serviceMethod: "m", version: "v1", schema: z.tuple([z.string(), z.number()]) } })
  const params = paramsJsonSchema(c)
  assert.notDeepEqual(params, {})
  assert.equal(params.type, "array")
  assert.deepEqual(params.prefixItems, [{ type: "string" }, { type: "number" }])
  assert.equal(params.minItems, 2)
  assert.equal(params.maxItems, 2)
})

test("contractToOpenApi renders a union as oneOf (not {})", () => {
  const c = buildContract("svc", { "svc.m": { serviceMethod: "m", version: "v1", schema: z.union([z.string(), z.number()]) } })
  const params = paramsJsonSchema(c)
  assert.notDeepEqual(params, {})
  assert.ok(Array.isArray(params.oneOf))
  assert.equal(params.oneOf.length, 2)
})

test("contractToOpenApi maps bigint to string/format bigint (wire-consistent)", () => {
  // On the JSON wire a bigint is serialized as a string (see bigint.utils.ts),
  // so the documented type must be `string`, not `integer`.
  const c = buildContract("svc", { "svc.m": { serviceMethod: "m", version: "v1", schema: z.object({ id: z.bigint() }) } })
  assert.deepEqual(paramsJsonSchema(c).properties.id, { type: "string", format: "bigint" })
})

test("default fields are omitted from required in OpenAPI and marked optional in TS", () => {
  const c = buildContract("svc", { "svc.m": { serviceMethod: "m", version: "v1", schema: z.object({ name: z.string(), role: z.string().default("user") }) } })
  assert.deepEqual(paramsJsonSchema(c).required, ["name"])
  assert.match(generateContractModule(c), /role\?:/)
})

test("generateContractModule emits real TS types for tuple/intersection/map/set", () => {
  const c = buildContract("svc", {
    "svc.tup": { serviceMethod: "tup", version: "v1", schema: z.tuple([z.string(), z.number()]) },
    "svc.inter": { serviceMethod: "inter", version: "v1", schema: z.intersection(z.object({ a: z.string() }), z.object({ b: z.number() })) },
    "svc.mp": { serviceMethod: "mp", version: "v1", schema: z.map(z.string(), z.number()) },
    "svc.st": { serviceMethod: "st", version: "v1", schema: z.set(z.string()) }
  })
  const mod = generateContractModule(c)
  assert.match(mod, /\[string, number\]/)
  assert.match(mod, /Map<string, number>/)
  assert.match(mod, /Set<string>/)
  assert.match(mod, /\) & \(/)
})
