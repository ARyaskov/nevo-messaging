import { test } from "node:test"
import assert from "node:assert/strict"
import { buildContract, describeSchema, NEVO_CONTRACT_METHOD } from "../src/common/contract"
import type { ServiceMethodMapping } from "../src/common/types"

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
