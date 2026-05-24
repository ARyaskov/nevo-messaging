import { test } from "node:test"
import assert from "node:assert/strict"
import { generateContractModule } from "../src/cli/generate"
import type { ServiceContract } from "../src/common/contract"

const baseContract: ServiceContract = {
  protocol: "1",
  serviceName: "user",
  serviceVersion: "1.4.2",
  generatedAt: 1714000000000,
  methods: [
    { signalName: "user.getById", version: "v1" },
    { signalName: "user.create", version: "v1", paramsSchema: { kind: "zod", shape: { type: "object", fields: { name: { type: "string" }, age: { type: "optional", inner: { type: "number" } } } } } },
    { signalName: "user.delete", version: "v2" }
  ]
}

test("emits an interface with quoted dotted names and methods sorted", () => {
  const ts = generateContractModule(baseContract)
  assert.match(ts, /export interface UserServiceContract \{/)
  assert.match(ts, /"user\.getById": \{ params: unknown; result: unknown \}/)
  assert.match(ts, /"user\.delete": \{ params: unknown; result: unknown \}/)
  assert.match(ts, /export const UserContractMethods/)
})

test("emits zod-shape derived params", () => {
  const ts = generateContractModule(baseContract)
  assert.match(ts, /"user\.create": \{ params: \{[\s\S]*name: string[\s\S]*age\?: number \| undefined[\s\S]*\}; result: unknown \}/)
})

test("override service map name", () => {
  const ts = generateContractModule(baseContract, { serviceMapName: "UserApi" })
  assert.match(ts, /export interface UserApi /)
})

test("emits service meta object", () => {
  const ts = generateContractModule(baseContract)
  assert.match(ts, /serviceName: "user"/)
  assert.match(ts, /serviceVersion: "1\.4\.2"/)
})
