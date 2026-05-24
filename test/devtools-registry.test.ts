import { test } from "node:test"
import assert from "node:assert/strict"
import { DevToolsRegistry, describeMethodsFromSignals } from "../src/common/devtools-registry"

test("registerService and listServices", () => {
  const r = new DevToolsRegistry()
  r.registerService({ serviceName: "user", topic: "user-events", instanceId: "i1", methods: [{ signalName: "user.getById", version: "v1" }] })
  r.registerService({ serviceName: "order", topic: "order-events", methods: [] })
  assert.equal(r.listServices().length, 2)
  assert.equal(r.getService("user")?.methods.length, 1)
})

test("re-registering preserves registeredAt", () => {
  const r = new DevToolsRegistry()
  r.registerService({ serviceName: "user", methods: [] })
  const first = r.getService("user")!.registeredAt
  r.registerService({ serviceName: "user", methods: [{ signalName: "x" }] })
  assert.equal(r.getService("user")!.registeredAt, first)
  assert.equal(r.getService("user")!.methods.length, 1)
})

test("recordCircuit and listCircuits", () => {
  const r = new DevToolsRegistry()
  r.recordCircuit("user:ping", "open", { failures: 5 })
  const c = r.getCircuit("user:ping")!
  assert.equal(c.state, "open")
  assert.equal(c.service, "user")
  assert.equal(c.method, "ping")
  assert.equal(c.failures, 5)
  r.recordCircuit("user:ping", "closed")
  assert.equal(r.getCircuit("user:ping")!.state, "closed")
})

test("describeMethodsFromSignals strips nevo.* and reads hasSchema", () => {
  const arr = describeMethodsFromSignals([
    { signalName: "user.getById", methodName: "getById", version: "v1" },
    { signalName: "user.create", methodName: "create", version: "v2", options: { schema: {} } },
    { signalName: "nevo.contract", methodName: "x", version: "v1" }
  ] as any)
  assert.equal(arr.length, 2)
  assert.equal(arr[1].hasSchema, true)
})
