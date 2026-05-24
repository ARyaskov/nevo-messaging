import { test } from "node:test"
import assert from "node:assert/strict"
import { DiscoveryRegistry } from "../src/common/discovery"

test("multiple instances of same service are tracked separately", () => {
  const reg = new DiscoveryRegistry()
  reg.update({ serviceName: "user", instanceId: "a", transport: "nats", ts: Date.now() })
  reg.update({ serviceName: "user", instanceId: "b", transport: "nats", ts: Date.now() })
  const list = reg.listByService("user")
  assert.equal(list.length, 2)
})

test("prune removes stale", () => {
  const reg = new DiscoveryRegistry()
  reg.update({ serviceName: "user", instanceId: "a", transport: "nats", ts: Date.now() - 100_000 })
  reg.list().forEach((e) => (e.lastSeen = Date.now() - 100_000))
  reg.prune(1000)
  assert.equal(reg.list().length, 0)
})

test("isAvailable", () => {
  const reg = new DiscoveryRegistry()
  reg.update({ serviceName: "user", instanceId: "a", transport: "nats", ts: Date.now() })
  assert.equal(reg.isAvailable("user", 5000), true)
  assert.equal(reg.isAvailable("absent", 5000), false)
})
