import { test } from "node:test"
import assert from "node:assert/strict"
import { DiscoveryRegistry } from "../src/common/discovery"
import {
  RegistryDiscoverySink,
  ConsulDiscoveryProvider,
  KubernetesDnsDiscoveryProvider,
  attachDiscoveryProvider
} from "../src/common/discovery-providers"

test("RegistryDiscoverySink replace evicts disappearing instances", () => {
  const reg = new DiscoveryRegistry()
  const sink = new RegistryDiscoverySink(reg)
  sink.replace("user", [
    { serviceName: "user", instanceId: "u-a", transport: "http", ts: Date.now() },
    { serviceName: "user", instanceId: "u-b", transport: "http", ts: Date.now() }
  ])
  assert.equal(reg.list().length, 2)
  sink.replace("user", [
    { serviceName: "user", instanceId: "u-a", transport: "http", ts: Date.now() }
  ])
  const ids = reg.listInstanceIdsFor("user")
  assert.deepEqual(ids, ["u-a"])
})

test("ConsulDiscoveryProvider polls /v1/health/service and writes to registry", async () => {
  const reg = new DiscoveryRegistry()
  let calls = 0
  const fetcher = (async (url: string) => {
    calls++
    if (url.includes("/v1/catalog/services")) {
      return new Response(JSON.stringify({ user: ["primary"] }), { status: 200, headers: { "content-type": "application/json" } })
    }
    return new Response(
      JSON.stringify([
        { Service: { ServiceID: "u-1", ServiceName: "user", ServiceAddress: "10.0.0.1", ServicePort: 8080, ServiceTags: ["user.getById"] }, Node: { Address: "10.0.0.1" } }
      ]),
      { status: 200, headers: { "content-type": "application/json" } }
    )
  }) as unknown as typeof fetch

  const provider = new ConsulDiscoveryProvider({ url: "http://consul:8500", pollIntervalMs: 50, fetcher })
  const detach = await attachDiscoveryProvider(reg, provider)
  // Allow one tick to settle.
  await new Promise((r) => setTimeout(r, 20))
  const entries = reg.list().filter((e) => e.serviceName === "user")
  assert.equal(entries.length, 1)
  assert.equal(entries[0].host, "10.0.0.1")
  assert.equal(entries[0].port, 8080)
  await detach()
  assert.ok(calls >= 2)
})

test("KubernetesDnsDiscoveryProvider resolves headless service A records", async () => {
  const reg = new DiscoveryRegistry()
  const provider = new KubernetesDnsDiscoveryProvider({
    services: [{ name: "user", port: 8080 }],
    pollIntervalMs: 50,
    resolver: {
      lookup: async () => [
        { address: "10.0.0.1", family: 4 },
        { address: "10.0.0.2", family: 4 }
      ] as any
    }
  })
  const detach = await attachDiscoveryProvider(reg, provider)
  await new Promise((r) => setTimeout(r, 20))
  const ids = reg.listInstanceIdsFor("user").sort()
  assert.deepEqual(ids, ["10.0.0.1", "10.0.0.2"])
  await detach()
})
