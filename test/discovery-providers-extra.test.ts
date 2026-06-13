import { test } from "node:test"
import assert from "node:assert/strict"
import { DiscoveryRegistry } from "../src/common/discovery"
import { RegistryDiscoverySink, attachDiscoveryProvider } from "../src/common/discovery-providers"
import {
  EtcdDiscoveryProvider,
  EurekaDiscoveryProvider,
  CloudMapDiscoveryProvider,
  NomadDiscoveryProvider,
  type EtcdClientLike,
  type CloudMapClientLike
} from "../src/common/discovery-providers-extra"

test("Etcd provider parses JSON values under the configured prefix", async () => {
  const reg = new DiscoveryRegistry()
  const client: EtcdClientLike = {
    async getPrefix(_prefix) {
      return {
        "/services/user/instance-a": JSON.stringify({
          serviceName: "user",
          host: "10.0.0.1",
          port: 8080
        }),
        "/services/user/instance-b": JSON.stringify({
          serviceName: "user",
          host: "10.0.0.2",
          port: 8080
        })
      }
    }
  }
  const provider = new EtcdDiscoveryProvider({ client, prefix: "/services/", pollIntervalMs: 50 })
  const detach = await attachDiscoveryProvider(reg, provider)
  await new Promise((r) => setTimeout(r, 30))
  const ids = reg.listInstanceIdsFor("user").sort()
  assert.deepEqual(ids.length, 2)
  await detach()
})

test("Eureka provider polls /apps/<name> and converts UP instances", async () => {
  const reg = new DiscoveryRegistry()
  const fetcher = (async (url: string) => {
    if (url.endsWith("/apps/user")) {
      return new Response(
        JSON.stringify({
          application: {
            instance: [
              { instanceId: "u-1", hostName: "host1", ipAddr: "10.0.0.1", port: { $: 8080 }, status: "UP" },
              { instanceId: "u-2", hostName: "host2", ipAddr: "10.0.0.2", port: { $: 8080 }, status: "DOWN" }
            ]
          }
        }),
        { status: 200, headers: { "content-type": "application/json" } }
      )
    }
    return new Response("{}", { status: 200, headers: { "content-type": "application/json" } })
  }) as unknown as typeof fetch
  const provider = new EurekaDiscoveryProvider({
    url: "http://eureka:8761/eureka",
    appNames: ["user"],
    pollIntervalMs: 50,
    fetcher
  })
  const detach = await attachDiscoveryProvider(reg, provider)
  await new Promise((r) => setTimeout(r, 30))
  const ids = reg.listInstanceIdsFor("user").sort()
  assert.deepEqual(ids, ["u-1"]) // DOWN dropped
  await detach()
})

test("AWS Cloud Map provider reads HEALTHY instances", async () => {
  const reg = new DiscoveryRegistry()
  const client: CloudMapClientLike = {
    async discoverInstances() {
      return {
        Instances: [{ InstanceId: "i-1", Attributes: { AWS_INSTANCE_IPV4: "10.0.0.1", AWS_INSTANCE_PORT: "8080" } }]
      }
    }
  }
  const provider = new CloudMapDiscoveryProvider({
    client,
    services: [{ namespace: "prod.local", name: "user" }],
    pollIntervalMs: 50
  })
  const detach = await attachDiscoveryProvider(reg, provider)
  await new Promise((r) => setTimeout(r, 30))
  const entries = reg.listByService("user")
  assert.equal(entries.length, 1)
  assert.equal(entries[0].host, "10.0.0.1")
  assert.equal(entries[0].port, 8080)
  await detach()
})

test("Nomad provider converts /v1/service/<name> response", async () => {
  const reg = new DiscoveryRegistry()
  const fetcher = (async (url: string) => {
    if (url.includes("/v1/service/user")) {
      return new Response(JSON.stringify([{ ID: "alloc-1.user", ServiceName: "user", Address: "10.0.0.1", Port: 8080, Tags: ["user.getById"] }]), {
        status: 200,
        headers: { "content-type": "application/json" }
      })
    }
    return new Response("[]", { status: 200 })
  }) as unknown as typeof fetch
  const provider = new NomadDiscoveryProvider({
    url: "http://nomad:4646",
    serviceNames: ["user"],
    pollIntervalMs: 50,
    fetcher
  })
  const detach = await attachDiscoveryProvider(reg, provider)
  await new Promise((r) => setTimeout(r, 30))
  const list = reg.listByService("user")
  assert.equal(list.length, 1)
  assert.equal(list[0].port, 8080)
  assert.deepEqual(list[0].capabilities, ["user.getById"])
  await detach()
})
