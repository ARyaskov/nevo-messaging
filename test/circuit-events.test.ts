import { test } from "node:test"
import assert from "node:assert/strict"
import { CircuitBreakerRegistry } from "../src/common/circuit-breaker"
import { DevToolsBus } from "../src/common/devtools"
import { DevToolsRegistry } from "../src/common/devtools-registry"

test("circuit transitions emit devtools events", async () => {
  const bus = new DevToolsBus({ originId: "test" })
  const registry = new DevToolsRegistry()
  const cb = new CircuitBreakerRegistry({ enabled: true, failureThreshold: 2, resetTimeoutMs: 50 }, { bus, registry })

  const events: any[] = []
  bus.on((e) => { if (e.type === "circuit") events.push(e) })

  cb.before("svc:m"); cb.onFailure("svc:m", new Error("x"))
  cb.before("svc:m"); cb.onFailure("svc:m", new Error("x"))
  assert.equal(events.length, 1)
  assert.equal(events[0].extra.to, "open")

  assert.equal(registry.getCircuit("svc:m")?.state, "open")

  await new Promise((r) => setTimeout(r, 70))
  cb.before("svc:m")
  assert.equal(events.length, 2)
  assert.equal(events[1].extra.to, "half-open")

  cb.onSuccess("svc:m")
  assert.equal(events.length, 3)
  assert.equal(events[2].extra.to, "closed")
})

test("no events when disabled", () => {
  const bus = new DevToolsBus()
  const events: any[] = []
  bus.on((e) => events.push(e))
  const cb = new CircuitBreakerRegistry({ enabled: false }, { bus })
  cb.before("x:y"); cb.onFailure("x:y", new Error("x"))
  assert.equal(events.length, 0)
})
