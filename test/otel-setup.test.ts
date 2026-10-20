import { test } from "node:test"
import assert from "node:assert/strict"
import { setupNevoTracing } from "../src/common/otel-setup"

// OTel SDK 2.x dropped `new Resource()` and `provider.addSpanProcessor()`; setup must
// still wire a provider so the patched (2.x) SDK line stays usable.
test("setupNevoTracing initialises against the installed OTel SDK", async () => {
  const handle = await setupNevoTracing({ serviceName: "otel-smoke", exporter: "console", sampleRate: 0 })
  try {
    const span = handle.tracer.startSpan("smoke")
    span.setAttribute("k", "v")
    span.end()
  } finally {
    await handle.shutdown()
  }
})

test("setupNevoTracing with exporter none is a no-op", async () => {
  const handle = await setupNevoTracing({ serviceName: "otel-none", exporter: "none" })
  await handle.shutdown()
  assert.ok(handle.tracer)
})
