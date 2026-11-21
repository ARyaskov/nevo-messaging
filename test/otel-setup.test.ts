import { test } from "node:test"
import assert from "node:assert/strict"
import { once } from "node:events"
import { createServer } from "node:http"
import type { AddressInfo } from "node:net"
import { setupNevoTracing } from "../src/common/otel-setup"

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

for (const exporter of ["otlp", "zipkin"] as const) {
  test(`setupNevoTracing exports a span with ${exporter} after async context propagation`, { timeout: 10_000 }, async () => {
    const requests: { path?: string; body: string }[] = []
    const collector = createServer(async (req, res) => {
      const chunks: Buffer[] = []
      for await (const chunk of req) chunks.push(chunk)
      requests.push({ path: req.url, body: Buffer.concat(chunks).toString() })
      res.writeHead(200, { "content-type": "application/json" }).end("{}")
    })
    collector.listen(0, "127.0.0.1")
    await once(collector, "listening")

    try {
      const path = exporter === "otlp" ? "/v1/traces" : "/api/v2/spans"
      const endpoint = `http://127.0.0.1:${(collector.address() as AddressInfo).port}${path}`
      const traceId = "1234567890abcdef1234567890abcdef"
      const parentId = "abcdef1234567890"
      const handle = await setupNevoTracing({ serviceName: "otel-test", exporter, endpoint })
      try {
        const trace = { traceparent: `00-${traceId}-${parentId}-01` }
        await handle.tracer.withSpan!("nevo-export-test", undefined, { trace }, async () => {
          await new Promise<void>((resolve) => setImmediate(resolve))
          const traceparent = handle.tracer.inject({}).trace?.traceparent
          assert.equal(traceparent?.split("-")[1], traceId)
          assert.notEqual(traceparent?.split("-")[2], parentId)
        })
      } finally {
        await handle.shutdown()
      }

      assert.equal(requests.length, 1)
      assert.equal(requests[0].path, path)
      assert.match(requests[0].body, /"name":"nevo-export-test"/)
      assert.match(requests[0].body, new RegExp(traceId))
    } finally {
      await new Promise<void>((resolve, reject) => collector.close((err) => (err ? reject(err) : resolve())))
    }
  })
}
