import { test } from "node:test"
import assert from "node:assert/strict"
import { promises as fs } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { runGen } from "../src/cli/gen-service"

async function makeOutDir(): Promise<string> {
  const dir = await fs.mkdtemp(join(tmpdir(), "nevo-gen-"))
  return dir
}

test("gen --type=consumer emits .consumer.ts with @Backpressure", async () => {
  const out = await makeOutDir()
  const code = await runGen(["node", "gen", "audit", "--type", "consumer", "--out", out, "--transport", "kafka", "--force"])
  assert.equal(code, 0)
  const consumer = await fs.readFile(join(out, "src", "audit", "audit.consumer.ts"), "utf8")
  assert.match(consumer, /@Backpressure/)
  assert.match(consumer, /wrapSubscriptionHandler/)
  assert.match(consumer, /KafkaClientBase/)
})

test("gen --type=worker emits .worker.ts with Outbox", async () => {
  const out = await makeOutDir()
  const code = await runGen(["node", "gen", "nightly-report", "--type", "worker", "--out", out, "--force"])
  assert.equal(code, 0)
  const worker = await fs.readFile(join(out, "src", "nightly-report", "nightly-report.worker.ts"), "utf8")
  assert.match(worker, /Outbox/)
  assert.match(worker, /flushOnce/)
  // the worker relies on Nest's OnModuleDestroy, not GracefulShutdown — no dead import
  assert.doesNotMatch(worker, /GracefulShutdown/)
})

test("gen --type=worker main.ts wires a SIGTERM/SIGINT graceful shutdown", async () => {
  const out = await makeOutDir()
  const code = await runGen(["node", "gen", "nightly-report", "--type", "worker", "--out", out, "--force"])
  assert.equal(code, 0)
  const main = await fs.readFile(join(out, "src", "main.ts"), "utf8")
  assert.match(main, /process\.on\(/)
  assert.match(main, /SIGTERM/)
  assert.match(main, /SIGINT/)
  assert.match(main, /app\.close\(\)/)
  // the old scaffold hung forever on an unreachable close(); that must be gone
  assert.doesNotMatch(main, /new Promise\(\(\) => \{\}\)/)
})

test("gen --type=saga emits .saga.ts with createSaga and compensation", async () => {
  const out = await makeOutDir()
  const code = await runGen(["node", "gen", "order", "--type", "saga", "--out", out, "--force"])
  assert.equal(code, 0)
  const saga = await fs.readFile(join(out, "src", "order", "order.saga.ts"), "utf8")
  assert.match(saga, /createSaga/)
  assert.match(saga, /compensate/)
  assert.match(saga, /InMemorySagaStore/)
})

test("gen --type=workflow emits .workflow.ts with EventStore replay", async () => {
  const out = await makeOutDir()
  const code = await runGen(["node", "gen", "onboarding", "--type", "workflow", "--out", out, "--force"])
  assert.equal(code, 0)
  const wf = await fs.readFile(join(out, "src", "onboarding", "onboarding.workflow.ts"), "utf8")
  assert.match(wf, /InMemoryEventStore/)
  assert.match(wf, /async resume\(/)
  assert.match(wf, /workflowId/)
})

test("gen --type=bogus exits 2", async () => {
  const code = await runGen(["node", "gen", "x", "--type", "bogus" as any])
  assert.equal(code, 2)
})

test("gen with no --type defaults to plain service template", async () => {
  const out = await makeOutDir()
  const code = await runGen(["node", "gen", "user", "--out", out, "--force"])
  assert.equal(code, 0)
  const svc = await fs.readFile(join(out, "src", "user", "user.service.ts"), "utf8")
  assert.match(svc, /UserService/)
})
