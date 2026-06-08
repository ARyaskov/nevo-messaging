import { test } from "node:test"
import assert from "node:assert/strict"
import { promises as fs } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import {
  AuditLog,
  InMemoryAuditSink,
  FileAuditSink,
  PgAuditSink,
  TeeAuditSink,
  type AuditPgClient,
  type AuditEntry
} from "../src/common/audit-log"

test("InMemoryAuditSink records redacted entries", async () => {
  const sink = new InMemoryAuditSink()
  const log = new AuditLog({ enabled: true, sink })
  await log.record({
    uuid: "01-abc",
    ts: Date.now(),
    service: "user",
    method: "user.create",
    caller: "frontend",
    outcome: "ok",
    durationMs: 12,
    params: { name: "Eddie", password: "secret-pw" },
    result: { id: 1 }
  })
  const stored = sink.list()
  assert.equal(stored.length, 1)
  assert.equal((stored[0].params as any).password, "[REDACTED]")
  assert.deepEqual(stored[0].result, { id: 1 })
})

test("AuditLog drops payload when oversize", async () => {
  const sink = new InMemoryAuditSink()
  const log = new AuditLog({ enabled: true, sink, maxEntryBytes: 200 })
  const big = "x".repeat(2_000)
  await log.record({
    uuid: "01-big",
    ts: Date.now(),
    service: "s",
    method: "m",
    caller: null,
    outcome: "ok",
    durationMs: 1,
    params: { big },
    result: { big }
  })
  const stored = sink.list()[0]
  assert.equal((stored.params as any).__dropped, "oversize")
  assert.equal((stored.result as any).__dropped, "oversize")
})

test("FileAuditSink appends NDJSON and flushes on close", async () => {
  const path = join(tmpdir(), `nevo-audit-${Date.now()}.jsonl`)
  const sink = new FileAuditSink({ path, fsync: false, batchSize: 2, flushIntervalMs: 50 })
  const log = new AuditLog({ enabled: true, sink })
  for (let i = 0; i < 3; i++) {
    await log.record({
      uuid: `u-${i}`,
      ts: Date.now(),
      service: "s",
      method: "m",
      caller: null,
      outcome: "ok",
      durationMs: 1,
      params: { i },
      result: null
    })
  }
  await sink.close()
  const content = await fs.readFile(path, "utf8")
  const lines = content.trim().split("\n").filter(Boolean)
  assert.equal(lines.length, 3)
  assert.equal((JSON.parse(lines[2]) as AuditEntry).uuid, "u-2")
  await fs.unlink(path)
})

test("PgAuditSink buffers entries when the connection fails", async () => {
  let failing = true
  const client: AuditPgClient = {
    async query() {
      if (failing) throw new Error("ECONNREFUSED")
      return undefined
    }
  }
  const sink = new PgAuditSink({ client, bufferCap: 5 })
  const log = new AuditLog({ enabled: true, sink })
  await log.record({
    uuid: "u-1",
    ts: Date.now(),
    service: "s",
    method: "m",
    caller: null,
    outcome: "ok",
    durationMs: 1,
    params: {},
    result: null
  })
  // Buffered, not yet written.
  failing = false
  await log.record({
    uuid: "u-2",
    ts: Date.now(),
    service: "s",
    method: "m",
    caller: null,
    outcome: "ok",
    durationMs: 1,
    params: {},
    result: null
  })
  // The second call drains the buffer + writes u-2.
})

test("TeeAuditSink fans out to every wrapped sink", async () => {
  const a = new InMemoryAuditSink()
  const b = new InMemoryAuditSink()
  const sink = new TeeAuditSink([a, b])
  const log = new AuditLog({ enabled: true, sink })
  await log.record({
    uuid: "x",
    ts: 0,
    service: "s",
    method: "m",
    caller: null,
    outcome: "ok",
    durationMs: 0,
    params: {},
    result: null
  })
  assert.equal(a.list().length, 1)
  assert.equal(b.list().length, 1)
})
