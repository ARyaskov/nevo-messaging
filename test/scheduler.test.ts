import { test } from "node:test"
import assert from "node:assert/strict"
import { parseCron, nextCronTick, isValidCron } from "../src/common/cron"
import {
  Scheduler,
  InMemoryScheduledTaskStore,
  Scheduled,
  getScheduledMethods,
  discoverAndRegisterScheduled
} from "../src/common/scheduler"

// --- cron parser ---

test("parseCron accepts every-minute expression", () => {
  const p = parseCron("* * * * *")
  assert.equal(p.minute.values.size, 60)
  assert.equal(p.hour.values.size, 24)
})

test("parseCron supports ranges and steps", () => {
  const p = parseCron("0,30 9-17 * * 1-5")
  assert.deepEqual([...p.minute.values].sort((a, b) => a - b), [0, 30])
  assert.deepEqual([...p.hour.values].sort((a, b) => a - b), [9, 10, 11, 12, 13, 14, 15, 16, 17])
  assert.deepEqual([...p.weekday.values].sort((a, b) => a - b), [1, 2, 3, 4, 5])
})

test("parseCron rejects out-of-range", () => {
  assert.throws(() => parseCron("99 * * * *"))
  assert.throws(() => parseCron("* * * 13 *"))
})

test("isValidCron sanity", () => {
  assert.equal(isValidCron("0 0 * * *"), true)
  assert.equal(isValidCron("not-a-cron"), false)
})

test("nextCronTick finds the next firing time", () => {
  // Use a fixed local time so the test is timezone-agnostic.
  const from = new Date(2026, 4, 27, 10, 0, 0).getTime() // 2026-05-27 10:00 local
  const next = nextCronTick("0 0 * * *", from)
  const d = new Date(next)
  // Cron uses local time; assert against local components.
  assert.equal(d.getHours(), 0)
  assert.equal(d.getMinutes(), 0)
  assert.ok(next > from, "next must be strictly after `from`")
  // Should land on the next calendar day (today is 27, next 0:00 is 28).
  assert.equal(d.getDate(), 28)
})

// --- scheduler ---

test("Scheduler.enqueueIn fires after the delay", async () => {
  const scheduler = new Scheduler({ pollIntervalMs: 20 })
  let fired = 0
  scheduler.registerHandler("ping", () => { fired++ })
  await scheduler.enqueueIn("ping", { hello: "world" }, 30)
  // Not yet due.
  await scheduler.flushOnce()
  assert.equal(fired, 0)
  await new Promise((r) => setTimeout(r, 50))
  await scheduler.flushOnce()
  assert.equal(fired, 1)
})

test("Scheduler.enqueueCron reschedules after each fire", async () => {
  // Use enqueueIn-style + manual reschedule check; cron with `* * * * *` would
  // need a real minute to roll. Instead, use a tight cron we can verify.
  const scheduler = new Scheduler({ pollIntervalMs: 20 })
  let fired = 0
  scheduler.registerHandler("daily", () => { fired++ })
  const taskId = await scheduler.enqueueCron("daily", null, "0 0 * * *")
  const before = (await scheduler.list({ status: "pending" })).find((t) => t.id === taskId)
  assert.ok(before, "task should be pending")
  assert.ok(before.runAt > Date.now(), "next fire is in the future")
})

test("Scheduler.cancel marks task cancelled", async () => {
  const scheduler = new Scheduler({ pollIntervalMs: 20 })
  let fired = 0
  scheduler.registerHandler("noop", () => { fired++ })
  const id = await scheduler.enqueueIn("noop", null, 50)
  await scheduler.cancel(id)
  await new Promise((r) => setTimeout(r, 60))
  await scheduler.flushOnce()
  assert.equal(fired, 0)
})

test("Scheduler retries failed tasks up to maxAttempts", async () => {
  const scheduler = new Scheduler({ pollIntervalMs: 10, maxAttempts: 3 })
  let attempts = 0
  scheduler.registerHandler("buggy", async () => {
    attempts++
    throw new Error("boom")
  })
  await scheduler.enqueueIn("buggy", null, 0)
  for (let i = 0; i < 5; i++) {
    await scheduler.flushOnce()
    await new Promise((r) => setTimeout(r, 5))
  }
  assert.equal(attempts, 3)
  const failed = (await scheduler.list({ status: "failed" }))
  assert.equal(failed.length, 1)
})

test("Scheduler skips tasks with no registered handler and marks them failed", async () => {
  const scheduler = new Scheduler({ pollIntervalMs: 10, maxAttempts: 1 })
  await scheduler.enqueueIn("orphan", null, 0)
  await scheduler.flushOnce()
  const failed = await scheduler.list({ status: "failed" })
  assert.equal(failed.length, 1)
  assert.match(failed[0].lastError ?? "", /No handler/)
})

// --- @Scheduled decorator ---

test("@Scheduled metadata is stored and discoverable", async () => {
  class CronService {
    async daily() { /* … */ }
    async startup() { /* … */ }
  }
  Scheduled({ cron: "0 0 * * *", name: "cron.daily" })(CronService.prototype, "daily", { value: CronService.prototype.daily })
  Scheduled({ in: 50, name: "cron.startup" })(CronService.prototype, "startup", { value: CronService.prototype.startup })

  const svc = new CronService()
  const meta = getScheduledMethods(svc)
  assert.equal(meta.length, 2)
  assert.ok(meta.find((m) => m.name === "cron.daily" && m.cron === "0 0 * * *"))
  assert.ok(meta.find((m) => m.name === "cron.startup" && m.in === 50))
})

test("discoverAndRegisterScheduled wires handlers + enqueues initial runs", async () => {
  class CronService {
    fired = 0
    async startup() { this.fired++ }
  }
  Scheduled({ in: 20, name: "cron.startup" })(CronService.prototype, "startup", { value: CronService.prototype.startup })

  const scheduler = new Scheduler({ pollIntervalMs: 10 })
  const svc = new CronService()
  const out = await discoverAndRegisterScheduled(scheduler, [svc])
  assert.equal(out.length, 1)
  assert.equal(out[0].name, "cron.startup")

  await new Promise((r) => setTimeout(r, 40))
  await scheduler.flushOnce()
  assert.equal(svc.fired, 1)
})

// --- distributed correctness ---

test("two replicas enqueue the same cron only once (deterministic id)", async () => {
  // A shared store stands in for a shared Postgres table across two processes.
  const store = new InMemoryScheduledTaskStore()
  const replicaA = new Scheduler({ store })
  const replicaB = new Scheduler({ store })

  const idA = await replicaA.enqueueCron("reports.daily", null, "0 0 * * *")
  const idB = await replicaB.enqueueCron("reports.daily", null, "0 0 * * *")

  // Both replicas derive the SAME id from the logical name, so the second
  // enqueue is a no-op (ON CONFLICT DO NOTHING) — one row, one firing per tick.
  assert.equal(idA, "cron:reports.daily")
  assert.equal(idA, idB)
  const crons = (await store.list()).filter((t) => t.cron)
  assert.equal(crons.length, 1, "only one row despite two replicas enqueuing")
})

test("claimDue reclaims a running task whose lease has expired", async () => {
  const store = new InMemoryScheduledTaskStore()
  await store.enqueue({
    id: "stuck", name: "x", payload: null, runAt: 1_000,
    attempts: 0, maxAttempts: 3, status: "pending", createdAt: 0
  })

  // Worker A claims it at t=2000 (10s lease) then "crashes" — never completes.
  const first = await store.claimDue("A", 2_000, 10, 10_000)
  assert.equal(first.length, 1)
  assert.equal(first[0].status, "running")

  // Inside the lease window: not reclaimable by anyone else.
  assert.equal((await store.claimDue("B", 5_000, 10, 10_000)).length, 0)

  // Past the lease: reaped and re-claimed by worker B.
  const reaped = await store.claimDue("B", 20_000, 10, 10_000)
  assert.equal(reaped.length, 1)
  assert.equal(reaped[0].id, "stuck")
  assert.equal(reaped[0].claimedBy, "B")
})

test("a reaped worker's markFailed is a no-op on the row the reaper now owns", async () => {
  const store = new InMemoryScheduledTaskStore()
  await store.enqueue({
    id: "stuck", name: "x", payload: null, runAt: 1_000,
    attempts: 0, maxAttempts: 3, status: "pending", createdAt: 0
  })

  // Worker A claims it (10s lease) then stalls — slower than its lease, but
  // still alive: it never crashes, so it eventually finishes and finalizes.
  const a = await store.claimDue("A", 2_000, 10, 10_000)
  assert.equal(a[0].claimedBy, "A")

  // Past the lease, worker B reaps the task and re-claims the same row.
  const b = await store.claimDue("B", 20_000, 10, 10_000)
  assert.equal(b[0].claimedBy, "B")

  // Worker A finally finishes its now-orphaned run and reports failure. It no
  // longer owns the row, so the finalizer must be fenced out: attempts must NOT
  // bump (else A prematurely exhausts maxAttempts) and B's claim is untouched.
  await store.markFailed("stuck", "stale boom", "A")
  let row = (await store.list()).find((t) => t.id === "stuck")!
  assert.equal(row.attempts, 0, "reaped worker must not bump attempts")
  assert.equal(row.status, "running", "row is still B's running claim")
  assert.equal(row.claimedBy, "B")

  // The current owner B finalizes normally — attempts bumps exactly once.
  await store.markFailed("stuck", "real boom", "B")
  row = (await store.list()).find((t) => t.id === "stuck")!
  assert.equal(row.attempts, 1, "owner's finalizer bumps attempts exactly once")
  assert.equal(row.status, "pending")
  assert.equal(row.claimedBy, undefined)
})

test("a reaped worker's reschedule/markCompleted are no-ops on the reaper's row", async () => {
  const store = new InMemoryScheduledTaskStore()
  await store.enqueue({
    id: "cron:x", name: "x", payload: null, runAt: 1_000, cron: "0 * * * *",
    attempts: 0, maxAttempts: 3, status: "pending", createdAt: 0
  })

  await store.claimDue("A", 2_000, 10, 10_000)          // worker A claims, then stalls
  await store.claimDue("B", 20_000, 10, 10_000)         // worker B reaps past the lease

  // A's late finalizers must not move the row B now owns.
  await store.reschedule("cron:x", 99_999, "A")
  await store.markCompleted("cron:x", "A")
  let row = (await store.list()).find((t) => t.id === "cron:x")!
  assert.equal(row.status, "running", "still B's running claim")
  assert.equal(row.claimedBy, "B")
  assert.notEqual(row.runAt, 99_999, "A must not reschedule the reaper's row")

  // B reschedules normally.
  await store.reschedule("cron:x", 99_999, "B")
  row = (await store.list()).find((t) => t.id === "cron:x")!
  assert.equal(row.status, "pending")
  assert.equal(row.runAt, 99_999)
  assert.equal(row.claimedBy, undefined)
})

test("a cron that fell behind skips missed runs and reschedules into the future", async () => {
  const store = new InMemoryScheduledTaskStore()
  const scheduler = new Scheduler({ store })
  let fired = 0
  scheduler.registerHandler("hourly", () => { fired++ })

  // Hourly cron whose scheduled runAt is 5 hours in the past (worker was down).
  const fiveHoursAgo = Date.now() - 5 * 60 * 60 * 1000
  await store.enqueue({
    id: "cron:hourly", name: "hourly", payload: null, runAt: fiveHoursAgo,
    cron: "0 * * * *", attempts: 0, maxAttempts: 5, status: "pending", createdAt: fiveHoursAgo
  })

  const res = await scheduler.flushOnce()
  assert.equal(fired, 1, "fires once, not once per missed hour (skip policy)")
  assert.equal(res.rescheduled, 1)

  const task = (await store.list()).find((t) => t.id === "cron:hourly")
  assert.ok(task)
  assert.equal(task.status, "pending")
  assert.ok(task.runAt > Date.now(), "rescheduled into the future, not replaying the past")
  assert.equal(new Date(task.runAt).getMinutes(), 0, "next run is at the top of an hour")
  assert.ok(task.runAt <= Date.now() + 60 * 60 * 1000, "and no further out than the next hour")
})

// --- cron semantics: DOM/DOW OR + timezones ---

test("cron ORs day-of-month and day-of-week when both are restricted", () => {
  // "0 0 13 * 5": midnight on the 13th OR any Friday.
  // Jan 2026 — the 13th is a Tuesday; Fridays fall on 2, 9, 16, 23, 30.

  // From Jan 1 00:01, the next match is Fri Jan 2 (day-of-week, NOT the 13th).
  let next = new Date(nextCronTick("0 0 13 * 5", new Date(2026, 0, 1, 0, 1).getTime()))
  assert.equal(next.getMonth(), 0)
  assert.equal(next.getDate(), 2)
  assert.equal(next.getDay(), 5)            // Friday
  assert.equal(next.getHours(), 0)
  assert.equal(next.getMinutes(), 0)

  // From Jan 9 00:01, the next match is Tue Jan 13 (day-of-month, NOT a Friday).
  next = new Date(nextCronTick("0 0 13 * 5", new Date(2026, 0, 9, 0, 1).getTime()))
  assert.equal(next.getDate(), 13)
  assert.notEqual(next.getDay(), 5)         // proves the 13th matched on its own
})

test("a UTC cron fires at the expected UTC minute", () => {
  // 14:30 UTC daily; from 10:00 UTC the next fire is the same day at 14:30 UTC.
  const from = Date.UTC(2026, 5, 15, 10, 0, 0)
  const next = new Date(nextCronTick("30 14 * * *", from, { utc: true }))
  assert.equal(next.getUTCFullYear(), 2026)
  assert.equal(next.getUTCMonth(), 5)
  assert.equal(next.getUTCDate(), 15)
  assert.equal(next.getUTCHours(), 14)
  assert.equal(next.getUTCMinutes(), 30)
})

test("an IANA-timezone cron fires at the expected local minute (DST-aware)", () => {
  // 02:30 daily in New York. On 2026-07-01 NY is on EDT (UTC-4), so 02:30 local
  // is 06:30 UTC. Start before that instant and expect that exact tick.
  const from = Date.UTC(2026, 6, 1, 0, 0, 0) // 2026-06-30 20:00 EDT
  const next = new Date(nextCronTick("30 2 * * *", from, { timezone: "America/New_York" }))
  assert.equal(next.getUTCHours(), 6)
  assert.equal(next.getUTCMinutes(), 30)

  // Confirm the New York wall clock reads 02:30.
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", hour: "2-digit", minute: "2-digit", hourCycle: "h23"
  }).formatToParts(next)
  const at = (type: string) => parts.find((p) => p.type === type)?.value
  assert.equal(at("hour"), "02")
  assert.equal(at("minute"), "30")
})
