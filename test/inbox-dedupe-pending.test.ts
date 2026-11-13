import { test } from "node:test"
import assert from "node:assert/strict"

/**
 * Pending-claim contract against a REAL Postgres Pool: a live owner holds the
 * claim behind an entry barrier, an overlapping duplicate must get a retryable
 * error (never a fake empty success), a crashed owner must still redeliver
 * durably, and a finished void completion must return undefined in a way
 * distinguishable from a pending skip. Also asserts no BEGIN is left open on
 * any released client: every BEGIN in the run is closed by COMMIT or ROLLBACK.
 * Without NEVO_TEST_PG_URL the test self-skips (recorded as NOT_RUN, not PASS).
 */
const pgUrl = process.env["NEVO_TEST_PG_URL"]

async function waitUntil(cond: () => boolean, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!cond()) {
    if (Date.now() > deadline) throw new Error("condition not reached in time")
    await new Promise((r) => setTimeout(r, 10))
  }
}

test(
  "PgInboxStore+Pool: live-peer claim rejects duplicates retryably; crash redelivers durably",
  { skip: pgUrl ? false : "NEVO_TEST_PG_URL is not set (no Postgres infra; recorded as NOT_RUN)", timeout: 30_000 },
  async () => {
    const { Pool } = await import("pg")
    const { Inbox } = await import("../src/common/inbox")
    const { PgInboxStore } = await import("../src/common/pg-stores")
    const { MessagingError } = await import("../src/common/errors")
    const { ErrorCode } = await import("../src/common/error-code")

    const pool = new Pool({ connectionString: pgUrl })
    // Record every statement; at the end BEGIN must balance with
    // COMMIT/ROLLBACK, so no client is released inside a transaction.
    const sqlLog: string[] = []
    const track = (text: unknown): void => {
      sqlLog.push(typeof text === "string" ? text : ((text as { text?: string } | undefined)?.text ?? ""))
    }
    // Forward ALL arguments: pg's callback form must survive the wrapper, or a
    // released client reused by pool.query dispatches with a lost callback and
    // the awaiting promise never settles.
    const tracked = pool as unknown as { query: (...args: unknown[]) => unknown }
    const origPoolQuery = tracked.query.bind(pool)
    tracked.query = (...args: unknown[]) => {
      track(args[0])
      return origPoolQuery(...args)
    }
    const trackClient = (c: { query: (...args: unknown[]) => unknown; release: () => void }) => {
      const orig = c.query.bind(c)
      c.query = (...args: unknown[]) => {
        track(args[0])
        return orig(...args)
      }
      return c
    }

    // A per-run schema isolates the fixed `nevo_inbox_seen_at_idx` index name
    // that PgInboxStore.migrate creates, so parallel pg tests cannot collide.
    const schema = `nevo_inbox_pending_${Date.now().toString(36)}`
    const table = `nevo_inbox_pending`
    try {
      await pool.query(`CREATE SCHEMA ${schema}`)
      const store = new PgInboxStore({ client: pool, table, schema })
      await store.migrate()
      await pool.query(`CREATE TABLE ${schema}.${table}_projection (id TEXT PRIMARY KEY)`)

      // --- Live owner holds the claim behind an entry barrier ---
      let ownerRuns = 0
      let releaseOwner!: () => void
      const gate = new Promise<void>((r) => {
        releaseOwner = r
      })
      const ownerInbox = new Inbox({ store, awaitTimeoutMs: 100 })
      const ownerPromise = ownerInbox
        .dedupe(
          "u-live",
          async () => {
            ownerRuns++
            await gate
            return { by: "A" }
          },
          {
            tx: async (commit) => {
              const txc = trackClient((await pool.connect()) as unknown as { query: (...args: unknown[]) => unknown; release: () => void })
              await txc.query("BEGIN")
              await txc.query(`INSERT INTO ${schema}.${table}_projection (id) VALUES ('a')`)
              await commit(txc) // markSeen lands on the caller's transaction
              await txc.query("ROLLBACK")
              txc.release() // back to the pool only after the transaction is closed
              throw new Error("owner crashed before COMMIT")
            }
          }
        )
        .catch((err) => err as unknown)
      await waitUntil(() => ownerRuns > 0)

      // --- Overlapping duplicate: retryable "not ready", never a fake success ---
      const dupInbox = new Inbox({ store, awaitTimeoutMs: 80 })
      let dupRuns = 0
      await assert.rejects(
        () =>
          dupInbox.dedupe("u-live", async () => {
            dupRuns++
            return { by: "B" }
          }),
        (err: unknown) =>
          err instanceof MessagingError && err.code === ErrorCode.SERVICE_UNAVAILABLE && err.retryable === true && err.message.includes("u-live")
      )
      const stillPending = await pool.query(`SELECT status FROM ${schema}.${table} WHERE uuid = 'u-live'`)
      assert.equal((stillPending as { rows: { status?: string }[] }).rows[0]?.status, "pending", "no result may be committed for the live owner")

      // --- Owner crashes (rollback); nothing durable, claim stays pending ---
      releaseOwner()
      await ownerPromise // resolves with the crash error (caught above)
      assert.equal(((await pool.query(`SELECT COUNT(*)::int AS n FROM ${schema}.${table}_projection`)) as { rows: { n: number }[] }).rows[0].n, 0)
      assert.equal(
        ((await pool.query(`SELECT status FROM ${schema}.${table} WHERE uuid = 'u-live'`)) as { rows: { status?: string }[] }).rows[0].status,
        "pending"
      )

      // --- Redelivery after the claim TTL expires: runs once, commits durably ---
      await new Promise((r) => setTimeout(r, 20))
      const shortTtl = new PgInboxStore({ client: pool, table, schema, claimTtlMs: 5 })
      const retryInbox = new Inbox({ store: shortTtl, awaitTimeoutMs: 200 })
      let redeliveryRuns = 0
      const redelivered = await retryInbox.dedupe(
        "u-live",
        async () => {
          redeliveryRuns++
          return { by: "C" }
        },
        {
          tx: async (commit) => {
            const txc = trackClient((await pool.connect()) as unknown as { query: (...args: unknown[]) => unknown; release: () => void })
            await txc.query("BEGIN")
            await txc.query(`INSERT INTO ${schema}.${table}_projection (id) VALUES ('c')`)
            await commit(txc)
            await txc.query("COMMIT")
            txc.release() // back to the pool only after the transaction is closed
          }
        }
      )
      assert.equal(redeliveryRuns, 1, "the redelivery must run the handler exactly once")
      assert.deepEqual(redelivered, { by: "C" })
      assert.equal(((await pool.query(`SELECT COUNT(*)::int AS n FROM ${schema}.${table}_projection`)) as { rows: { n: number }[] }).rows[0].n, 1)
      assert.equal(
        ((await pool.query(`SELECT status FROM ${schema}.${table} WHERE uuid = 'u-live'`)) as { rows: { status?: string }[] }).rows[0].status,
        "done"
      )

      // --- Post-finish duplicate replays the durable result, no re-run ---
      const replayed = await dupInbox.dedupe("u-live", async () => {
        dupRuns++
        return { by: "D" }
      })
      assert.equal(dupRuns, 0, "a finished entry must not re-run the handler")
      assert.deepEqual(replayed, { by: "C" })

      // --- No open BEGIN on any released client ---
      const begins = sqlLog.filter((s) => /^\s*BEGIN\b/i.test(s)).length
      const ends = sqlLog.filter((s) => /^\s*(COMMIT|ROLLBACK)\b/i.test(s)).length
      assert.equal(begins, ends, "every BEGIN must be closed by COMMIT or ROLLBACK before release")
    } finally {
      await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).catch(() => {})
      await pool.end()
    }
  }
)

test(
  "PgInboxStore+Pool: a finished void completion returns undefined, distinguishable from pending",
  { skip: pgUrl ? false : "NEVO_TEST_PG_URL is not set (no Postgres infra; recorded as NOT_RUN)", timeout: 30_000 },
  async () => {
    const { Pool } = await import("pg")
    const { Inbox } = await import("../src/common/inbox")
    const { PgInboxStore } = await import("../src/common/pg-stores")

    const pool = new Pool({ connectionString: pgUrl })
    // Per-run schema isolates the fixed `nevo_inbox_seen_at_idx` index name.
    const schema = `nevo_inbox_void_${Date.now().toString(36)}`
    const table = `nevo_inbox_void`
    try {
      await pool.query(`CREATE SCHEMA ${schema}`)
      const store = new PgInboxStore({ client: pool, table, schema })
      await store.migrate()

      const inboxA = new Inbox({ store, awaitTimeoutMs: 100 })
      await inboxA.dedupe("u-void", async () => {
        /* handler returns void */
      })
      assert.equal(await store.isDone("u-void"), true, "the void completion must be reported done")
      assert.equal(await store.getResult("u-void"), undefined, "a void completion stores no value")

      // A later duplicate returns undefined BECAUSE the entry is finished —
      // distinguishable from the old pending-skip via the durable state.
      const inboxB = new Inbox({ store, awaitTimeoutMs: 100 })
      let runs = 0
      const r = await inboxB.dedupe("u-void", async () => {
        runs++
        return { should: "not run" }
      })
      assert.equal(runs, 0, "the finished void entry must not re-run the handler")
      assert.equal(r, undefined)
      assert.equal(
        ((await pool.query(`SELECT status FROM ${schema}.${table} WHERE uuid = 'u-void'`)) as { rows: { status?: string }[] }).rows[0].status,
        "done"
      )
    } finally {
      await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).catch(() => {})
      await pool.end()
    }
  }
)
