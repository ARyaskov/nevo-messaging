import { test } from "node:test"
import assert from "node:assert/strict"

/**
 * Discriminating exactly-once check for the Postgres inbox: a live owner whose
 * claim is pending, a genuinely overlapping duplicate, and a crash + durable
 * redelivery, against a REAL Postgres via `PgInboxStore`. It needs a live
 * server (NEVO_TEST_PG_URL) and the optional `pg` driver on the import path.
 * Without them the test self-skips — there is deliberately no fake Postgres
 * stand-in, so a skip must be recorded as NOT_RUN, not as a PASS.
 */
const pgUrl = process.env["NEVO_TEST_PG_URL"]

test(
  "PgInboxStore: overlapping duplicate gets retryable-not-ready; crash still redelivers exactly once",
  { skip: pgUrl ? false : "NEVO_TEST_PG_URL is not set (no Postgres infra; recorded as NOT_RUN)" },
  async () => {
    const { Client } = await import("pg")
    const { Inbox } = await import("../src/common/inbox")
    const { PgInboxStore } = await import("../src/common/pg-stores")
    const { MessagingError } = await import("../src/common/errors")
    const { ErrorCode } = await import("../src/common/error-code")

    const client = new Client({ connectionString: pgUrl })
    await client.connect()
    // A per-run schema isolates the fixed `nevo_inbox_seen_at_idx` index name
    // that PgInboxStore.migrate creates, so parallel pg tests cannot collide.
    const schema = `nevo_inbox_exact_${Date.now().toString(36)}`
    const table = `nevo_inbox_exactonce`
    const txClients: { end(): Promise<void> }[] = []
    const beginTx = async () => {
      const tx = new Client({ connectionString: pgUrl })
      await tx.connect()
      await tx.query("BEGIN")
      txClients.push(tx)
      return tx
    }
    try {
      await client.query(`CREATE SCHEMA ${schema}`)
      const store = new PgInboxStore({ client, table, schema })
      await store.migrate()
      // The projection table exists outside any transaction, so executor A's
      // ROLLBACK removes its insert but not the table itself.
      await client.query(`CREATE TABLE ${schema}.${table}_projection (id TEXT PRIMARY KEY)`)

      // Executor A: wins the claim and stays LIVE behind an entry barrier
      // (an open transaction with a pending claim and no result).
      let aRuns = 0
      let releaseA!: () => void
      const gateA = new Promise<void>((r) => {
        releaseA = r
      })
      const inboxA = new Inbox({ store, awaitTimeoutMs: 100 })
      const txA = await beginTx()
      const aPromise = inboxA
        .dedupe(
          "evt-exact-1",
          async () => {
            aRuns++
            await txA.query(`INSERT INTO ${schema}.${table}_projection (id) VALUES ('a')`)
            await gateA
            return { by: "A" }
          },
          {
            tx: async (commit) => {
              await commit(txA)
              await txA.query("ROLLBACK")
              // Crash semantics: the wrapper rejects, so nothing is durable.
              throw new Error("executor A crashed before COMMIT")
            }
          }
        )
        .catch((err) => err as unknown)
      while (aRuns === 0) await new Promise((r) => setTimeout(r, 10))

      // Executor B runs WHILE A is still live and pending — a real overlap,
      // not a sequential rerun. B must get a retryable "not ready" error,
      // never a fake empty success and never its own execution.
      const inboxB = new Inbox({ store, awaitTimeoutMs: 50 })
      let bRuns = 0
      await assert.rejects(
        () =>
          inboxB.dedupe("evt-exact-1", async () => {
            bRuns++
            return { by: "B" }
          }),
        (err: unknown) =>
          err instanceof MessagingError && err.code === ErrorCode.SERVICE_UNAVAILABLE && err.retryable === true && err.message.includes("evt-exact-1")
      )
      assert.equal(bRuns, 0, "B must not double-execute while the live peer holds the claim")

      releaseA()
      // A's crash rejects its own call; the rolled-back projection proves it.
      await aPromise
      await txA.end()
      const rowsA = await client.query(`SELECT COUNT(*)::int AS n FROM ${schema}.${table}_projection`)
      assert.equal(rowsA.rows[0].n, 0, "rolled-back executor A must leave no projection rows")
      const pending = await client.query(`SELECT status, result FROM ${schema}.${table} WHERE uuid = 'evt-exact-1'`)
      assert.equal(pending.rows[0]?.status, "pending", "the crashed owner leaves a pending claim")
      assert.equal(pending.rows[0]?.result, null, "the crashed owner left no result")

      // Executor C: after A's claim is past its (short) TTL, the re-claim wins,
      // the handler runs once and projection + markSeen COMMIT in one txn.
      await new Promise((r) => setTimeout(r, 20))
      const shortTtlStore = new PgInboxStore({ client, table, schema, claimTtlMs: 5 })
      const inboxC = new Inbox({ store: shortTtlStore, awaitTimeoutMs: 100 })
      let cRuns = 0
      const txC = await beginTx()
      const cResult = await inboxC.dedupe(
        "evt-exact-1",
        async () => {
          cRuns++
          await txC.query(`INSERT INTO ${schema}.${table}_projection (id) VALUES ('c')`)
          return { by: "C" }
        },
        {
          tx: async (commit) => {
            await commit(txC)
            await txC.query("COMMIT")
          }
        }
      )
      await txC.end()
      assert.equal(cRuns, 1, `the re-claim winner must run the handler exactly once, ran ${cRuns}`)
      assert.deepEqual(cResult, { by: "C" }, "the redelivered executor's result must be returned")

      // Executor D: the durable result is replayed to later duplicates — no
      // error, no re-run, never the old undefined skip.
      let dRuns = 0
      const dResult = await inboxB.dedupe("evt-exact-1", async () => {
        dRuns++
        return { by: "D" }
      })
      assert.equal(dRuns, 0, "a finished entry must not re-run the handler")
      assert.deepEqual(dResult, { by: "C" }, "the duplicate receives the durable result")

      assert.equal((await client.query(`SELECT COUNT(*)::int AS n FROM ${schema}.${table}_projection`)).rows[0].n, 1)
      const dedup = await client.query(`SELECT status FROM ${schema}.${table} WHERE uuid = 'evt-exact-1'`)
      assert.equal(dedup.rows[0]?.status, "done", "the dedup record must be `done` after the committed run")
    } finally {
      for (const tx of txClients) await tx.end().catch(() => {})
      await client.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).catch(() => {})
      await client.end()
    }
  }
)
