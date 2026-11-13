import { test } from "node:test"

/**
 * Discriminating exactly-once check for the Postgres inbox: two executors plus
 * a rollback, against a REAL Postgres via `PgInboxStore`. It needs a live
 * server (NEVO_TEST_PG_URL) and the optional `pg` driver on the import path.
 * Without them the test self-skips — there is deliberately no fake Postgres
 * stand-in, so a skip must be recorded as NOT_RUN, not as a PASS.
 */
const pgUrl = process.env["NEVO_TEST_PG_URL"]

test(
  "PgInboxStore: two executors + rollback keep the projection exactly-once",
  { skip: pgUrl ? false : "NEVO_TEST_PG_URL is not set (no Postgres infra; recorded as NOT_RUN)" },
  async () => {
    const { Client } = await import("pg")
    const { Inbox } = await import("../src/common/inbox")
    const { PgInboxStore } = await import("../src/common/pg-stores")

    const client = new Client({ connectionString: pgUrl })
    await client.connect()
    const table = `nevo_inbox_exactonce_${Date.now().toString(36)}`
    try {
      const store = new PgInboxStore({ client, table })
      await store.migrate()
      // The projection table exists outside any transaction, so executor A's
      // ROLLBACK removes its insert but not the table itself.
      await client.query(`CREATE TABLE ${table}_projection (id TEXT PRIMARY KEY)`)

      const beginTx = async () => {
        const tx = new Client({ connectionString: pgUrl })
        await tx.connect()
        await tx.query("BEGIN")
        return tx
      }

      // Executor A: wins the claim, "applies the projection", then ROLLBACKs
      // (simulating a crash between projection and COMMIT). Nothing durable,
      // and the inbox row stays `pending` with no result.
      const inboxA = new Inbox({ store, awaitTimeoutMs: 100 })
      const txA = await beginTx()
      await inboxA
        .dedupe(
          "evt-exact-1",
          async () => {
            await txA.query(`INSERT INTO ${table}_projection (id) VALUES ('a')`)
            return { by: "A" }
          },
          {
            tx: async (commit) => {
              await commit(txA)
              await txA.query("ROLLBACK")
              // Crash semantics: the wrapper rejects, so no follower trusts A.
              throw new Error("executor A crashed before COMMIT")
            }
          }
        )
        .catch(() => {})
      await txA.end()
      const rowsA = await client.query(`SELECT COUNT(*)::int AS n FROM ${table}_projection`)
      if (rowsA.rows[0].n !== 0) throw new Error("rolled-back executor A must leave no projection rows")

      // Executor B: the claim is held by A (pending, fresh) with no result —
      // B must wait, re-claim, then SKIP, never racing A.
      const inboxB = new Inbox({ store, awaitTimeoutMs: 50 })
      let bRuns = 0
      const bResult = await inboxB.dedupe("evt-exact-1", async () => {
        bRuns++
        return { by: "B" }
      })
      if (bRuns !== 0) throw new Error("B must not double-execute while the claim is held")
      if (bResult !== undefined) throw new Error("a skipped executor has no result to return")

      // Executor C: after A's claim is past its (short) TTL, the re-claim wins,
      // the handler runs once and projection + markSeen COMMIT in one txn.
      await new Promise((r) => setTimeout(r, 20))
      const shortTtlStore = new PgInboxStore({ client, table, claimTtlMs: 5 })
      const inboxC = new Inbox({ store: shortTtlStore, awaitTimeoutMs: 100 })
      let cRuns = 0
      const txC = await beginTx()
      const cResult = await inboxC.dedupe(
        "evt-exact-1",
        async () => {
          cRuns++
          await txC.query(`INSERT INTO ${table}_projection (id) VALUES ('c')`)
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
      if (cRuns !== 1) throw new Error(`the re-claim winner must run the handler exactly once, ran ${cRuns}`)
      if ((cResult as { by?: string })?.by !== "C") throw new Error("the re-claim winner's result must be returned")

      const rows = await client.query(`SELECT COUNT(*)::int AS n FROM ${table}_projection`)
      if (rows.rows[0].n !== 1) throw new Error(`exactly one committed projection row expected, got ${rows.rows[0].n}`)
      const dedup = await client.query(`SELECT status FROM ${table} WHERE uuid = 'evt-exact-1'`)
      if (dedup.rows[0]?.status !== "done") throw new Error("the dedup record must be `done` after the committed run")
    } finally {
      await client.query(`DROP TABLE IF EXISTS ${table}_projection`).catch(() => {})
      await client.query(`DROP TABLE IF EXISTS ${table}`).catch(() => {})
      await client.end()
    }
  }
)
