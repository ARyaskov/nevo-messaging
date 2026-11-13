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

interface TrackedClient {
  client: { query: (...args: unknown[]) => unknown; release: () => void }
  /** Per-connection SQL log; `release` asserts this connection's transaction is closed. */
  log: string[]
}

// Connects a pool client with per-connection SQL tracking. `release` asserts the
// connection's own BEGIN is closed by COMMIT/ROLLBACK — no client may return to
// the pool inside an open transaction. ALL query arguments are forwarded
// unchanged: pg's callback form must survive the wrapper, or a released client
// reused by pool.query dispatches with a lost callback and the awaiting promise
// never settles.
async function connectTracked(pool: { connect: () => Promise<unknown> }, sqlLog?: string[]): Promise<TrackedClient> {
  const client = (await pool.connect()) as { query: (...args: unknown[]) => unknown; release: () => void }
  const log: string[] = []
  const origQuery = client.query.bind(client)
  client.query = (...args: unknown[]) => {
    const text = typeof args[0] === "string" ? args[0] : ((args[0] as { text?: string } | undefined)?.text ?? "")
    log.push(text)
    sqlLog?.push(text)
    return origQuery(...args)
  }
  const origRelease = client.release.bind(client)
  client.release = () => {
    // Release-order proof, asserted at release time: replaying the connection's
    // own SQL must show NO transaction still open — every BEGIN is closed by a
    // COMMIT/ROLLBACK before the client returns to the pool. A trailing no-op
    // ROLLBACK after COMMIT is fine (the docs pattern's finally), so the count
    // of terminators may exceed the count of BEGINs; what may never happen is
    // an unbalanced open BEGIN.
    let open = 0
    for (const s of log) {
      if (/^\s*BEGIN\b/i.test(s)) open++
      else if (/^\s*(COMMIT|ROLLBACK)\b/i.test(s)) open = Math.max(0, open - 1)
    }
    assert.equal(open, 0, "client released with an open transaction (BEGIN not closed by COMMIT/ROLLBACK before release)")
    return origRelease()
  }
  return { client, log }
}

// The docs/inbox.md "transactional, exactly-once" pattern: the caller BEGINs on
// its own client BEFORE dedupe, the commit wrapper resolves only after COMMIT,
// and the finally ROLLBACK+release must also cover the paths that return before
// opts.tx runs (cached duplicate, follower, retryable busy).
async function docsPattern<T>(
  inbox: {
    dedupe: (
      uuid: string,
      handler: () => Promise<unknown>,
      opts?: { tx?: (commit: (tx?: unknown) => Promise<void>) => Promise<void> }
    ) => Promise<unknown>
  },
  uuid: string,
  client: TrackedClient["client"],
  handler: () => Promise<T>,
  tx: (commit: (tx?: unknown) => Promise<void>) => Promise<void>
): Promise<T> {
  try {
    await client.query("BEGIN")
    return (await inbox.dedupe(uuid, handler, { tx })) as T
  } finally {
    // No-op after a successful COMMIT; closes the BEGIN of early returns.
    await client.query("ROLLBACK").catch(() => {})
    client.release()
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
    // Pool-level statements (store claim/markSeen without tx) are recorded in
    // the shared sqlLog; per-client tracking lives in connectTracked below.
    const tracked = pool as unknown as { query: (...args: unknown[]) => unknown }
    const origPoolQuery = tracked.query.bind(pool)
    tracked.query = (...args: unknown[]) => {
      track(args[0])
      return origPoolQuery(...args)
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
              const { client: txc } = await connectTracked(pool, sqlLog)
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
            const { client: txc } = await connectTracked(pool, sqlLog)
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

test(
  "PgInboxStore+Pool: docs BEGIN-before-dedupe pattern — early returns never release an open transaction",
  { skip: pgUrl ? false : "NEVO_TEST_PG_URL is not set (no Postgres infra; recorded as NOT_RUN)", timeout: 30_000 },
  async () => {
    const { Pool } = await import("pg")
    const { Inbox } = await import("../src/common/inbox")
    const { PgInboxStore } = await import("../src/common/pg-stores")
    const { MessagingError } = await import("../src/common/errors")
    const { ErrorCode } = await import("../src/common/error-code")

    const pool = new Pool({ connectionString: pgUrl })
    // Per-run schema isolates the fixed `nevo_inbox_seen_at_idx` index name.
    const schema = `nevo_inbox_docs_${Date.now().toString(36)}`
    const table = `nevo_inbox_docs`
    try {
      await pool.query(`CREATE SCHEMA ${schema}`)
      const store = new PgInboxStore({ client: pool, table, schema })
      await store.migrate()
      await pool.query(`CREATE TABLE ${schema}.${table}_projection (id TEXT PRIMARY KEY)`)
      const projectionCount = async (id: string): Promise<number> =>
        ((await pool.query(`SELECT COUNT(*)::int AS n FROM ${schema}.${table}_projection WHERE id = $1`, [id])) as { rows: { n: number }[] }).rows[0]
          .n

      const inbox = new Inbox({ store, awaitTimeoutMs: 100 })

      // --- Normal commit: handler runs, wrapper commits, release follows COMMIT ---
      {
        const { client: c, log } = await connectTracked(pool)
        let txRan = false
        const result = await docsPattern(
          inbox,
          "u-doc",
          c,
          async () => {
            await c.query(`INSERT INTO ${schema}.${table}_projection (id) VALUES ('doc-a')`)
            return { by: "A" }
          },
          async (commit) => {
            txRan = true
            await commit(c) // markSeen lands on the caller's transaction
            await c.query("COMMIT")
          }
        )
        assert.deepEqual(result, { by: "A" })
        assert.equal(txRan, true, "the executing call must invoke the commit wrapper")
        assert.equal(await projectionCount("doc-a"), 1)
        // Release-order proof for this client: COMMIT is in the log before release.
        assert.ok(
          log.some((s) => /^\s*COMMIT\b/i.test(s)),
          "the executing call must COMMIT before release"
        )
      }

      // --- Cached duplicate: returns BEFORE opts.tx; finally ROLLBACK closes the BEGIN ---
      {
        const { client: c, log } = await connectTracked(pool)
        let txRan = false
        const result = await docsPattern(
          inbox,
          "u-doc",
          c,
          async () => {
            throw new Error("handler must not re-run")
          },
          async () => {
            txRan = true
          }
        )
        assert.deepEqual(result, { by: "A" })
        assert.equal(txRan, false, "a cached duplicate must return before the commit wrapper runs")
        assert.ok(
          log.some((s) => /^\s*ROLLBACK\b/i.test(s)),
          "the early return's BEGIN must be closed by the finally ROLLBACK before release"
        )
      }

      // --- Finished void: durable void completion returns undefined before opts.tx ---
      await inbox.dedupe("u-doc-void", async () => {
        /* handler returns void */
      })
      {
        const { client: c, log } = await connectTracked(pool)
        let txRan = false
        const result = await docsPattern(
          inbox,
          "u-doc-void",
          c,
          async () => ({ should: "not run" }),
          async () => {
            txRan = true
          }
        )
        assert.equal(result, undefined)
        assert.equal(txRan, false, "a finished void duplicate must return before the commit wrapper runs")
        assert.equal(await store.isDone("u-doc-void"), true)
        assert.ok(
          log.some((s) => /^\s*ROLLBACK\b/i.test(s)),
          "the early return's BEGIN must be closed by the finally ROLLBACK before release"
        )
      }

      // --- Follower: concurrent same-uuid call awaits the leader and returns
      // before its own opts.tx; its BEGIN is still closed by finally ROLLBACK ---
      {
        let runs = 0
        let releaseGate!: () => void
        const gate = new Promise<void>((r) => {
          releaseGate = r
        })
        const handler = async (): Promise<{ by: string }> => {
          runs++
          await gate
          return { by: "F" }
        }
        const leader = await connectTracked(pool)
        const follower = await connectTracked(pool)
        let followerTxRan = false
        const leaderPromise = docsPattern(inbox, "u-doc-follower", leader.client, handler, async (commit) => {
          await commit(leader.client)
          await leader.client.query("COMMIT")
        })
        await waitUntil(() => runs > 0)
        const followerPromise = docsPattern(inbox, "u-doc-follower", follower.client, handler, async () => {
          followerTxRan = true
        })
        releaseGate()
        assert.deepEqual(await leaderPromise, { by: "F" })
        assert.deepEqual(await followerPromise, { by: "F" })
        assert.equal(runs, 1, "the follower must not re-run the handler")
        assert.equal(followerTxRan, false, "the follower must return before its own commit wrapper runs")
        assert.ok(
          follower.log.some((s) => /^\s*ROLLBACK\b/i.test(s)),
          "the follower's BEGIN must be closed by the finally ROLLBACK before release"
        )
      }

      // --- Retryable busy: a live peer holds the claim; the BEGIN-before-dedupe
      // caller gets a retryable error before its own opts.tx runs ---
      {
        const ownerInbox = new Inbox({ store, awaitTimeoutMs: 100 })
        let ownerRuns = 0
        let releaseOwner!: () => void
        const gate = new Promise<void>((r) => {
          releaseOwner = r
        })
        const owner = await connectTracked(pool)
        const ownerPromise = docsPattern(
          ownerInbox,
          "u-doc-busy",
          owner.client,
          async () => {
            ownerRuns++
            await gate
            return { by: "O" }
          },
          async (commit) => {
            await commit(owner.client)
            await owner.client.query("COMMIT")
          }
        ).catch((err) => err as unknown)
        await waitUntil(() => ownerRuns > 0)

        const caller = await connectTracked(pool)
        let callerTxRan = false
        await assert.rejects(
          () =>
            docsPattern(
              inbox,
              "u-doc-busy",
              caller.client,
              async () => ({ should: "not run" }),
              async () => {
                callerTxRan = true
              }
            ),
          (err: unknown) => err instanceof MessagingError && err.code === ErrorCode.SERVICE_UNAVAILABLE && err.retryable === true
        )
        assert.equal(callerTxRan, false, "a retryable busy must return before the commit wrapper runs")
        assert.ok(
          caller.log.some((s) => /^\s*ROLLBACK\b/i.test(s)),
          "the busy caller's BEGIN must be closed by the finally ROLLBACK before release"
        )
        releaseOwner()
        assert.deepEqual(await ownerPromise, { by: "O" })
      }

      // --- Error rollback: wrapper rejects before COMMIT; finally ROLLBACK undoes
      // the projection write and no dedup record is committed ---
      {
        const { client: c, log } = await connectTracked(pool)
        await assert.rejects(
          () =>
            docsPattern(
              inbox,
              "u-doc-err",
              c,
              async () => {
                await c.query(`INSERT INTO ${schema}.${table}_projection (id) VALUES ('doc-err')`)
                return { by: "E" }
              },
              async () => {
                throw new Error("COMMIT never happened")
              }
            ),
          /COMMIT never happened/
        )
        assert.equal(await projectionCount("doc-err"), 0, "the failed wrapper's projection write must be rolled back")
        assert.equal(await store.isDone("u-doc-err"), false, "a failed wrapper must not mark the entry done")
        assert.ok(
          log.some((s) => /^\s*ROLLBACK\b/i.test(s)),
          "the error path must be closed by the finally ROLLBACK before release"
        )
      }
    } finally {
      await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).catch(() => {})
      await pool.end()
    }
  }
)
