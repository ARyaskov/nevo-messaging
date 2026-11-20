import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { PgOutboxStore, type PgClient } from "../src/common/pg-stores"

const pgUrl = process.env["NEVO_TEST_PG_URL"]

test("a competing PostgreSQL outbox claim skips the entire locked partition", { skip: !pgUrl }, async () => {
  const { Client } = require("pg")
  const first = new Client({ connectionString: pgUrl })
  const second = new Client({ connectionString: pgUrl })
  await Promise.all([first.connect(), second.connect()])
  const table = `test_outbox_${randomUUID().replaceAll("-", "")}`
  const owner = new PgOutboxStore({ client: first as PgClient, table })
  const competitor = new PgOutboxStore({ client: second as PgClient, table })
  try {
    await owner.migrate()
    for (const id of ["a", "b"]) {
      await owner.save({
        id,
        serviceName: "catalog",
        method: "changed",
        params: {},
        partitionKey: "same",
        createdAt: 1,
        attempts: 0,
        status: "pending"
      })
    }
    await first.query("BEGIN")
    assert.deepEqual(
      (await owner.listPending(1)).map((row) => row.id),
      ["a"]
    )
    assert.deepEqual(await competitor.listPending(1), [])
    await owner.markPublished("a")
    await first.query("COMMIT")
    assert.deepEqual(
      (await competitor.listPending(1)).map((row) => row.id),
      ["b"]
    )
  } finally {
    await first.query("ROLLBACK")
    await first.query(`DROP TABLE IF EXISTS "${table}"`)
    await Promise.all([first.end(), second.end()])
  }
})
