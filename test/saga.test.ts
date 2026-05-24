import { test } from "node:test"
import assert from "node:assert/strict"
import { createSaga } from "../src/common/saga"

test("saga executes steps in order", async () => {
  const order: string[] = []
  const result = await createSaga<{ x: number }>()
    .step({ name: "a", execute: () => { order.push("a") } })
    .step({ name: "b", execute: () => { order.push("b") } })
    .run({ x: 1 })
  assert.equal(result.status, "success")
  assert.deepEqual(order, ["a", "b"])
})

test("saga compensates on failure", async () => {
  const order: string[] = []
  const result = await createSaga<{ x: number }>()
    .step({ name: "a", execute: () => { order.push("a") }, compensate: () => { order.push("-a") } })
    .step({ name: "b", execute: () => { order.push("b") }, compensate: () => { order.push("-b") } })
    .step({ name: "c", execute: () => { throw new Error("boom") } })
    .run({ x: 1 })
  assert.equal(result.status, "failed")
  assert.deepEqual(order, ["a", "b", "-b", "-a"])
})
