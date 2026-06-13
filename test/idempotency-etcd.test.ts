import { test } from "node:test"
import assert from "node:assert/strict"
import type { Etcd3 } from "etcd3"
import { EtcdIdempotencyStore, type IdempotencyEtcdClient } from "../src/common/idempotency-etcd"

const etcd3ClientIsStructurallyCompatible: Etcd3 extends IdempotencyEtcdClient ? true : false = true

class FakePut implements PromiseLike<unknown> {
  private blob = ""
  constructor(private readonly apply: (value: string) => void) {}
  value(value: string | Buffer | number): FakePut {
    this.blob = String(value)
    return this
  }
  commit(): void {
    this.apply(this.blob)
  }
  then<TResult1 = unknown, TResult2 = never>(
    onfulfilled?: ((value: unknown) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    this.commit()
    return Promise.resolve(onfulfilled ? onfulfilled(undefined) : (undefined as TResult1))
  }
}

function fakeEtcd(): IdempotencyEtcdClient & { values: Map<string, string>; txCount: number } {
  const values = new Map<string, string>()
  let txCount = 0
  const client: IdempotencyEtcdClient & { values: Map<string, string>; txCount: number } = {
    values,
    txCount,
    get(key) {
      return { string: async () => values.get(String(key)) ?? null }
    },
    put(key) {
      return new FakePut((value) => values.set(String(key), value))
    },
    delete() {
      return {
        key(key) {
          values.delete(String(key))
          return Promise.resolve()
        }
      }
    },
    lease() {
      return {
        put(key) {
          return new FakePut((value) => values.set(String(key), value))
        },
        async revoke() {}
      }
    },
    if(key) {
      let operation: FakePut | undefined
      return {
        then(op: unknown) {
          operation = op as FakePut
          return this
        },
        async commit() {
          client.txCount = ++txCount
          const succeeded = !values.has(String(key))
          if (succeeded) operation?.commit()
          return { succeeded }
        }
      }
    }
  }
  return client
}

test("EtcdIdempotencyStore claims atomically across replicas and publishes the result", async () => {
  assert.equal(etcd3ClientIsStructurallyCompatible, true)
  const client = fakeEtcd()
  const a = new EtcdIdempotencyStore<{ ok: boolean }>({ client, ttlMs: 60_000 })
  const b = new EtcdIdempotencyStore<{ ok: boolean }>({ client, ttlMs: 60_000 })

  assert.deepEqual(await a.claim("order-1"), { acquired: true })
  assert.deepEqual(await b.claim("order-1"), { acquired: false, existing: undefined })
  await a.set("order-1", { ok: true })
  assert.deepEqual(await b.get("order-1"), { ok: true })
  assert.equal(client.txCount, 2)
})

test("EtcdIdempotencyStore delete removes the distributed value", async () => {
  const client = fakeEtcd()
  const store = new EtcdIdempotencyStore<number>({ client })
  await store.set("k", 42)
  assert.equal(await store.get("k"), 42)
  await store.delete("k")
  assert.equal(await store.has("k"), false)
})
