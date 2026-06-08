import { test } from "node:test"
import assert from "node:assert/strict"
import { typed } from "../src/typed-client"
import type { TypedClient, QueryEmitClient, ServiceContractShape } from "../src/typed-client"
import { generateContractModule } from "../src/cli/generate"
import type { ServiceContract } from "../src/common/contract"

// ---------------------------------------------------------------------------
// A sample contract shaped exactly like what `generateContractModule` emits:
// `{ [signalName]: { params; result } }`.
// ---------------------------------------------------------------------------

interface User {
  id: string
  name: string
}

interface SampleContract {
  "user.getById": { params: { id: string }; result: User }
  "user.create": { params: { name: string }; result: { id: string } }
  "user.touch": { params: { id: string }; result: void }
}

// It must be assignable to the generic constraint the facade uses.
const _contractIsValidShape: ServiceContractShape = {} as SampleContract
void _contractIsValidShape

// ---------------------------------------------------------------------------
// Compile-time / type-level checks.
//
// These are erased at runtime (the test runner uses tsx, which strips types),
// but they fail `tsc` if the inference regresses — that is the point of the
// "type-safe messaging" claim. `Expect<Equal<...>>` is the standard trick: an
// inequality collapses the type to `never`/`false` and the `Expect` constraint
// then errors at build time.
// ---------------------------------------------------------------------------

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false
type Expect<T extends true> = T

// Inferred params for a method come from `TContract[method]["params"]`.
type _ParamsGetById = Parameters<TypedClient<SampleContract>["query"]>
// Inferred result for a method comes from `TContract[method]["result"]`.
type _ResultGetById = Awaited<ReturnType<typeof getByIdResult>>

declare const api: TypedClient<SampleContract>
function getByIdResult() {
  return api.query("user", "user.getById", { id: "1" })
}
function createResult() {
  return api.query("user", "user.create", { name: "Ada" })
}

// query result is inferred from the contract entry, not `any`/`unknown`.
type _AssertGetByIdResult = Expect<Equal<Awaited<ReturnType<typeof getByIdResult>>, User>>
type _AssertCreateResult = Expect<Equal<Awaited<ReturnType<typeof createResult>>, { id: string }>>
type _AssertVoidResult = Expect<Equal<Awaited<ReturnType<typeof touchResult>>, void>>

function touchResult() {
  return api.query("user", "user.touch", { id: "1" })
}

// The `method` argument is constrained to the contract keys.
type _MethodKeys = Parameters<TypedClient<SampleContract>["query"]>[1]
type _AssertMethodKeys = Expect<Equal<_MethodKeys, "user.getById" | "user.create" | "user.touch">>

// `params` is inferred per-method (third positional arg of `query`).
// We assert it via a dedicated extractor so a regression to `any` is caught.
type ParamsOf<K extends keyof SampleContract & string> = Parameters<
  <KK extends K>(...args: [string, KK, SampleContract[KK]["params"]]) => void
>[2]
type _AssertGetByIdParams = Expect<Equal<ParamsOf<"user.getById">, { id: string }>>
type _AssertCreateParams = Expect<Equal<ParamsOf<"user.create">, { name: string }>>

// emit shares the same params inference and resolves to void.
function emitResult() {
  return api.emit("user", "user.touch", { id: "1" })
}
type _AssertEmitResult = Expect<Equal<Awaited<ReturnType<typeof emitResult>>, void>>

// Negative checks: the compiler must reject a wrong-shaped param object and an
// unknown method name. These would be no-ops if params/method degraded to `any`.
// (tsx strips types at runtime, so @ts-expect-error only bites under `tsc`.)
function _negativeTypeChecks() {
  // @ts-expect-error — `name` is not a valid param for "user.getById" (expects { id: string })
  api.query("user", "user.getById", { name: "wrong" })
  // @ts-expect-error — "user.unknown" is not a method on the contract
  api.query("user", "user.unknown", { id: "1" })
  // @ts-expect-error — emit params are type-checked too
  api.emit("user", "user.create", { id: "missing-name" })
}

// Reference the type aliases / fns so unused-local lint/compile checks don't drop them.
type _Touch = [_ParamsGetById, _ResultGetById, _AssertGetByIdResult, _AssertCreateResult, _AssertVoidResult, _AssertMethodKeys, _AssertGetByIdParams, _AssertCreateParams, _AssertEmitResult]
void _negativeTypeChecks

// ---------------------------------------------------------------------------
// Runtime smoke test: `typed()` is a pure cast, so calls must forward verbatim
// to the underlying client and return its value unchanged.
// ---------------------------------------------------------------------------

interface RecordedCall {
  kind: "query" | "emit"
  service: string
  method: string
  params: unknown
  opts?: unknown
}

function makeFakeClient(responses: Record<string, unknown>): QueryEmitClient & { calls: RecordedCall[] } {
  const calls: RecordedCall[] = []
  return {
    calls,
    async query<T = unknown>(service: string, method: string, params: unknown, opts?: unknown): Promise<T> {
      calls.push({ kind: "query", service, method, params, opts })
      return responses[method] as T
    },
    async emit(service: string, method: string, params: unknown, opts?: unknown): Promise<void> {
      calls.push({ kind: "emit", service, method, params, opts })
    }
  }
}

test("typed() returns the same instance (pure compile-time wrapper)", () => {
  const fake = makeFakeClient({})
  const api = typed<SampleContract>(fake)
  assert.equal(api, fake)
})

test("typed query forwards service/method/params/opts and returns the result", async () => {
  const fake = makeFakeClient({ "user.getById": { id: "1", name: "Ada" } })
  const api = typed<SampleContract>(fake)

  const user = await api.query("user", "user.getById", { id: "1" }, { idempotencyKey: "k1" })
  assert.deepEqual(user, { id: "1", name: "Ada" })
  assert.equal(fake.calls.length, 1)
  assert.deepEqual(fake.calls[0], {
    kind: "query",
    service: "user",
    method: "user.getById",
    params: { id: "1" },
    opts: { idempotencyKey: "k1" }
  })
})

test("typed emit forwards verbatim and resolves to undefined", async () => {
  const fake = makeFakeClient({})
  const api = typed<SampleContract>(fake)

  const r = await api.emit("user", "user.touch", { id: "9" })
  assert.equal(r, undefined)
  assert.equal(fake.calls.length, 1)
  assert.deepEqual(fake.calls[0], {
    kind: "emit",
    service: "user",
    method: "user.touch",
    params: { id: "9" },
    opts: undefined
  })
})

test("typed() preserves access to the underlying client's other methods", async () => {
  // A client with an extra method beyond query/emit stays usable through the
  // intersection type returned by typed().
  const extra = {
    ...makeFakeClient({ "user.create": { id: "42" } }),
    getInstanceId(): string {
      return "inst-1"
    }
  }
  const api = typed<SampleContract, typeof extra>(extra)
  const created = await api.query("user", "user.create", { name: "Grace" })
  assert.deepEqual(created, { id: "42" })
  assert.equal(api.getInstanceId(), "inst-1")
})

// ---------------------------------------------------------------------------
// Codegen wiring: the generated module must import TypedClient and emit a
// `XClient = TypedClient<XServiceContract>` alias alongside the interface, so a
// generated contract is directly consumable as a typed client.
// ---------------------------------------------------------------------------

const codegenContract: ServiceContract = {
  protocol: "1",
  serviceName: "user",
  serviceVersion: "1.0.0",
  generatedAt: 1714000000000,
  methods: [{ signalName: "user.getById", version: "v1" }]
}

test("generateContractModule imports TypedClient and emits a typed client alias", () => {
  const ts = generateContractModule(codegenContract)
  assert.match(ts, /import type \{ TypedClient \} from "@riaskov\/nevo-messaging"/)
  assert.match(ts, /export interface UserServiceContract \{/)
  assert.match(ts, /export type UserClient = TypedClient<UserServiceContract>/)
})

test("typed client alias name follows an overridden service map name", () => {
  const ts = generateContractModule(codegenContract, { serviceMapName: "UserApi" })
  assert.match(ts, /export interface UserApi \{/)
  assert.match(ts, /export type UserApiClient = TypedClient<UserApi>/)
})
