/**
 * Type-safe facade over the any-shaped transport clients.
 *
 * The transport clients (`NevoNatsClient`, `NevoHttpClient`, …) expose a
 * `query<T>(service, method, params)` / `emit(service, method, params)` surface
 * where `method` is a free `string` and `params` is `unknown`. That is the
 * runtime contract, but it gives no compile-time guarantee that the params you
 * pass — or the result you await — match the remote method.
 *
 * {@link TypedClient} closes that gap. Given a generated contract shaped
 * `{ [method]: { params; result } }` (exactly what {@link generateContractModule}
 * emits), it derives strongly-typed `query` / `emit` signatures: the `method`
 * argument is constrained to the contract's keys, `params` is inferred from
 * `TContract[method]["params"]`, and the awaited value is
 * `TContract[method]["result"]`.
 *
 * It is a pure compile-time wrapper — {@link typed} returns the underlying
 * client unchanged at runtime (no proxy, no allocation), so there is zero
 * overhead and any extra transport methods (`publish`, `broadcast`, `subscribe`,
 * `close`, …) remain reachable via the intersection type.
 */

/** A single contract entry: the params accepted and the result produced. */
export interface ContractMethod {
  params: unknown
  result: unknown
}

/**
 * Shape a generated service contract conforms to: a map of method name to its
 * {@link ContractMethod} descriptor. This mirrors the interface emitted by the
 * CLI codegen, e.g. `{ "user.getById": { params: { id: string }; result: User } }`.
 */
export type ServiceContractShape = Record<string, ContractMethod>

/** Per-call options understood by every transport client's `query`. */
export interface TypedQueryOptions {
  version?: string
  idempotencyKey?: string
  headers?: Record<string, string>
  timeoutMs?: number
  tenantId?: string
}

/** Per-call options understood by every transport client's `emit`. */
export interface TypedEmitOptions {
  version?: string
  idempotencyKey?: string
  headers?: Record<string, string>
  tenantId?: string
}

/**
 * The minimal runtime surface {@link typed} needs from a transport client. Every
 * nevo client (`NevoNatsClient`, `NevoKafkaClient`, `NevoHttpClient`,
 * `NevoHttp2Client`, `NevoWsClient`, `NevoSocketClient`) already satisfies this.
 */
export interface QueryEmitClient {
  query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: TypedQueryOptions): Promise<T>
  emit(serviceName: string, method: string, params: unknown, opts?: TypedEmitOptions): Promise<void>
}

/**
 * Strongly-typed view of a transport client for one service contract.
 *
 * `K extends keyof TContract` keys the call to a known method; `params` and the
 * resolved value are then inferred from that method's contract entry.
 */
export interface TypedClient<TContract extends ServiceContractShape> {
  /**
   * Request/response against `method`. `params` is type-checked against
   * `TContract[method]["params"]` and the promise resolves to
   * `TContract[method]["result"]`.
   */
  query<K extends keyof TContract & string>(
    service: string,
    method: K,
    params: TContract[K]["params"],
    opts?: TypedQueryOptions
  ): Promise<TContract[K]["result"]>

  /**
   * Fire-and-forget against `method`. `params` is type-checked against
   * `TContract[method]["params"]`.
   */
  emit<K extends keyof TContract & string>(
    service: string,
    method: K,
    params: TContract[K]["params"],
    opts?: TypedEmitOptions
  ): Promise<void>
}

/**
 * Wrap an existing transport client in a typed facade for `TContract`.
 *
 * Purely a type-level cast — the same `client` instance is returned, so all of
 * its other methods stay available through the intersection. Pass the generated
 * contract type explicitly:
 *
 * ```ts
 * const api = typed<UserServiceContract>(natsClient)
 * const user = await api.query("user", "user.getById", { id: "1" })
 * //    ^? UserServiceContract["user.getById"]["result"]
 * ```
 *
 * @param client any object exposing `query` / `emit` (every nevo client does)
 * @returns the same instance, typed as `TypedClient<TContract> & TClient`
 */
export function typed<TContract extends ServiceContractShape, TClient extends QueryEmitClient = QueryEmitClient>(
  client: TClient
): TypedClient<TContract> & TClient {
  return client as unknown as TypedClient<TContract> & TClient
}
