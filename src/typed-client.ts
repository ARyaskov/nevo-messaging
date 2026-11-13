/** A single contract entry: the params accepted and the result produced. */
export interface ContractMethod {
  params: unknown
  result: unknown
}

/** A generated service contract: map of method name to its {@link ContractMethod}. */
export type ServiceContractShape = Record<string, ContractMethod>

/** Per-call options understood by every transport client's `query`. */
export interface TypedQueryOptions {
  version?: string
  idempotencyKey?: string
  headers?: Record<string, string>
  timeoutMs?: number
  tenantId?: string
  /** Cooperative cancellation: forwarded to the transport; aborting rejects with CANCELLED. */
  signal?: AbortSignal
}

/** Per-call options understood by every transport client's `emit`. */
export interface TypedEmitOptions {
  version?: string
  idempotencyKey?: string
  headers?: Record<string, string>
  tenantId?: string
  /** Cooperative cancellation: forwarded to the transport; aborting rejects with CANCELLED. */
  signal?: AbortSignal
}

/** The minimal runtime surface {@link typed} needs from a transport client. */
export interface QueryEmitClient {
  query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: TypedQueryOptions): Promise<T>
  emit(serviceName: string, method: string, params: unknown, opts?: TypedEmitOptions): Promise<void>
}

/**
 * Strongly-typed view of a transport client for one service contract. The facade
 * does not intersect the raw client type, so arbitrary method strings never
 * type-check; the raw client stays reachable through {@link TypedClient._raw}.
 */
export interface TypedClient<TContract extends ServiceContractShape, TClient extends QueryEmitClient = QueryEmitClient> {
  /** Request/response against `method`, with params and result inferred from the contract. */
  query<K extends keyof TContract & string>(
    service: string,
    method: K,
    params: TContract[K]["params"],
    opts?: TypedQueryOptions
  ): Promise<TContract[K]["result"]>

  /** Fire-and-forget against `method`, with params inferred from the contract. */
  emit<K extends keyof TContract & string>(service: string, method: K, params: TContract[K]["params"], opts?: TypedEmitOptions): Promise<void>

  /** The underlying transport client (raw `query`/`emit` plus any extra methods). */
  readonly _raw: TClient
}

/** Wrap a transport client in a typed facade for `TContract` (a pure type-level wrapper). */
export function typed<TContract extends ServiceContractShape, TClient extends QueryEmitClient = QueryEmitClient>(
  client: TClient
): TypedClient<TContract, TClient> {
  return {
    query: async <K extends keyof TContract & string>(
      service: string,
      method: K,
      params: TContract[K]["params"],
      opts?: TypedQueryOptions
    ): Promise<TContract[K]["result"]> => (await client.query(service, method, params, opts)) as TContract[K]["result"],
    emit: async <K extends keyof TContract & string>(
      service: string,
      method: K,
      params: TContract[K]["params"],
      opts?: TypedEmitOptions
    ): Promise<void> => {
      await client.emit(service, method, params, opts)
    },
    _raw: client
  }
}
