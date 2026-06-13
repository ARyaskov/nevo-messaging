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
}

/** Per-call options understood by every transport client's `emit`. */
export interface TypedEmitOptions {
  version?: string
  idempotencyKey?: string
  headers?: Record<string, string>
  tenantId?: string
}

/** The minimal runtime surface {@link typed} needs from a transport client. */
export interface QueryEmitClient {
  query<T = unknown>(serviceName: string, method: string, params: unknown, opts?: TypedQueryOptions): Promise<T>
  emit(serviceName: string, method: string, params: unknown, opts?: TypedEmitOptions): Promise<void>
}

/** Strongly-typed view of a transport client for one service contract. */
export interface TypedClient<TContract extends ServiceContractShape> {
  /** Request/response against `method`, with params and result inferred from the contract. */
  query<K extends keyof TContract & string>(
    service: string,
    method: K,
    params: TContract[K]["params"],
    opts?: TypedQueryOptions
  ): Promise<TContract[K]["result"]>

  /** Fire-and-forget against `method`, with params inferred from the contract. */
  emit<K extends keyof TContract & string>(service: string, method: K, params: TContract[K]["params"], opts?: TypedEmitOptions): Promise<void>
}

/** Wrap a transport client in a typed facade for `TContract` (a pure type-level cast). */
export function typed<TContract extends ServiceContractShape, TClient extends QueryEmitClient = QueryEmitClient>(
  client: TClient
): TypedClient<TContract> & TClient {
  return client as unknown as TypedClient<TContract> & TClient
}
