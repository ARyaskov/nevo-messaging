interface Request {
  method?: string
  headers: Record<string, string | string[] | undefined>
  body?: unknown
}
interface Response {
  statusCode: number
  setHeader(name: string, value: string): void
  end(): void
}
type NextFunction = () => void

export interface NevoHttpGatewayOptions {
  enableIdempotencyHeader?: boolean
  cors?: { origin?: string | string[]; methods?: string[]; allowHeaders?: string[]; exposeHeaders?: string[]; credentials?: boolean; maxAge?: number }
  rateLimitHeader?: boolean
  requestIdHeader?: string
}

export function createNevoHttpGateway(opts: NevoHttpGatewayOptions = {}): (req: Request, res: Response, next: NextFunction) => void {
  return (req, res, next) => {
    if (opts.cors) {
      const origin = Array.isArray(opts.cors.origin) ? opts.cors.origin.join(",") : opts.cors.origin
      if (origin) res.setHeader("access-control-allow-origin", origin)
      if (opts.cors.methods?.length) res.setHeader("access-control-allow-methods", opts.cors.methods.join(","))
      if (opts.cors.allowHeaders?.length) res.setHeader("access-control-allow-headers", opts.cors.allowHeaders.join(","))
      if (opts.cors.exposeHeaders?.length) res.setHeader("access-control-expose-headers", opts.cors.exposeHeaders.join(","))
      if (opts.cors.credentials) res.setHeader("access-control-allow-credentials", "true")
      if (opts.cors.maxAge !== undefined) res.setHeader("access-control-max-age", String(opts.cors.maxAge))
      if (req.method === "OPTIONS") { res.statusCode = 204; res.end(); return }
    }

    if (opts.requestIdHeader) {
      const headerVal = req.headers[opts.requestIdHeader] ?? req.headers["x-request-id"]
      if (headerVal && typeof req.body === "object" && req.body !== null) {
        const body = req.body as Record<string, unknown>
        if (!body.meta || typeof body.meta !== "object") body.meta = {}
        const meta = body.meta as Record<string, unknown>
        if (!meta.headers || typeof meta.headers !== "object") meta.headers = {}
        ;(meta.headers as Record<string, string>)["x-request-id"] = String(headerVal)
      }
    }

    if (opts.enableIdempotencyHeader !== false) {
      const idem = req.headers["idempotency-key"]
      if (idem && typeof req.body === "object" && req.body !== null) {
        const body = req.body as Record<string, unknown>
        if (!body.meta || typeof body.meta !== "object") body.meta = {}
        const meta = body.meta as Record<string, unknown>
        if (!meta.idempotencyKey) meta.idempotencyKey = String(idem)
      }
    }

    next()
  }
}
