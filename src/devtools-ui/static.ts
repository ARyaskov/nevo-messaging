import { promises as fs } from "node:fs"
import * as path from "node:path"
import type { ServerResponse } from "node:http"

/**
 * Built client assets. Resolved relative to the compiled file, so it works both
 * from `dist/devtools-ui/static.js` and when consumed from node_modules.
 */
export const PUBLIC_DIR = path.join(__dirname, "public")

const CONTENT_TYPES: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".map": "application/json; charset=utf-8",
  ".woff2": "font/woff2"
}

export class MissingAssetsError extends Error {
  constructor(dir: string) {
    super(
      `DevTools UI assets not found at ${dir}.\n` +
        `The package ships them prebuilt; if you are running from a source checkout, run "pnpm run build:devtools-ui" first.`
    )
    this.name = "MissingAssetsError"
  }
}

export async function assertAssetsPresent(): Promise<void> {
  try {
    await fs.access(path.join(PUBLIC_DIR, "index.html"))
  } catch {
    throw new MissingAssetsError(PUBLIC_DIR)
  }
}

/** Blocks `..` traversal by requiring the resolved path to stay inside PUBLIC_DIR. */
function resolveWithinPublic(pathname: string): string | null {
  const relative = decodeURIComponent(pathname).replace(/^\/+/, "")
  const resolved = path.resolve(PUBLIC_DIR, relative)
  const root = path.resolve(PUBLIC_DIR)
  if (resolved !== root && !resolved.startsWith(root + path.sep)) return null
  return resolved
}

async function readIfFile(filePath: string): Promise<Buffer | null> {
  try {
    const stat = await fs.stat(filePath)
    if (!stat.isFile()) return null
    return await fs.readFile(filePath)
  } catch {
    return null
  }
}

/**
 * Serves a built asset, falling back to index.html for unknown paths so the
 * client-side router owns routes like `/services/user`.
 */
export async function handleStatic(res: ServerResponse, pathname: string): Promise<void> {
  const resolved = resolveWithinPublic(pathname === "/" ? "index.html" : pathname)

  if (resolved) {
    const body = await readIfFile(resolved)
    if (body) {
      const ext = path.extname(resolved)
      // Hashed bundle names make long-lived caching safe; index.html must not be cached.
      const immutable = ext === ".js" || ext === ".css"
      res.writeHead(200, {
        "content-type": CONTENT_TYPES[ext] ?? "application/octet-stream",
        "content-length": body.length,
        "cache-control": immutable ? "public, max-age=31536000, immutable" : "no-store"
      })
      res.end(body)
      return
    }
  }

  const index = await readIfFile(path.join(PUBLIC_DIR, "index.html"))
  if (!index) {
    const message = new MissingAssetsError(PUBLIC_DIR).message
    res.writeHead(500, { "content-type": "text/plain; charset=utf-8" })
    res.end(message)
    return
  }

  res.writeHead(200, {
    "content-type": "text/html; charset=utf-8",
    "content-length": index.length,
    "cache-control": "no-store"
  })
  res.end(index)
}
