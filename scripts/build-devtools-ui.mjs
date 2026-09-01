#!/usr/bin/env node
/**
 * Bundles the DevTools dashboard into static assets shipped inside `dist`.
 *
 * React and every view are baked into one file, so the published package gains
 * no runtime dependency — `react`/`react-dom` stay devDependencies here. The
 * views import framework types only (`import type`), which esbuild erases, so
 * none of the transports leak into the browser bundle.
 */
import { build } from "esbuild"
import { promises as fs } from "node:fs"
import { dirname, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const clientDir = resolve(root, "src/devtools-ui/client")
const outDir = resolve(root, "dist/devtools-ui/public")

const watch = process.argv.includes("--watch")

await fs.mkdir(outDir, { recursive: true })

const options = {
  entryPoints: [resolve(clientDir, "index.tsx")],
  outdir: outDir,
  entryNames: "app",
  assetNames: "[name]",
  bundle: true,
  format: "esm",
  target: ["es2022"],
  platform: "browser",
  jsx: "automatic",
  minify: !watch,
  sourcemap: watch ? "inline" : false,
  legalComments: "none",
  define: { "process.env.NODE_ENV": watch ? '"development"' : '"production"' },
  loader: { ".css": "css" },
  logLevel: "info"
}

async function copyHtml() {
  await fs.copyFile(resolve(clientDir, "index.html"), resolve(outDir, "index.html"))
}

if (watch) {
  const { context } = await import("esbuild")
  const ctx = await context(options)
  await ctx.watch()
  await copyHtml()
  console.log(`[devtools-ui] watching ${clientDir}`)
} else {
  await build(options)
  await copyHtml()

  const sizes = await Promise.all(
    ["app.js", "app.css", "index.html"].map(async (name) => {
      const stat = await fs.stat(resolve(outDir, name))
      return `${name} ${(stat.size / 1024).toFixed(1)} KB`
    })
  )
  console.log(`[devtools-ui] built -> dist/devtools-ui/public (${sizes.join(", ")})`)
}
