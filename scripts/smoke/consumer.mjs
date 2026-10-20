// An ESM application consuming the built (CommonJS) package. The app imports Nest
// through the ESM loader while dist/ requires it; both must see one module instance,
// or Nest's DI and decorators would silently diverge.
import "reflect-metadata"
import assert from "node:assert/strict"
import { createRequire } from "node:module"
import * as common from "@nestjs/common"
import * as core from "@nestjs/core"
import nevo from "../../dist/index.js"

const requireFromDist = createRequire(new URL("../../dist/index.js", import.meta.url))
assert.equal(requireFromDist("@nestjs/common").Module, common.Module, "dist/ and the app must share @nestjs/common")
assert.equal(requireFromDist("@nestjs/core").NestFactory, core.NestFactory, "dist/ and the app must share @nestjs/core")

const { default: runApp } = await import("./run-app.cjs")
await runApp({ common, core, nevo, label: "ESM" })
