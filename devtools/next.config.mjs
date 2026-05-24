import path from "node:path"
import { fileURLToPath } from "node:url"

const here = path.dirname(fileURLToPath(import.meta.url))

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,

  experimental: {
    serverActions: { allowedOrigins: ["*"] }
  },

  // Next.js 16 defaults to Turbopack. Pin its workspace root to this directory
  // so it does not climb up to the parent repo (which has its own pnpm-lock.yaml).
  turbopack: {
    root: here
  },

  // Keep the framework as a runtime CommonJS require on the server side.
  //
  // The framework's barrel re-exports every transport, and each transport
  // statically references `@nestjs/microservices`, `@nestjs/platform-fastify`,
  // etc. Those packages have lazy `loadPackage()` guards around their own
  // optional deps (`nats`, `kafkajs`, `@fastify/static`, …) — bundlers walk
  // them anyway when transpiling.
  //
  // Marking the framework (and its NestJS deps) as external skips the walk:
  // Next emits a `require()` at runtime, and the lazy guards do their job.
  serverExternalPackages: [
    "@riaskov/nevo-messaging",
    "@nestjs/common",
    "@nestjs/core",
    "@nestjs/microservices",
    "@nestjs/platform-fastify",
    "@nats-io/transport-node",
    "@nats-io/nats-core",
    "@nats-io/jetstream",
    "kafkajs",
    "@fastify/static"
  ]
}

export default nextConfig
