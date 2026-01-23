import { AccessControlConfig, MessageMeta } from "./types"
import { ErrorCode } from "./error-code"

function decodeJwtPayload(token: string): Record<string, unknown> | null {
  const parts = token.split(".")
  if (parts.length < 2) {
    return null
  }

  try {
    const payload = parts[1].replace(/-/g, "+").replace(/_/g, "/")
    const padded = payload.padEnd(payload.length + ((4 - (payload.length % 4)) % 4), "=")
    const json = Buffer.from(padded, "base64").toString("utf8")
    return JSON.parse(json)
  } catch {
    return null
  }
}

export function extractCallerService(meta?: MessageMeta): string | undefined {
  if (meta?.service) {
    return meta.service
  }

  const token = meta?.auth?.token
  if (!token) {
    return undefined
  }

  const payload = decodeJwtPayload(token)
  if (!payload) {
    return undefined
  }

  const serviceName = (payload["serviceName"] || payload["service"] || payload["svc"] || payload["sub"]) as string | undefined

  return serviceName
}

function matchPattern(pattern: string | undefined, value: string): boolean {
  if (!pattern || pattern === "*") {
    return true
  }
  return pattern === value
}

function listHasValue(list: string[] | undefined, value: string | undefined): boolean {
  if (!list || list.length === 0) {
    return false
  }
  if (list.includes("*")) {
    return true
  }
  if (!value) {
    return false
  }
  return list.includes(value)
}

export function isAccessAllowed(config: AccessControlConfig | undefined, topic: string, method: string, callerService: string | undefined): boolean {
  if (!config) {
    return true
  }

  const allowAllByDefault = config.allowAllByDefault !== false
  const rules = config.rules || []

  let matched = false

  for (const rule of rules) {
    if (!matchPattern(rule.topic, topic) || !matchPattern(rule.method, method)) {
      continue
    }

    matched = true

    if (listHasValue(rule.deny, callerService)) {
      return false
    }

    if (rule.allow && rule.allow.length > 0) {
      return listHasValue(rule.allow, callerService)
    }
  }

  return matched ? allowAllByDefault : allowAllByDefault
}

export function logAccessDenied(config: AccessControlConfig | undefined, details: Record<string, unknown>) {
  if (config?.logDenied === false) {
    return
  }
  console.warn("[NevoMessaging][ACL] Access denied:", details)
}

export function createAccessDeniedError(method: string, serviceName: string, callerService?: string) {
  return {
    code: ErrorCode.UNAUTHORIZED,
    message: "Access denied",
    details: {
      method,
      serviceName,
      callerService
    },
    service: serviceName
  }
}
