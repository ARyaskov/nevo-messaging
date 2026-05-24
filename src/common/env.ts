const PROD_VALUES = new Set(["production", "prod"])

function detect(): boolean {
  const nodeEnv = process.env["NODE_ENV"]
  const mode = process.env["MODE"]
  if (nodeEnv && PROD_VALUES.has(nodeEnv.toLowerCase())) return true
  if (mode && PROD_VALUES.has(mode.toLowerCase())) return true
  return false
}

export const IS_PROD: boolean = detect()
export const IS_DEV: boolean = !IS_PROD

export function isProduction(): boolean {
  return IS_PROD
}

export function isDevelopment(): boolean {
  return IS_DEV
}
