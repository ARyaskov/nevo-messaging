const SQL_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/

/** Table/schema names are interpolated into DDL/DML, so only plain identifiers are accepted. */
export function assertSqlIdentifier(value: string, label: string): string {
  if (typeof value !== "string" || !SQL_IDENTIFIER.test(value)) {
    throw new Error(`Invalid SQL identifier for ${label}: ${JSON.stringify(value)} (expected ${SQL_IDENTIFIER})`)
  }
  return value
}

/** Accepts `table` or `schema.table`. */
export function assertQualifiedSqlIdentifier(value: string, label: string): string {
  const parts = typeof value === "string" ? value.split(".") : [value]
  if (parts.length > 2) throw new Error(`Invalid SQL identifier for ${label}: ${JSON.stringify(value)}`)
  for (const part of parts) assertSqlIdentifier(part, label)
  return value
}
