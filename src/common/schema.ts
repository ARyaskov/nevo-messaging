import "reflect-metadata"
import { MessagingError } from "./errors"
import { ErrorCode } from "./error-code"

export const SCHEMA_METADATA_KEY = "nevo:schema"

export interface SchemaValidator<T = unknown> {
  parse(input: unknown): T
}

export interface SchemaLike {
  parse?: (input: unknown) => unknown
  safeParse?: (input: unknown) => { success: boolean; data?: unknown; error?: unknown }
  validate?: (input: unknown) => unknown
}

export function toValidator(schema: unknown): SchemaValidator | null {
  if (!schema) return null
  const s = schema as SchemaLike

  if (typeof s.parse === "function") {
    return { parse: (input) => (s.parse as Function).call(s, input) }
  }
  if (typeof s.safeParse === "function") {
    return {
      parse: (input) => {
        const result = (s.safeParse as Function).call(s, input)
        if (!result.success) {
          throw new MessagingError(ErrorCode.VALIDATION_FAILED, {
            message: "Schema validation failed",
            errors: serializeZodIssues(result.error)
          })
        }
        return result.data
      }
    }
  }
  if (typeof s.validate === "function") {
    return { parse: (input) => (s.validate as Function).call(s, input) }
  }
  if (typeof schema === "function") {
    const Ctor = schema as new () => unknown
    return classValidatorAdapter(Ctor)
  }
  return null
}

function classValidatorAdapter(Ctor: new () => unknown): SchemaValidator | null {
  try {
    const { validateSync } = require("class-validator")
    const { plainToInstance } = require("class-transformer")
    return {
      parse: (input) => {
        const instance = plainToInstance(Ctor, input)
        const errors = validateSync(instance as object)
        if (errors.length) {
          throw new MessagingError(ErrorCode.VALIDATION_FAILED, {
            message: "Validation failed",
            errors: errors.map((e: any) => ({ property: e.property, constraints: e.constraints }))
          })
        }
        return instance
      }
    }
  } catch {
    return null
  }
}

function serializeZodIssues(err: unknown): unknown {
  if (!err) return err
  if (typeof err === "object" && err !== null && "issues" in (err as object)) {
    return (err as any).issues
  }
  return err
}

export function Schema(schema: unknown): MethodDecorator {
  return (target, propertyKey) => {
    const ctorOrTarget: any = (target as any).constructor ?? target
    const existing = (Reflect.getMetadata(SCHEMA_METADATA_KEY, ctorOrTarget) as Map<string, unknown> | undefined) ?? new Map<string, unknown>()
    existing.set(propertyKey as string, schema)
    Reflect.defineMetadata(SCHEMA_METADATA_KEY, existing, ctorOrTarget)
  }
}

export function getSchemaFor(target: any, propertyKey: string): unknown | undefined {
  const ctorOrTarget: any = target?.constructor ?? target
  const map = Reflect.getMetadata(SCHEMA_METADATA_KEY, ctorOrTarget) as Map<string, unknown> | undefined
  return map?.get(propertyKey)
}
