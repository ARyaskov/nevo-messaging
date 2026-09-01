/**
 * Type surface the dashboard consumes, re-exported from the framework source.
 *
 * The client is bundled from inside this package, so it cannot import
 * "@riaskov/nevo-messaging" by name the way an external app would. These are
 * type-only re-exports — esbuild erases them and nothing lands in the bundle.
 */
export type { DevToolsEvent, DevToolsEventType } from "../../common/devtools"
export type { ServiceInfo, ServiceMethodInfo, CircuitInfo } from "../../common/devtools-registry"
export type { AccessRule, AccessControlConfig } from "../../common/types"
