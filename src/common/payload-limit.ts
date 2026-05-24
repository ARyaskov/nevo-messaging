import { PayloadTooLargeError } from "./errors"

export const DEFAULT_MAX_PAYLOAD_BYTES = 4 * 1024 * 1024

export function enforcePayloadLimit(buf: Uint8Array | string, limit = DEFAULT_MAX_PAYLOAD_BYTES): void {
  const size = typeof buf === "string" ? Buffer.byteLength(buf, "utf8") : buf.byteLength
  if (size > limit) {
    throw new PayloadTooLargeError(size, limit)
  }
}
