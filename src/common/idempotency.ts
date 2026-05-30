import type { IdempotencyOptions } from "./types"

function detachBuffers(value: unknown, depth = 0): unknown {
  if (depth > 4 || value == null) return value
  if (value instanceof Uint8Array) {
    if (value.byteOffset !== 0 || value.byteLength !== value.buffer.byteLength) {
      const copy = Buffer.allocUnsafeSlow(value.byteLength)
      copy.set(value)
      return copy
    }
    return value
  }
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) value[i] = detachBuffers(value[i], depth + 1)
    return value
  }
  if (typeof value === "object") {
    for (const k of Object.keys(value as object)) {
      (value as Record<string, unknown>)[k] = detachBuffers((value as Record<string, unknown>)[k], depth + 1)
    }
  }
  return value
}

interface Node<T> {
  key: string
  value: T
  expiresAt: number
  prev: Node<T> | null
  next: Node<T> | null
}

export class LruIdempotencyCache<T = unknown> {
  private readonly map = new Map<string, Node<T>>()
  private head: Node<T> | null = null
  private tail: Node<T> | null = null
  private readonly maxEntries: number
  private readonly ttlMs: number
  private readonly enabled: boolean

  constructor(opts?: IdempotencyOptions) {
    this.enabled = opts?.enabled !== false
    this.maxEntries = opts?.maxEntries ?? 10000
    this.ttlMs = opts?.ttlMs ?? 5 * 60_000
  }

  isEnabled(): boolean { return this.enabled }
  size(): number { return this.map.size }

  private detach(node: Node<T>): void {
    if (node.prev) node.prev.next = node.next
    else this.head = node.next
    if (node.next) node.next.prev = node.prev
    else this.tail = node.prev
    node.prev = null
    node.next = null
  }

  private moveToHead(node: Node<T>): void {
    if (this.head === node) return
    this.detach(node)
    node.next = this.head
    if (this.head) this.head.prev = node
    this.head = node
    if (!this.tail) this.tail = node
  }

  private appendHead(node: Node<T>): void {
    node.next = this.head
    if (this.head) this.head.prev = node
    this.head = node
    if (!this.tail) this.tail = node
  }

  private dropTail(): void {
    if (!this.tail) return
    const node = this.tail
    this.detach(node)
    this.map.delete(node.key)
  }

  has(key: string): boolean {
    if (!this.enabled) return false
    const node = this.map.get(key)
    if (!node) return false
    if (Date.now() > node.expiresAt) {
      this.detach(node)
      this.map.delete(key)
      return false
    }
    this.moveToHead(node)
    return true
  }

  get(key: string): T | undefined {
    if (!this.enabled) return undefined
    const node = this.map.get(key)
    if (!node) return undefined
    if (Date.now() > node.expiresAt) {
      this.detach(node)
      this.map.delete(key)
      return undefined
    }
    this.moveToHead(node)
    return node.value
  }

  set(key: string, value: T): void {
    if (!this.enabled) return
    const detached = detachBuffers(value) as T
    const existing = this.map.get(key)
    // Wall-clock (Date.now), matching the replay-guard freshness window, so the dedup
    // TTL and the window are measured on one consistent clock.
    const expiresAt = Date.now() + this.ttlMs
    if (existing) {
      existing.value = detached
      existing.expiresAt = expiresAt
      this.moveToHead(existing)
      return
    }
    const node: Node<T> = { key, value: detached, expiresAt, prev: null, next: null }
    this.map.set(key, node)
    this.appendHead(node)
    if (this.map.size > this.maxEntries) this.dropTail()
  }

  /**
   * Remove `key` entirely (unlink the node + drop the map entry) so a later
   * {@link has}/{@link get} reports absent. Distinct from `set(key, undefined)`,
   * which would leave a live tombstone node that `has` still counts as present.
   * Returns true when an entry was removed.
   */
  delete(key: string): boolean {
    const node = this.map.get(key)
    if (!node) return false
    this.detach(node)
    this.map.delete(key)
    return true
  }

  clear(): void {
    this.map.clear()
    this.head = null
    this.tail = null
  }
}
