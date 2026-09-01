import { DiscoveryAnnouncement, DiscoveryEntry } from "./types"
import { getDefaultLogger } from "./logger"

// Announcements carry a caller-controlled serviceName/instanceId, so the registry
// is bounded independently of pruning.
export const DEFAULT_MAX_DISCOVERY_ENTRIES = 4096

export interface DiscoveryRegistryOptions {
  maxEntries?: number
}

function entryKey(serviceName: string, instanceId: string): string {
  return `${serviceName}::${instanceId}`
}

export class DiscoveryRegistry {
  private readonly services = new Map<string, DiscoveryEntry>()
  private readonly byService = new Map<string, Set<string>>()
  private readonly maxEntries: number
  private pruneTimer?: NodeJS.Timeout
  private ttlMs: number = 15000
  private capacityWarned = false

  constructor(opts?: DiscoveryRegistryOptions) {
    this.maxEntries = Math.max(1, opts?.maxEntries ?? DEFAULT_MAX_DISCOVERY_ENTRIES)
  }

  update(announcement: DiscoveryAnnouncement) {
    if (!announcement?.serviceName) return
    const instanceId = announcement.instanceId || announcement.clientId || announcement.serviceName
    const key = entryKey(announcement.serviceName, instanceId)
    const entry: DiscoveryEntry = {
      ...announcement,
      instanceId,
      lastSeen: Date.now()
    }
    const isNew = !this.services.has(key)
    if (isNew) this.evictIfFull()
    else this.services.delete(key)
    this.services.set(key, entry)
    this.indexAdd(announcement.serviceName, key)
  }

  startBackgroundPrune(ttlMs: number, intervalMs: number = Math.max(1000, Math.floor(ttlMs / 3))): void {
    this.ttlMs = ttlMs
    this.stopBackgroundPrune()
    this.pruneTimer = setInterval(() => this.prune(ttlMs), intervalMs)
    if (typeof this.pruneTimer.unref === "function") this.pruneTimer.unref()
  }

  stopBackgroundPrune(): void {
    if (this.pruneTimer) {
      clearInterval(this.pruneTimer)
      this.pruneTimer = undefined
    }
  }

  prune(ttlMs: number = this.ttlMs) {
    const now = Date.now()
    for (const [key, entry] of this.services.entries()) {
      if (now - entry.lastSeen > ttlMs) this.drop(key, entry.serviceName)
    }
  }

  list(): DiscoveryEntry[] {
    return this.services.values().toArray()
  }

  listByService(serviceName: string): DiscoveryEntry[] {
    const keys = this.byService.get(serviceName)
    if (!keys) return []
    const out: DiscoveryEntry[] = []
    for (const key of keys) {
      const entry = this.services.get(key)
      if (entry) out.push(entry)
    }
    return out
  }

  isAvailable(serviceName: string, ttlMs: number = this.ttlMs): boolean {
    const keys = this.byService.get(serviceName)
    if (!keys) return false
    const now = Date.now()
    for (const key of keys) {
      const entry = this.services.get(key)
      if (entry && now - entry.lastSeen <= ttlMs) return true
    }
    return false
  }

  /** Remove a single instance by `serviceName + instanceId`. */
  removeInstance(serviceName: string, instanceId: string): boolean {
    const key = entryKey(serviceName, instanceId)
    if (!this.services.has(key)) return false
    this.drop(key, serviceName)
    return true
  }

  /** List instance ids currently registered for a service. */
  listInstanceIdsFor(serviceName: string): string[] {
    return this.listByService(serviceName).map((entry) => entry.instanceId)
  }

  size(): number {
    return this.services.size
  }

  private indexAdd(serviceName: string, key: string): void {
    let keys = this.byService.get(serviceName)
    if (!keys) {
      keys = new Set()
      this.byService.set(serviceName, keys)
    }
    keys.add(key)
  }

  private drop(key: string, serviceName: string): void {
    this.services.delete(key)
    const keys = this.byService.get(serviceName)
    if (!keys) return
    keys.delete(key)
    if (keys.size === 0) this.byService.delete(serviceName)
  }

  // Stale entries go first, so forged announcements can't push out live peers.
  private evictIfFull(): void {
    if (this.services.size < this.maxEntries) return
    const now = Date.now()
    for (const [key, entry] of this.services.entries()) {
      if (now - entry.lastSeen > this.ttlMs) {
        this.drop(key, entry.serviceName)
        if (this.services.size < this.maxEntries) return
      }
    }
    while (this.services.size >= this.maxEntries) {
      const oldest = this.services.entries().next().value as [string, DiscoveryEntry] | undefined
      if (!oldest) return
      this.drop(oldest[0], oldest[1].serviceName)
    }
    if (!this.capacityWarned) {
      this.capacityWarned = true
      try {
        getDefaultLogger().warn(
          { event: "discovery.capacity_reached", maxEntries: this.maxEntries },
          "DiscoveryRegistry is at capacity; live instances are being evicted. Raise maxEntries or restrict who may publish announcements."
        )
      } catch {}
    }
  }
}
