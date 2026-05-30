import { DiscoveryAnnouncement, DiscoveryEntry } from "./types"

function entryKey(serviceName: string, instanceId: string): string {
  return `${serviceName}::${instanceId}`
}

export class DiscoveryRegistry {
  private readonly services = new Map<string, DiscoveryEntry>()
  private pruneTimer?: NodeJS.Timeout
  private ttlMs: number = 15000

  update(announcement: DiscoveryAnnouncement) {
    if (!announcement?.serviceName) return
    const instanceId = announcement.instanceId || announcement.clientId || announcement.serviceName
    const key = entryKey(announcement.serviceName, instanceId)
    const entry: DiscoveryEntry = {
      ...announcement,
      instanceId,
      lastSeen: Date.now()
    }
    this.services.set(key, entry)
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
      if (now - entry.lastSeen > ttlMs) {
        this.services.delete(key)
      }
    }
  }

  list(): DiscoveryEntry[] {
    return this.services.values().toArray()
  }

  listByService(serviceName: string): DiscoveryEntry[] {
    return this.services.values().filter((e) => e.serviceName === serviceName).toArray()
  }

  isAvailable(serviceName: string, ttlMs: number = this.ttlMs): boolean {
    const now = Date.now()
    return this.services.values().some((e) => e.serviceName === serviceName && now - e.lastSeen <= ttlMs)
  }

  /**
   * Remove a single instance by `serviceName + instanceId`. Used by external
   * discovery providers (Consul, Kubernetes DNS, …) to evict entries the
   * upstream source no longer reports.
   */
  removeInstance(serviceName: string, instanceId: string): boolean {
    return this.services.delete(`${serviceName}::${instanceId}`)
  }

  /** List instance ids currently registered for a service. */
  listInstanceIdsFor(serviceName: string): string[] {
    const out: string[] = []
    for (const entry of this.services.values()) {
      if (entry.serviceName === serviceName) out.push(entry.instanceId)
    }
    return out
  }
}
