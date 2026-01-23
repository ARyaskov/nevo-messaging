import { DiscoveryAnnouncement, DiscoveryEntry } from "./types"

export class DiscoveryRegistry {
  private readonly services = new Map<string, DiscoveryEntry>()

  update(announcement: DiscoveryAnnouncement) {
    const now = Date.now()
    const entry: DiscoveryEntry = {
      ...announcement,
      lastSeen: now
    }
    this.services.set(announcement.serviceName, entry)
  }

  prune(ttlMs: number) {
    const now = Date.now()
    for (const [serviceName, entry] of this.services.entries()) {
      if (now - entry.lastSeen > ttlMs) {
        this.services.delete(serviceName)
      }
    }
  }

  list(): DiscoveryEntry[] {
    return [...this.services.values()]
  }

  isAvailable(serviceName: string, ttlMs: number): boolean {
    const entry = this.services.get(serviceName)
    if (!entry) {
      return false
    }
    return Date.now() - entry.lastSeen <= ttlMs
  }
}
