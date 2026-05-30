// 5-field cron parser: "minute hour day month weekday".
// Supports `*`, `,`, `-`, `/`. No `L`, `#`, `?` (Quartz extensions).
//
// Returns the next firing timestamp at or after `from`.
//
// Timezone: by default schedules are evaluated in the server's LOCAL time
// (matching `Date` getters). Pass `{ utc: true }` to evaluate in UTC, or
// `{ timezone: "Area/City" }` for any IANA zone (resolved via
// `Intl.DateTimeFormat`). DST is handled by mapping wall-clock times back to
// real epoch instants; wall-clock times that don't exist on a spring-forward
// day are skipped.

interface CronField {
  values: Set<number>
  // True only when the field was literally "*". Needed for the POSIX
  // day-of-month vs day-of-week OR rule (see `dayMatches`). "*/2" is NOT a
  // wildcard — it restricts the field.
  wildcard: boolean
}

interface ParsedCron {
  minute: CronField
  hour: CronField
  day: CronField
  month: CronField
  weekday: CronField
}

export interface CronOptions {
  /** IANA timezone, e.g. "America/New_York". Defaults to server local time. */
  timezone?: string
  /** Evaluate in UTC. Shorthand for `timezone: "UTC"`. */
  utc?: boolean
}

const RANGES = {
  minute: [0, 59],
  hour: [0, 23],
  day: [1, 31],
  month: [1, 12],
  weekday: [0, 6]
} as const

function parseField(spec: string, name: keyof typeof RANGES): CronField {
  const [min, max] = RANGES[name]
  const values = new Set<number>()
  for (const part of spec.split(",")) {
    const [rangePart, stepPart] = part.split("/")
    const step = stepPart ? Number(stepPart) : 1
    if (!Number.isFinite(step) || step < 1) throw new Error(`cron: bad step in "${spec}"`)
    let from: number, to: number
    if (rangePart === "*") {
      from = min
      to = max
    } else if (rangePart.includes("-")) {
      const [a, b] = rangePart.split("-").map(Number)
      from = a
      to = b
    } else {
      const v = Number(rangePart)
      from = v
      to = v
    }
    if (!Number.isFinite(from) || !Number.isFinite(to) || from < min || to > max || from > to) {
      throw new Error(`cron: out-of-range "${spec}" for ${name}`)
    }
    for (let v = from; v <= to; v += step) values.add(v)
  }
  return { values, wildcard: spec === "*" }
}

export function parseCron(expr: string): ParsedCron {
  const parts = expr.trim().split(/\s+/)
  if (parts.length !== 5) throw new Error(`cron: expected 5 fields, got ${parts.length}: "${expr}"`)
  return {
    minute: parseField(parts[0], "minute"),
    hour: parseField(parts[1], "hour"),
    day: parseField(parts[2], "day"),
    month: parseField(parts[3], "month"),
    weekday: parseField(parts[4], "weekday")
  }
}

interface WallClock {
  year: number
  month: number   // 1-12
  day: number     // 1-31
  hour: number    // 0-23
  minute: number  // 0-59
  weekday: number // 0-6, Sunday = 0
}

const MINUTE = 60_000

// Wall-clock fields for `epochMs` in the target timezone (or server local time
// when `tz` is undefined). Resolution is minutes; seconds are ignored.
function wallClock(epochMs: number, tz?: string): WallClock {
  if (!tz) {
    const d = new Date(epochMs)
    return {
      year: d.getFullYear(),
      month: d.getMonth() + 1,
      day: d.getDate(),
      hour: d.getHours(),
      minute: d.getMinutes(),
      weekday: d.getDay()
    }
  }
  const p = zonedParts(epochMs, tz)
  return {
    ...p,
    // A calendar date maps to a fixed weekday independent of timezone.
    weekday: new Date(Date.UTC(p.year, p.month - 1, p.day)).getUTCDay()
  }
}

interface ZonedParts {
  year: number
  month: number
  day: number
  hour: number
  minute: number
}

function zonedParts(epochMs: number, tz: string): ZonedParts {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23"
  })
  const out: Record<string, number> = {}
  for (const part of fmt.formatToParts(new Date(epochMs))) {
    if (part.type !== "literal") out[part.type] = Number(part.value)
  }
  return { year: out.year, month: out.month, day: out.day, hour: out.hour, minute: out.minute }
}

// Epoch ms for a wall-clock minute, interpreted in `tz` (or local when undefined).
function toEpoch(year: number, month: number, day: number, hour: number, minute: number, tz?: string): number {
  if (!tz) return new Date(year, month - 1, day, hour, minute, 0, 0).getTime()
  const asUTC = Date.UTC(year, month - 1, day, hour, minute, 0, 0)
  // Two-pass offset resolution so we land on the correct side of a DST change.
  const offset1 = tzOffsetMs(asUTC, tz)
  let epoch = asUTC - offset1
  const offset2 = tzOffsetMs(epoch, tz)
  if (offset2 !== offset1) epoch = asUTC - offset2
  return epoch
}

// How far ahead of UTC the zone's wall clock runs, in ms, at `epochMs`.
function tzOffsetMs(epochMs: number, tz: string): number {
  const p = zonedParts(epochMs, tz)
  const asUTC = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, 0, 0)
  return asUTC - Math.floor(epochMs / MINUTE) * MINUTE
}

function dayMatches(parsed: ParsedCron, wc: WallClock): boolean {
  if (!parsed.month.values.has(wc.month)) return false
  const domRestricted = !parsed.day.wildcard
  const dowRestricted = !parsed.weekday.wildcard
  const domHit = parsed.day.values.has(wc.day)
  const dowHit = parsed.weekday.values.has(wc.weekday)
  // POSIX: when BOTH day-of-month and day-of-week are restricted, the day
  // matches if EITHER field matches (logical OR) — e.g. "0 0 13 * 5" fires on
  // the 13th OR any Friday. When only one is restricted, it applies and the
  // wildcard field matches everything.
  if (domRestricted && dowRestricted) return domHit || dowHit
  if (domRestricted) return domHit
  if (dowRestricted) return dowHit
  return true
}

// 00:00 of the calendar day after `wc`, as wall-clock fields in `tz`.
function nextDayStart(wc: WallClock, tz?: string): WallClock {
  const base = new Date(Date.UTC(wc.year, wc.month - 1, wc.day))
  base.setUTCDate(base.getUTCDate() + 1)
  const epoch = toEpoch(base.getUTCFullYear(), base.getUTCMonth() + 1, base.getUTCDate(), 0, 0, tz)
  return wallClock(epoch, tz)
}

/** Next firing at-or-after `from`, in ms epoch. Returns -1 if none in next ~4 years. */
export function nextCronTick(expr: string, from: number, opts: CronOptions = {}): number {
  const parsed = parseCron(expr)
  const tz = opts.utc ? "UTC" : opts.timezone

  const hours = [...parsed.hour.values].sort((a, b) => a - b)
  const minutes = [...parsed.minute.values].sort((a, b) => a - b)

  // First candidate minute strictly after `from`.
  const fromMinute = Math.floor(from / MINUTE) * MINUTE + MINUTE

  let wc = wallClock(fromMinute, tz)
  // On the first day, don't consider times earlier than `fromMinute`.
  let lowerHour = wc.hour
  let lowerMinute = wc.minute

  // Step day-by-day (≈1.5k iterations for a 4-year horizon) and only resolve
  // the hour/minute on days that match — far cheaper than the old
  // minute-by-minute scan (worst case ≈2.1M iterations for rare schedules).
  const MAX_DAYS = 366 * 4 + 2
  for (let d = 0; d < MAX_DAYS; d++) {
    if (dayMatches(parsed, wc)) {
      for (const h of hours) {
        if (h < lowerHour) continue
        for (const m of minutes) {
          if (h === lowerHour && m < lowerMinute) continue
          const candidate = toEpoch(wc.year, wc.month, wc.day, h, m, tz)
          if (candidate < fromMinute) continue
          // Skip wall-clock times that don't exist (spring-forward gap):
          // `toEpoch` normalizes them forward, so the round-trip won't match.
          const back = wallClock(candidate, tz)
          if (back.hour !== h || back.minute !== m) continue
          return candidate
        }
      }
    }
    wc = nextDayStart(wc, tz)
    lowerHour = 0
    lowerMinute = 0
  }
  return -1
}

/** Quick sanity check. */
export function isValidCron(expr: string): boolean {
  try { parseCron(expr); return true } catch { return false }
}
