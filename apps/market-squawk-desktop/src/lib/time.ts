const NANOSECONDS_PER_MILLISECOND = 1_000_000n

const localDateTime = new Intl.DateTimeFormat(undefined, {
  year: "numeric", month: "short", day: "numeric",
  hour: "numeric", minute: "2-digit", second: "2-digit", timeZoneName: "short",
})
const calendarDate = new Intl.DateTimeFormat(undefined, {
  year: "numeric", month: "short", day: "numeric", timeZone: "UTC",
})

export function timestampFromUnixNanos(value: string | bigint): Date | null {
  try {
    const nanos = typeof value === "bigint" ? value : BigInt(value)
    const milliseconds = nanos / NANOSECONDS_PER_MILLISECOND
    const asNumber = Number(milliseconds)
    if (!Number.isSafeInteger(asNumber)) return null
    const date = new Date(asNumber)
    return Number.isNaN(date.valueOf()) ? null : date
  } catch {
    return null
  }
}

export function formatTimestamp(value: string | bigint): string {
  const date = timestampFromUnixNanos(value)
  return date === null ? "Unavailable" : localDateTime.format(date)
}

/** Presents an instant locally; callers retain its exact source in time/title attributes. */
export function formatProductTimestamp(value: string): string {
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return formatCalendarDate(value)
  const date = new Date(value)
  return Number.isNaN(date.valueOf()) ? "Unavailable" : localDateTime.format(date)
}

/** A trading or financial date is a calendar coordinate, not a local-time instant. */
export function formatCalendarDate(value: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return "Unavailable"
  const date = new Date(`${value}T00:00:00Z`)
  if (Number.isNaN(date.valueOf()) || date.toISOString().slice(0, 10) !== value) return "Unavailable"
  return calendarDate.format(date)
}

export function isStale(
  receivedAtUnixNanos: string | bigint,
  maximumAgeMilliseconds: number,
  now = Date.now(),
): boolean {
  const receivedAt = timestampFromUnixNanos(receivedAtUnixNanos)
  return !receivedAt || now - receivedAt.valueOf() > maximumAgeMilliseconds
}
