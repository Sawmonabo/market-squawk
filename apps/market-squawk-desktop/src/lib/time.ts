const NANOSECONDS_PER_MILLISECOND = 1_000_000n

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
  return timestampFromUnixNanos(value)?.toLocaleString() ?? "Unavailable"
}

export function isStale(
  receivedAtUnixNanos: string | bigint,
  maximumAgeMilliseconds: number,
  now = Date.now(),
): boolean {
  const receivedAt = timestampFromUnixNanos(receivedAtUnixNanos)
  return !receivedAt || now - receivedAt.valueOf() > maximumAgeMilliseconds
}
