const NANOS_PER_SECOND = 1_000_000_000n
const SECONDS_PER_MINUTE = 60n
const MINUTES_PER_HOUR = 60n
const HOURS_PER_DAY = 24n
const NANOS_PER_DAY =
  NANOS_PER_SECOND * SECONDS_PER_MINUTE * MINUTES_PER_HOUR * HOURS_PER_DAY

/** Formats a retained integer without passing it through JavaScript's Number type. */
export function formatLosslessInteger(value: string): string {
  try {
    return BigInt(value).toLocaleString("en-US")
  } catch {
    return value
  }
}

/** Formats an exact Unix-nanosecond timestamp with BigInt-only calendar arithmetic. */
export function formatUnixNanos(value: string): string {
  try {
    const unixNanos = BigInt(value)
    const days = floorDivide(unixNanos, NANOS_PER_DAY)
    const nanosOfDay = unixNanos - days * NANOS_PER_DAY
    const secondsOfDay = nanosOfDay / NANOS_PER_SECOND
    const nanos = nanosOfDay % NANOS_PER_SECOND
    const hour = secondsOfDay / (SECONDS_PER_MINUTE * MINUTES_PER_HOUR)
    const minute =
      (secondsOfDay / SECONDS_PER_MINUTE) % MINUTES_PER_HOUR
    const second = secondsOfDay % SECONDS_PER_MINUTE
    const { year, month, day } = civilDateFromUnixDays(days)

    const date = `${formatYear(year)}-${pad(month, 2)}-${pad(day, 2)}`
    const time = `${pad(hour, 2)}:${pad(minute, 2)}:${pad(second, 2)}`
    return `${date} ${time}.${pad(nanos, 9)} UTC`
  } catch {
    return `${value} nanoseconds from the Unix epoch`
  }
}

function civilDateFromUnixDays(days: bigint): {
  year: bigint
  month: bigint
  day: bigint
} {
  const shifted = days + 719_468n
  const era = floorDivide(shifted, 146_097n)
  const dayOfEra = shifted - era * 146_097n
  const yearOfEra =
    (dayOfEra -
      dayOfEra / 1_460n +
      dayOfEra / 36_524n -
      dayOfEra / 146_096n) /
    365n
  let year = yearOfEra + era * 400n
  const dayOfYear =
    dayOfEra -
    (365n * yearOfEra + yearOfEra / 4n - yearOfEra / 100n)
  const monthPrime = (5n * dayOfYear + 2n) / 153n
  const day = dayOfYear - (153n * monthPrime + 2n) / 5n + 1n
  const month = monthPrime + (monthPrime < 10n ? 3n : -9n)
  if (month <= 2n) year += 1n
  return { year, month, day }
}

function floorDivide(dividend: bigint, divisor: bigint): bigint {
  const quotient = dividend / divisor
  const remainder = dividend % divisor
  return remainder < 0n ? quotient - 1n : quotient
}

function formatYear(value: bigint): string {
  if (value >= 0n && value <= 9_999n) return pad(value, 4)
  const magnitude = value < 0n ? -value : value
  return `${value < 0n ? "-" : "+"}${pad(magnitude, 6)}`
}

function pad(value: bigint, width: number): string {
  return value.toString().padStart(width, "0")
}
