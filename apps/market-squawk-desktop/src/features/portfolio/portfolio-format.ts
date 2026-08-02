import { formatMoney, humanize } from "@/lib/formatters"

import type { Money } from "./portfolio-contracts"

export function shortIdentity(value: string, label: string) {
  const tail = value.split("-").at(-1)?.slice(-8) ?? value.slice(-8)
  return `${label} ${tail.toUpperCase()}`
}

export function formatPercent(value: string | number | undefined) {
  if (value === undefined) return "Not available"
  const numeric = typeof value === "number" ? value : Number(value)
  if (!Number.isFinite(numeric)) return "Not available"
  return Intl.NumberFormat(undefined, {
    style: "percent",
    maximumFractionDigits: 2,
    minimumFractionDigits: 0,
  }).format(numeric)
}

export function formatTimestamp(unixNanos: string | number | null) {
  if (unixNanos === null) return "Not recorded"
  try {
    const milliseconds =
      typeof unixNanos === "number"
        ? Math.trunc(unixNanos / 1_000_000)
        : Number(BigInt(unixNanos) / 1_000_000n)
    if (!Number.isFinite(milliseconds)) return "Not recorded"
    return Intl.DateTimeFormat(undefined, {
      dateStyle: "medium",
      timeStyle: "short",
    }).format(new Date(milliseconds))
  } catch {
    return "Not recorded"
  }
}

export function compactMoney(value: Money) {
  const numeric = Number(value.amount)
  if (!Number.isFinite(numeric)) return formatMoney(value)
  return `${value.currency.toUpperCase()} ${Intl.NumberFormat(undefined, {
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(numeric)}`
}

export function evidenceLabel(value: string | undefined) {
  return value ? humanize(value) : "Not reported"
}

export function percentageToBasisPoints(value: string): number | null {
  const match = /^(-?)(\d+)(?:\.(\d{1,2}))?$/.exec(value.trim())
  if (!match) return null
  const whole = Number(match[2])
  const fractional = Number((match[3] ?? "").padEnd(2, "0"))
  if (!Number.isSafeInteger(whole) || !Number.isSafeInteger(fractional)) return null
  const unsigned = whole * 100 + fractional
  return match[1] === "-" ? -unsigned : unsigned
}

export function basisPointsToUnitRate(basisPoints: number): string {
  const sign = basisPoints < 0 ? "-" : ""
  const absolute = Math.abs(basisPoints)
  const whole = Math.floor(absolute / 10_000)
  const fraction = String(absolute % 10_000).padStart(4, "0").replace(/0+$/, "")
  return fraction ? `${sign}${whole}.${fraction}` : `${sign}${whole}`
}

export function basisPointsToPercentage(basisPoints: number): string {
  const sign = basisPoints < 0 ? "-" : ""
  const absolute = Math.abs(basisPoints)
  const whole = Math.floor(absolute / 100)
  const fraction = String(absolute % 100).padStart(2, "0").replace(/0+$/, "")
  return fraction ? `${sign}${whole}.${fraction}` : `${sign}${whole}`
}
