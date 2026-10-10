import { formatTimestamp as formatUnixNanos } from "@/lib/time"

export { formatUnixNanos }

/** Displays a saved money amount exactly, including its sign and currency. */
export function formatMoney(value: { amount: string; currency: string }): string {
  return `${value.amount} ${value.currency}`
}

/** Formats a retained integer without passing it through JavaScript's Number type. */
export function formatLosslessInteger(value: string): string {
  try {
    return BigInt(value).toLocaleString("en-US")
  } catch {
    return value
  }
}
