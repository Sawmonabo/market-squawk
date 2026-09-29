import { groupDecimal } from "@/lib/formatters"

import type { PortfolioAccount } from "./portfolio-contracts"

export function formatProductTime(value: string | null): string {
  if (value === null) return "Not recorded"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "Not recorded"
  return Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date)
}

export function portfolioDisplayName(account: PortfolioAccount): string {
  return account.portfolioName === account.accountName
    ? account.portfolioName
    : `${account.portfolioName} · ${account.accountName}`
}

export function investmentDisplayName(investment: {
  name: string
  symbol: string | null
}): string {
  return investment.symbol ? `${investment.name} (${investment.symbol})` : investment.name
}

/** Shift an exact unit-rate string to percentage units without floating point. */
export function formatPortfolioRate(value: string | undefined): string {
  if (value === undefined) return "Not available"
  const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(value)
  if (!match) return "Not available"
  const sign = match[1] ?? ""
  const integer = match[2] ?? "0"
  const fraction = match[3] ?? ""
  const digits = integer + fraction.padEnd(2, "0")
  const decimalPosition = integer.length + 2
  const whole = digits.slice(0, decimalPosition).replace(/^0+(?=\d)/, "")
  const remainder = digits.slice(decimalPosition)
  return `${groupDecimal(`${sign}${whole}${remainder ? `.${remainder}` : ""}`)}%`
}
