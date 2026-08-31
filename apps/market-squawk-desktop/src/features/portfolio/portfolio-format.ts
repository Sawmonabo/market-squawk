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
