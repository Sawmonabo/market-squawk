export interface MoneyValue {
  amount: string
  currency: string
}

export function formatMoney(value: MoneyValue): string {
  return `${value.currency.toUpperCase()} ${groupDecimal(value.amount)}`
}

export function groupDecimal(value: string): string {
  const match = /^(-?)(\d+)(\.\d+)?$/.exec(value)
  if (!match) return value
  const sign = match[1] ?? ""
  const integer = match[2] ?? ""
  const fraction = match[3] ?? ""
  return `${sign}${integer.replace(/\B(?=(\d{3})+(?!\d))/g, ",")}${fraction}`
}

export function humanize(value: string): string {
  const words = value
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .trim()
  return words ? words.charAt(0).toUpperCase() + words.slice(1) : "Value"
}

export function friendlyResearchCollectionName(value: string): string {
  const name = value.toLocaleLowerCase()
  if (name.includes("fund_nav") || name.includes("fund-nav") || name.includes("fund nav")) {
    return "Mutual fund NAV history"
  }
  if (name.includes("option")) return "Options history"
  if (name.includes("macro") || name.includes("economic") || name.includes("rate")) {
    return "Economic indicators"
  }
  if (name.includes("filing") || name.includes("fundamental")) {
    return "Company and fund reports"
  }
  if (name.includes("feature")) return "Model inputs"
  if (name.includes("label") || name.includes("outcome")) return "Model outcomes"
  if (name.includes("bar") || name.includes("price") || name.includes("eod")) {
    return "Market price history"
  }
  return "Research collection"
}
