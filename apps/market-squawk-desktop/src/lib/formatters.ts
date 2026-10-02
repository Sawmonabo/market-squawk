export interface MoneyValue {
  amount: string
  currency: string
}

export function formatMoney(value: MoneyValue): string {
  return `${value.currency.toUpperCase()} ${groupDecimal(value.amount)}`
}

/** Group exact decimals; optional precision and percent units affect display only. */
export function groupDecimal(value: string, options?: { maximumFractionDigits: number; style?: "percent" }): string {
  const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(value)
  if (!match) return value
  let sign = match[1] ?? ""
  let integer = match[2] ?? ""
  let fraction = match[3] ?? ""
  if (options?.style === "percent") {
    // Move the decimal point in a unit-rate string, retaining every original digit.
    const digits = integer + fraction.padEnd(2, "0")
    const decimalPosition = integer.length + 2
    integer = digits.slice(0, decimalPosition).replace(/^0+(?=\d)/, "")
    fraction = digits.slice(decimalPosition)
  }
  if (options !== undefined && fraction.length > options.maximumFractionDigits) {
    const precision = options.maximumFractionDigits
    const kept = fraction.slice(0, precision)
    // Round the display half away from zero without converting financial values to Number.
    const rounded = BigInt(integer + kept) + (fraction[precision]! >= "5" ? 1n : 0n)
    const digits = String(rounded).padStart(precision + 1, "0")
    integer = precision === 0 ? digits : digits.slice(0, -precision)
    fraction = precision === 0 ? "" : digits.slice(-precision)
    if (rounded === 0n) sign = ""
  }
  return `${sign}${integer.replace(/\B(?=(\d{3})+(?!\d))/g, ",")}${fraction ? `.${fraction}` : ""}${options?.style === "percent" ? "%" : ""}`
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
