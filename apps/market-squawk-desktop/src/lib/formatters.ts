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
