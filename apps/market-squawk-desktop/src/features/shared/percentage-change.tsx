import { groupDecimal } from "@/lib/formatters"
import { cn } from "@/lib/utils"

/** Direction applies only to a financial change, never to an accounting amount or action. */
export function PercentageChange({ value }: { value: string | null }) {
  const direction = value === null ? "unavailable" : !/[1-9]/.test(value) ? "unchanged"
    : value.startsWith("-") ? "loss" : "gain"
  const text = value === null ? "Change unavailable" : direction === "unchanged" ? "0%"
    : `${direction === "gain" ? "+" : ""}${groupDecimal(value)}%`
  const label = direction === "unavailable" ? text : direction === "unchanged" ? `Unchanged: ${text}`
    : `${direction === "gain" ? "Gain" : "Loss"}: ${text}`
  return <span className={cn("font-mono tabular-nums [overflow-wrap:anywhere]", direction === "gain" ? "text-[var(--success)]"
    : direction === "loss" ? "text-destructive" : "text-muted-foreground")}>
    <span className="sr-only">{label}</span><span aria-hidden="true">{text}</span>
  </span>
}
