import type { MarketHistoryResult } from "./market-history"

export function MarketHistoryChart({ result }: { result: MarketHistoryResult | null }) {
  if (!result?.data) {
    return (
      <section className="mt-5 rounded-xl border border-border bg-card/30 p-5">
        <h3 className="text-sm font-semibold">Price history is unavailable</h3>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">Historical prices cannot be shown right now.</p>
      </section>
    )
  }

  const history = result.data
  return (
    <section className="mt-5 rounded-xl border border-border bg-card/30 p-5">
      <h3 className="text-base font-semibold">Daily price history</h3>
      <p className="mt-2 text-xs leading-5 text-muted-foreground">
        {history.partial ? "Part of the available history is shown." : "The available history is shown."}
      </p>
      <ol className="mt-4 divide-y divide-border" aria-label="Daily closing prices">
        {history.bars.slice(-30).map((bar) => (
          <li key={bar.startsAt} className="flex justify-between gap-4 py-2 text-xs">
            <time dateTime={bar.startsAt}>{formatDate(bar.startsAt)}</time>
            <span className="font-mono">{bar.close} {history.currency}</span>
          </li>
        ))}
      </ol>
    </section>
  )
}

function formatDate(value: string): string {
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? "Date unavailable" : parsed.toLocaleDateString()
}
