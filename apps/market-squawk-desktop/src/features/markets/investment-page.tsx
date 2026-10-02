import { RefreshButton } from "@/components/ui/refresh-button"
import { useState } from "react"
import { useQuery, useQueryClient, useIsFetching } from "@tanstack/react-query"
import { Link, useParams } from "react-router-dom"

import { useProduct, useSystem } from "@/app/product-context"
import { currentDisplayQueryOptions, productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { AnalysisLaunch } from "@/features/opportunities/analysis-launch"
import { PercentageChange } from "@/features/shared/percentage-change"
import { formatMoney, groupDecimal } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { MarketHistoryRead } from "./market-history-read"
import { InvestmentProfile } from "./investment-profile"
import { InvestmentFinancials } from "./investment-financials"
import { marketAvailabilityLabel, marketPriceBasisLabel, marketSelectionTokenSchema, parseMarketInstrumentResult, type MarketProductRow } from "./market-product"

export function InvestmentPage() {
  const product = useProduct()
  const { selectionToken: requestedSelection } = useParams<{ selectionToken: string }>()
  const selection = marketSelectionTokenSchema.safeParse(requestedSelection)

  if (!selection.success) return <InvestmentUnavailable message="This investment selection is unavailable. Search Markets to choose an investment." />
  if (product.status !== "ready") return <InvestmentUnavailable message="Investment information is unavailable right now." />

  return <SelectedInvestment
    key={`${product.bootstrap.productSessionToken}:${selection.data}`}
    selectionToken={selection.data}
    bootstrap={product.bootstrap}
    transport={product.transport}
  />
}

function SelectedInvestment({ selectionToken, bootstrap, transport }: {
  selectionToken: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const { eventConnection } = useSystem()
  const [showHistory, setShowHistory] = useState(true)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const queryClient = useQueryClient()
  const refresh = () => {
    setRefreshRevision((value) => value + 1)
    void queryClient.refetchQueries({ type: "active", predicate: (query) => {
      const key = query.queryKey
      const input = key[5] as { selectionToken?: string } | undefined
      return key[1] === bootstrap.productSessionToken
        && ((["Market.GetInstrument", "Research.GetInvestmentProfile"].includes(String(key[4]))
          && input?.selectionToken === selectionToken)
          || key[4] === "Desktop.AnalyticalProfiles")
    } })
  }
  const detail = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetInstrument", { selectionToken }),
    ...currentDisplayQueryOptions,
    retry: false,
    refetchOnWindowFocus: false,
    queryFn: async ({ signal }) => {
      const result = await transport.query({ query: "marketInstrument", selectionToken }, { signal })
      if (signal.aborted) throw new DOMException("The view was closed.", "AbortError")
      return parseMarketInstrumentResult(result, selectionToken)
    },
  })
  const pageReads = useIsFetching({ predicate: (query) => {
    const key = query.queryKey
    if (key[1] !== bootstrap.productSessionToken) return false
    const input = key[5] as { selectionToken?: string; historyToken?: string } | undefined
    return input?.selectionToken === selectionToken
      || (input?.historyToken !== undefined && input.historyToken === detail.data?.historyToken)
  } })
  const row = detail.data ?? null
  const disconnected = eventConnection.status !== "connected"
  const unverified = !detail.isFetchedAfterMount || detail.isError || disconnected
  const title = row === null ? "Investment" : [row.identity.symbol, row.identity.name]
    .filter((value, index, values) => value !== null && values.indexOf(value) === index).join(" · ")
  const companyName = row?.identity.name !== row?.identity.symbol ? row?.identity.name : null
  const priceLabels = row === null ? "Checking price information" : [marketPriceBasisLabel(row),
    unverified && row.price !== null ? "Saved price · Freshness not checked" : marketAvailabilityLabel(row)]
    .filter((value, index, values) => value !== null && values.indexOf(value) === index).join(" · ")

  return <main className="mx-auto w-full max-w-[1180px] space-y-4 p-5 lg:p-7">
    <header className="border-b border-border pb-4">
      <Link className="text-xs text-primary underline-offset-4 hover:underline" to="/markets">Back to Markets</Link>
      <div className="mt-3 grid items-start gap-3 sm:grid-cols-[minmax(0,1fr)_auto]">
        <div className="min-w-0">
          <h1 aria-label={title} className="min-w-0">
            <span className="block break-words font-mono text-2xl font-semibold tracking-tight">{row?.identity.symbol ?? row?.identity.name ?? "Investment"}</span>
            {row?.identity.symbol && companyName ? <span className="mt-1 block max-w-[52ch] break-words text-sm font-normal leading-5 text-muted-foreground">{companyName}</span> : null}
          </h1>
        </div>
        <section aria-label="Investment price" className="min-w-0 sm:text-right">
          <div className="flex flex-wrap items-center gap-3 sm:justify-end">
            <h2 className="sr-only">Price</h2>
            <p className="font-mono text-2xl tabular-nums">{row?.price ? formatMoney({ amount: row.price.value, currency: row.price.currency }) : "Price unavailable"}</p>
            <RefreshButton label="Refresh investment" refreshing={pageReads > 0} onClick={refresh} />
          </div>
          <p className="mt-1 min-h-4 text-xs text-muted-foreground">{priceLabels}{row ? <> · <PercentageChange value={row.changePercent} /></> : null}</p>
          <p className="mt-1 min-h-4 text-xs text-muted-foreground">{row?.asOf ? <time dateTime={row.asOf}>{new Date(row.asOf).toLocaleString()}</time> : "Availability not established"}</p>
        </section>
      </div>
      <div className="mt-1 min-h-5 text-xs leading-5">
        {detail.isError ? <p role="alert" className="text-destructive">{row === null
          ? "This investment could not be opened. Try again, or search Markets to choose a fresh selection."
          : "The price could not be refreshed. Showing the last checked information; its freshness is unverified."}</p>
          : disconnected && row !== null ? <p role="status" className="text-muted-foreground">Connection interrupted. Showing the last checked information; its freshness is unverified.</p>
            : detail.isFetching && !detail.isFetchedAfterMount ? <p role="status" className="text-muted-foreground">{row ? "Checking price freshness…" : "Opening the selected investment…"}</p> : null}
      </div>
      {row !== null ? <InvestmentQuote row={row} unverified={unverified} /> : null}
    </header>
    <div className="grid items-start gap-4 xl:grid-cols-[minmax(0,1fr)_320px]">
      <section className="min-w-0 rounded-xl border border-border bg-card/30 p-4" aria-label="Investment price history">
        <div className="flex items-center justify-between gap-3">
          <h2 className="text-base font-semibold">Price history</h2>
          <Button variant="ghost" size="sm" onClick={() => setShowHistory((shown) => !shown)}>{showHistory ? "Hide price history" : "Show price history"}</Button>
        </div>
        {showHistory ? row?.historyToken ? <MarketHistoryRead key={`${bootstrap.productSessionToken}:${row.historyToken}`}
          historyToken={row.historyToken} bootstrap={bootstrap} transport={transport} refreshRevision={refreshRevision} />
          : <div className="mt-3 flex min-h-[640px] items-center justify-center text-sm text-muted-foreground">
            <p role="status">{detail.isFetching && row === null ? "Checking available price history…" : "Price history is unavailable for this investment."}</p>
          </div> : <p className="mt-3 text-xs text-muted-foreground">Price history is hidden. Show it to reopen the chart.</p>}
      </section>
      <aside className="min-w-0 space-y-4" aria-label="Investment details and analysis">
        <InvestmentProfile selectionToken={selectionToken} bootstrap={bootstrap} transport={transport} />
        <section className="rounded-xl border border-border bg-card/30 p-4" aria-label="Investment analysis">
          <h2 className="mb-3 text-base font-semibold">Investment analysis</h2>
          <AnalysisLaunch transport={transport} scope={bootstrap.productSessionToken} selectionToken={selectionToken} />
        </section>
      </aside>
    </div>
    <InvestmentFinancials selectionToken={selectionToken} bootstrap={bootstrap} transport={transport} refreshRevision={refreshRevision} />
  </main>
}

function InvestmentQuote({ row, unverified }: { row: MarketProductRow; unverified: boolean }) {
  const quote = row.quote
  const price = (value: string | null) => value === null || quote === null ? "Unavailable" : formatMoney({ amount: value, currency: quote.currency })
  const size = (value: string | null) => value === null ? "Unavailable" : groupDecimal(value)
  return <section className="border-t border-border pt-3" aria-label="Quote and last trade">
    <h2 className="sr-only">Quote and last trade</h2>
    {quote === null ? <p className="text-sm text-muted-foreground">Bid, ask and trade information is not available for this investment yet.</p> : <>
      <dl className="grid grid-cols-2 gap-x-4 gap-y-3 sm:grid-cols-4">
        {([
          ["Bid", price(quote.bidPrice), "Bid size", size(quote.bidSize)],
          ["Ask", price(quote.askPrice), "Ask size", size(quote.askSize)],
          ["Midpoint", price(quote.midPrice), null, null],
          ["Last trade", price(quote.lastPrice), "Trade size", size(quote.lastSize)],
        ] as const).map(([label, value, sizeLabel, sizeValue]) => <div key={label} className="min-w-0">
          <dt className="text-xs text-muted-foreground">{label}</dt>
          <dd className="mt-1 break-words font-mono text-sm tabular-nums">{value}</dd>
          {sizeLabel !== null ? <>
            <dt className="mt-1 mr-2 inline-block text-xs text-muted-foreground">{sizeLabel}</dt>
            <dd className="inline break-words font-mono text-xs tabular-nums">{sizeValue}</dd>
          </> : null}
        </div>)}
      </dl>
      {quote.tradeStatus === "ambiguous" ? <p role="status" className="mt-3 text-xs text-muted-foreground">Several trades share the latest timestamp, so a single last trade cannot be established. Bid and ask are shown separately when available.</p> : null}
      <div className="mt-2 grid gap-x-4 gap-y-1 text-xs leading-5 text-muted-foreground sm:grid-cols-2">
        <p>Quote: {unverified ? "freshness not checked" : quote.quoteFresh ? "current at last check" : "not current"}
          {quote.quoteObservedAt ? <> · <time dateTime={quote.quoteObservedAt}>{new Date(quote.quoteObservedAt).toLocaleString()}</time></> : null}</p>
        <p>Last trade: {unverified ? "freshness not checked" : quote.lastFresh ? "current at last check" : "not current"}
          {quote.lastObservedAt ? <> · <time dateTime={quote.lastObservedAt}>{new Date(quote.lastObservedAt).toLocaleString()}</time></> : null}</p>
      </div>
    </>}
  </section>
}

function InvestmentUnavailable({ message }: { message: string }) {
  return <main className="mx-auto w-full max-w-[1180px] p-5 lg:p-7">
    <h1 className="text-3xl font-semibold">Investment</h1>
    <p role="alert" className="mt-3 text-sm text-muted-foreground">{message}</p>
    <Button asChild className="mt-4" variant="outline"><Link to="/markets">Explore Markets</Link></Button>
  </main>
}
