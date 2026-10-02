import { useQuery } from "@tanstack/react-query"
import { Link, useParams } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { AnalysisLaunch } from "@/features/opportunities/analysis-launch"
import { DemandPanel } from "@/features/shared/demand-panel"
import { formatMoney } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { MarketHistoryRead } from "./market-history-read"
import { marketAvailabilityLabel, marketSelectionTokenSchema, parseMarketInstrumentResult } from "./market-product"

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
  const detail = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetInstrument", { selectionToken }),
    gcTime: 0,
    retry: false,
    refetchOnWindowFocus: false,
    queryFn: async ({ signal }) => {
      const result = await transport.query({ query: "marketInstrument", selectionToken }, { signal })
      if (signal.aborted) throw new DOMException("The view was closed.", "AbortError")
      return parseMarketInstrumentResult(result, selectionToken)
    },
  })
  const row = detail.data ?? null
  const title = row === null ? "Investment" : [row.identity.symbol, row.identity.name]
    .filter((value, index, values) => value !== null && values.indexOf(value) === index).join(" · ")

  return <main className="mx-auto w-full max-w-[1180px] space-y-5 p-5 lg:p-7">
    <header className="border-b border-border pb-5">
      <Link className="text-xs text-primary underline-offset-4 hover:underline" to="/markets">Back to Markets</Link>
      <h1 className="mt-3 text-3xl font-semibold">{title}</h1>
      <p className="mt-2 text-sm text-muted-foreground">Review available prices, history and investment analysis.</p>
    </header>
    <section className="rounded-xl border border-border p-5" aria-label="Investment price">
      <div className="flex items-start justify-between gap-4">
        <h2 className="text-lg font-semibold">Price</h2>
        <Button variant="outline" size="sm" disabled={detail.isFetching} onClick={() => void detail.refetch()}>Refresh price</Button>
      </div>
      {detail.isPending ? <p role="status" className="mt-3 text-sm text-muted-foreground">Opening the selected investment…</p> : null}
      {detail.isError ? <p role="alert" className="mt-3 text-sm text-destructive">
        {row === null ? "This investment could not be opened. Try again, or search Markets to choose a fresh selection."
          : "The price could not be refreshed. Showing the last checked information; its freshness is unverified."}
      </p> : null}
      {row !== null ? <>
        <p className="mt-3 font-mono text-2xl">{row.price ? formatMoney({ amount: row.price.value, currency: row.price.currency }) : "Price unavailable"}</p>
        <p className="mt-2 text-sm text-muted-foreground">{detail.isError && row.price !== null
          ? "Saved price · Freshness not checked"
          : detail.isFetching ? `${marketAvailabilityLabel(row)} at last check` : marketAvailabilityLabel(row)}{row.changePercent !== null ? ` · ${row.changePercent}%` : ""}</p>
        {row.asOf ? <time className="mt-2 block text-xs text-muted-foreground" dateTime={row.asOf}>{new Date(row.asOf).toLocaleString()}</time> : null}
        {detail.isFetching ? <p role="status" className="mt-2 text-xs text-muted-foreground">Updating price information…</p> : null}
      </> : null}
    </section>
    {row !== null ? <section className="rounded-xl border border-border p-5" aria-label="Investment analysis">
      <h2 className="mb-4 text-lg font-semibold">Investment analysis</h2>
      <AnalysisLaunch transport={transport} scope={bootstrap.productSessionToken} selectionToken={row.selectionToken} />
    </section> : null}
    {row?.historyToken ? <DemandPanel key={row.historyToken} title="Open price history" className="rounded-xl border border-border p-5">
      <MarketHistoryRead historyToken={row.historyToken} bootstrap={bootstrap} transport={transport} />
    </DemandPanel> : row !== null ? <p className="text-sm text-muted-foreground">Price history is unavailable for this investment.</p> : null}
  </main>
}

function InvestmentUnavailable({ message }: { message: string }) {
  return <main className="mx-auto w-full max-w-[1180px] p-5 lg:p-7">
    <h1 className="text-3xl font-semibold">Investment</h1>
    <p role="alert" className="mt-3 text-sm text-muted-foreground">{message}</p>
    <Button asChild className="mt-4" variant="outline"><Link to="/markets">Explore Markets</Link></Button>
  </main>
}
