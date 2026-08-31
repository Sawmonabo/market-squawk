import * as React from "react"
import { useQuery } from "@tanstack/react-query"

import { useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { MarketHistoryChart } from "./market-history-chart"
import { parseMarketHistoryResult } from "./market-history"
import { parseMarketInstrumentResult, parseMarketProductResult, type MarketProductRow } from "./market-product"
import { parseInvestmentSearchPage } from "./reference-market"

const queryPolicy = { retry: false, refetchOnWindowFocus: false } as const

export function MarketsPage() {
  const product = useProduct()
  if (product.status !== "ready") return <Page message="Market information is unavailable right now." />
  return <ReadyMarketsPage bootstrap={product.bootstrap} transport={product.transport} />
}

function ReadyMarketsPage({ bootstrap, transport }: { bootstrap: DesktopBootstrap; transport: ProductTransport }) {
  const [search, setSearch] = React.useState("")
  const [submittedSearch, setSubmittedSearch] = React.useState<string | null>(null)
  const [selectionToken, setSelectionToken] = React.useState<string | null>(null)
  const [overviewPageToken, setOverviewPageToken] = React.useState<string | null>(null)
  const [searchPageToken, setSearchPageToken] = React.useState<string | null>(null)
  const overview = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetOverview", { overviewPageToken }),
    queryFn: () => transport.query({ query: "marketOverview", ...(overviewPageToken ? { pageToken: overviewPageToken } : {}) }),
    ...queryPolicy,
  })
  const searchResult = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.SearchUniverse", { query: submittedSearch, searchPageToken }),
    enabled: submittedSearch !== null,
    queryFn: () => transport.query({ query: "marketUniverse", text: submittedSearch!, ...(searchPageToken ? { pageToken: searchPageToken } : {}) }),
    ...queryPolicy,
  })
  const rows = overview.data ? parseMarketProductResult(overview.data).data : []
  const searchPage = searchResult.data ? parseInvestmentSearchPage(searchResult.data) : null
  const matches = searchPage?.data ?? []
  const selected = rows.find((row) => row.selectionToken === selectionToken) ?? null
  const detail = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetInstrument", { selectionToken }),
    enabled: selectionToken !== null,
    queryFn: () => transport.query({ query: "marketInstrument", selectionToken: selectionToken! }),
    ...queryPolicy,
  })
  const detailRow = detail.data && selectionToken ? parseMarketInstrumentResult(detail.data, selectionToken) : selected
  const historyToken = detailRow?.historyToken ?? null
  const history = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetHistory", { historyToken }),
    enabled: historyToken !== null,
    queryFn: () => transport.query({ query: "marketHistory", historyToken: historyToken! }),
    ...queryPolicy,
  })

  return <Page>
    <form className="flex gap-2" onSubmit={(event) => { event.preventDefault(); const value = search.trim(); if (value) { setSearchPageToken(null); setSubmittedSearch(value) } }}>
      <Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Find an investment" maxLength={64} />
      <Button type="submit">Search</Button>
    </form>
    <div className="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
      {rows.map((row) => <MarketCard key={row.selectionToken} row={row} onSelect={() => setSelectionToken(row.selectionToken)} />)}
      {matches.map((row) => <button className="rounded-xl border p-4 text-left" key={row.selectionToken} onClick={() => setSelectionToken(row.selectionToken)}>{row.name ?? row.symbol}</button>)}
    </div>
    <div className="mt-4 flex gap-2">
      {overview.data && parseMarketProductResult(overview.data).page.nextPageToken ? <Button variant="outline" onClick={() => setOverviewPageToken(parseMarketProductResult(overview.data!).page.nextPageToken)}>More markets</Button> : null}
      {searchPage?.page.nextPageToken ? <Button variant="outline" onClick={() => setSearchPageToken(searchPage.page.nextPageToken)}>More results</Button> : null}
    </div>
    {detailRow ? <section className="mt-5 rounded-xl border p-5"><h2 className="text-lg font-semibold">{detailRow.identity.name ?? detailRow.identity.symbol}</h2><p className="mt-2 font-mono">{detailRow.price ? `${detailRow.price.value} ${detailRow.price.currency}` : "Price unavailable"}</p>{detailRow.changePercent ? <p className="text-sm">{detailRow.changePercent}%</p> : null}<p className="mt-2 text-xs text-muted-foreground">{detailRow.asOf ? new Date(detailRow.asOf).toLocaleString() : "Updated time unavailable"}</p></section> : <p className="mt-5 text-sm text-muted-foreground">No investment selected.</p>}
    {historyToken ? <MarketHistoryChart result={history.data ? parseMarketHistoryResult(history.data, historyToken) : null} /> : null}
  </Page>
}

function MarketCard({ row, onSelect }: { row: MarketProductRow; onSelect: () => void }) {
  return <button type="button" onClick={onSelect} className="rounded-xl border p-4 text-left"><h2 className="font-semibold">{row.identity.name ?? row.identity.symbol}</h2><p className="mt-2 font-mono">{row.price ? `${row.price.value} ${row.price.currency}` : "Price unavailable"}</p>{row.changePercent ? <p className="text-sm">{row.changePercent}%</p> : null}</button>
}

function Page({ children, message }: { children?: React.ReactNode; message?: string }) {
  return <main className="mx-auto w-full max-w-[1180px] p-5 lg:p-7"><h1 className="text-3xl font-semibold">Markets</h1><p className="mt-2 text-sm text-muted-foreground">Explore current prices and historical context for investments you choose.</p><div className="mt-6">{message ?? children}</div></main>
}
