import * as React from "react"
import { useMutation, useQuery } from "@tanstack/react-query"

import { useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { AnalysisLaunch } from "@/features/opportunities/analysis-launch"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { formatTimestamp } from "@/lib/time"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { MarketHistoryChart } from "./market-history-chart"
import { parseMarketHistoryResult } from "./market-history"
import {
  marketSessionRequestSchema, parseMarketInstrumentResult, parseMarketProductResult,
  parseMarketSessionContext, type MarketProductRow, type MarketSessionContext,
  type MarketSessionReference, type MarketSessionRequest,
} from "./market-product"
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
    {detailRow ? <div className="mt-4"><AnalysisLaunch transport={transport} scope={bootstrap.productSessionToken} selectionToken={detailRow.selectionToken} /></div> : null}
    {historyToken ? <MarketHistoryChart result={history.data ? parseMarketHistoryResult(history.data, historyToken) : null} /> : null}
    <MarketSessionPanel key={bootstrap.productSessionToken} bootstrap={bootstrap} transport={transport} />
  </Page>
}

const sessionProducts: { value: MarketSessionRequest["product"]; label: string }[] = [
  { value: "equity", label: "Stocks and funds" },
  { value: "option", label: "Options" },
  { value: "bond", label: "Bonds" },
  { value: "future", label: "Futures" },
  { value: "forex", label: "Currencies" },
]
const sessionStatus = {
  scheduled_sessions: "Sessions reported",
  explicitly_open: "Reported open",
  explicitly_closed: "Reported closed",
  unknown: "Status unavailable",
} as const
const sessionRole = {
  core: "Regular session", pre: "Before regular hours", post: "After regular hours",
  intermission: "Intermission", source_defined: "Other reported session",
} as const

function MarketSessionPanel({ bootstrap, transport }: { bootstrap: DesktopBootstrap; transport: ProductTransport }) {
  const [product, setProduct] = React.useState("")
  const [date, setDate] = React.useState("")
  const [confirmation, setConfirmation] = React.useState<MarketSessionRequest | null>(null)
  const [reference, setReference] = React.useState<MarketSessionReference | null>(null)
  const [context, setContext] = React.useState<MarketSessionContext | null>(null)
  const canAcquire = bootstrap.capabilities.includes("market_session_context")
  const canRead = bootstrap.capabilities.includes("market_session_read")
  const selected = marketSessionRequestSchema.safeParse({ product, date })
  const load = useMutation({
    mutationFn: async (action: { request: MarketSessionRequest } | { reference: MarketSessionReference }) => {
      if ("reference" in action) {
        const result = await transport.query({ query: "marketSessionRead", reference: action.reference })
        return parseMarketSessionContext(result, action.reference.request, action.reference)
      }
      const result = await transport.query({ query: "marketSessionContext", ...action.request, confirmed: true })
      return parseMarketSessionContext(result, action.request)
    },
    onMutate: () => setContext(null),
    onSuccess: (value) => { setReference(value.reference); setContext(value); setConfirmation(null) },
    onError: () => setConfirmation(null),
  })

  return <section className="mt-5 rounded-xl border p-5">
    <h2 className="text-lg font-semibold">Trading sessions</h2>
    <p className="mt-2 text-sm text-muted-foreground">Choose a market and date to view reported trading hours. Missing entries or hours do not mean a market is closed.</p>
    <form className="mt-4 flex flex-wrap items-end gap-3" onSubmit={(event) => {
      event.preventDefault()
      if (canAcquire && selected.success && !load.isPending) setConfirmation(selected.data)
    }}>
      <div className="space-y-2">
        <Label htmlFor="session-product">Market</Label>
        <select id="session-product" className="h-10 rounded-md border border-input bg-background px-3 text-sm"
          value={product} disabled={load.isPending} onChange={(event) => setProduct(event.target.value)}>
          <option value="">Select a market</option>
          {sessionProducts.map((choice) => <option key={choice.value} value={choice.value}>{choice.label}</option>)}
        </select>
      </div>
      <div className="space-y-2">
        <Label htmlFor="session-date">Date</Label>
        <Input id="session-date" type="date" value={date} disabled={load.isPending} onChange={(event) => setDate(event.target.value)} />
      </div>
      <Button type="submit" disabled={!canAcquire || !selected.success || load.isPending}>Load trading sessions</Button>
    </form>
    {!canAcquire ? <p className="mt-3 text-sm text-muted-foreground">Trading-session lookup is unavailable.</p> : null}
    {load.isPending ? <p className="mt-3 text-sm" role="status">Loading trading-session information…</p> : null}
    {load.isError ? <p className="mt-3 text-sm" role="alert">Trading-session information is unavailable. Check Connections in Settings or try again.</p> : null}
    {context ? <div className="mt-4">
      <h3 className="font-medium">{sessionProducts.find((choice) => choice.value === context.product)?.label} · {context.date}</h3>
      <p className="mt-1 text-xs text-muted-foreground">Only returned entries are shown. Times use your local time zone.</p>
      <ol className="mt-3 space-y-3">
        {context.entries.map((entry) => <li key={entry.entry} className="rounded-lg border p-3">
          <p className="text-sm">Entry {entry.entry} · {sessionStatus[entry.status]}</p>
          {entry.sessionPresence !== "reported" ? <p className="mt-1 text-xs text-muted-foreground">Session details were not reported.</p> : null}
          {entry.windows.length === 0 ? <p className="mt-1 text-xs text-muted-foreground">No session times were returned.</p> : <ul className="mt-2 space-y-1 text-xs">
            {entry.windows.map((window) => <li key={`${window.role}:${window.ordinal}`}>
              {sessionRole[window.role]}: {formatTimestamp(window.startUnixNanos)} – {formatTimestamp(window.endUnixNanos)}
            </li>)}
          </ul>}
        </li>)}
      </ol>
    </div> : null}
    {reference && canRead ? <Button className="mt-4" variant="outline" disabled={load.isPending}
      onClick={() => load.mutate({ reference })}>Reopen saved sessions for {reference.request.date}</Button> : null}
    <Dialog open={confirmation !== null} onOpenChange={(open) => { if (!open && !load.isPending) setConfirmation(null) }}>
      <DialogContent showCloseButton={!load.isPending}>
        <DialogHeader>
          <DialogTitle>Load trading-session information?</DialogTitle>
          <DialogDescription>Request and save the reported sessions for {sessionProducts.find((choice) => choice.value === confirmation?.product)?.label} on {confirmation?.date}.</DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="ghost" disabled={load.isPending} onClick={() => setConfirmation(null)}>Cancel</Button>
          <Button disabled={load.isPending || !confirmation} onClick={() => { if (confirmation) load.mutate({ request: confirmation }) }}>
            {load.isPending ? "Loading…" : "Load and save sessions"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  </section>
}

function MarketCard({ row, onSelect }: { row: MarketProductRow; onSelect: () => void }) {
  return <button type="button" onClick={onSelect} className="rounded-xl border p-4 text-left"><h2 className="font-semibold">{row.identity.name ?? row.identity.symbol}</h2><p className="mt-2 font-mono">{row.price ? `${row.price.value} ${row.price.currency}` : "Price unavailable"}</p>{row.changePercent ? <p className="text-sm">{row.changePercent}%</p> : null}</button>
}

function Page({ children, message }: { children?: React.ReactNode; message?: string }) {
  return <main className="mx-auto w-full max-w-[1180px] p-5 lg:p-7"><h1 className="text-3xl font-semibold">Markets</h1><p className="mt-2 text-sm text-muted-foreground">Explore current prices and historical context for investments you choose.</p><div className="mt-6">{message ?? children}</div></main>
}
