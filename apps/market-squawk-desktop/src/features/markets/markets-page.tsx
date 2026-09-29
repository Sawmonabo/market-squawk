import * as React from "react"
import { keepPreviousData, useMutation, useQuery } from "@tanstack/react-query"
import { useSearchParams } from "react-router-dom"

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
import { parseMarketHistoryResult, sourceInstantUnixNanos, type MarketHistoryBar, type MarketHistoryViewportInput } from "./market-history"
import {
  marketSelectionTokenSchema, marketSessionRequestSchema, parseMarketInstrumentResult, parseMarketProductResult,
  parseMarketSessionContext, type MarketProductRow, type MarketSessionContext,
  type MarketSessionReference, type MarketSessionRequest,
} from "./market-product"
import { parseInvestmentSearchPage } from "./reference-market"

import { CursorNavigation, useCursorNavigation } from "../shared/cursor-navigation"
import { DemandPanel } from "../shared/demand-panel"

const queryPolicy = { retry: false, refetchOnWindowFocus: false } as const

export function MarketsPage() {
  const product = useProduct()
  if (product.status !== "ready") return <Page message="Market information is unavailable right now." />
  return <ReadyMarketsPage key={product.bootstrap.productSessionToken} bootstrap={product.bootstrap} transport={product.transport} />
}

function ReadyMarketsPage({ bootstrap, transport }: { bootstrap: DesktopBootstrap; transport: ProductTransport }) {
  const [search, setSearch] = React.useState("")
  const [submittedSearch, setSubmittedSearch] = React.useState<string | null>(null)
  const [searchParams, setSearchParams] = useSearchParams()
  const requestedSelection = searchParams.get("selectionToken")
  const admittedSelection = marketSelectionTokenSchema.safeParse(requestedSelection)
  const selectionToken = admittedSelection.success ? admittedSelection.data : null
  const selectInvestment = (token: string) => setSearchParams((current) => {
    const next = new URLSearchParams(current)
    next.set("selectionToken", token)
    return next
  })
  const overviewNavigation = useCursorNavigation()
  const searchNavigation = useCursorNavigation()
  const overviewPageToken = overviewNavigation.after
  const searchPageToken = searchNavigation.after
  const overview = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetOverview", { query: "marketOverview", ...(overviewPageToken ? { pageToken: overviewPageToken } : {}) }),
    gcTime: 0,
    queryFn: ({ signal }) => transport.query({ query: "marketOverview", ...(overviewPageToken ? { pageToken: overviewPageToken } : {}) }, { signal }),
    ...queryPolicy,
  })
  const searchResult = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.SearchUniverse", { query: submittedSearch, searchPageToken }),
    enabled: submittedSearch !== null,
    gcTime: 0,
    queryFn: ({ signal }) => transport.query({ query: "marketUniverse", text: submittedSearch!, ...(searchPageToken ? { pageToken: searchPageToken } : {}) }, { signal }),
    ...queryPolicy,
  })
  const rows = overview.data ? parseMarketProductResult(overview.data).data : []
  const searchPage = searchResult.data ? parseInvestmentSearchPage(searchResult.data) : null
  const matches = searchPage?.data ?? []
  const detail = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetInstrument", { selectionToken }),
    enabled: selectionToken !== null,
    gcTime: 0,
    queryFn: async ({ signal }) => {
      if (selectionToken === null) throw new Error("Select an investment before opening it.")
      const result = await transport.query({ query: "marketInstrument", selectionToken }, { signal })
      if (signal.aborted) throw new DOMException("The view was closed.", "AbortError")
      return parseMarketInstrumentResult(result, selectionToken)
    },
    ...queryPolicy,
  })
  const detailRow = selectionToken !== null && detail.isSuccess ? detail.data : null
  const historyToken = detailRow?.historyToken ?? null

  return <Page>
    <form className="flex gap-2" onSubmit={(event) => {
      event.preventDefault()
      const value = search.trim()
      if (value) {
        if (value === submittedSearch && searchPageToken === undefined) void searchResult.refetch()
        searchNavigation.restart()
        setSubmittedSearch(value)
      }
    }}>
      <Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Find an investment" maxLength={64} />
      <Button type="submit">Search</Button>
    </form>
    <div className="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
      {rows.map((row) => <MarketCard key={row.selectionToken} row={row} onSelect={() => selectInvestment(row.selectionToken)} />)}
      {matches.map((row) => <button className="rounded-xl border p-4 text-left" key={row.selectionToken} onClick={() => selectInvestment(row.selectionToken)}>{row.name ?? row.symbol}</button>)}
    </div>
    <CursorNavigation navigation={overviewNavigation} next={overview.data ? parseMarketProductResult(overview.data).page.nextPageToken : null} busy={overview.isFetching}
      onRestart={() => { if (overviewPageToken === undefined) void overview.refetch() }} />
    {submittedSearch !== null ? <CursorNavigation navigation={searchNavigation} next={searchPage?.page.nextPageToken} busy={searchResult.isFetching}
      onRestart={() => { if (searchPageToken === undefined) void searchResult.refetch() }} /> : null}
    {requestedSelection !== null && (selectionToken === null || detail.isError) ? <p role="alert" className="mt-5 text-sm text-destructive">This investment could not be opened. Search again to choose a fresh investment selection.</p>
      : selectionToken !== null && detail.isPending ? <p role="status" className="mt-5 text-sm text-muted-foreground">Opening the selected investment…</p>
        : detailRow ? <section className="mt-5 rounded-xl border p-5"><h2 className="text-lg font-semibold">{detailRow.identity.name ?? detailRow.identity.symbol}</h2><p className="mt-2 font-mono">{detailRow.price ? `${detailRow.price.value} ${detailRow.price.currency}` : "Price unavailable"}</p>{detailRow.changePercent ? <p className="text-sm">{detailRow.changePercent}%</p> : null}<p className="mt-2 text-xs text-muted-foreground">{detailRow.asOf ? new Date(detailRow.asOf).toLocaleString() : "Updated time unavailable"}</p></section> : <p className="mt-5 text-sm text-muted-foreground">No investment selected.</p>}
    {detailRow ? <div className="mt-4"><AnalysisLaunch key={detailRow.selectionToken} transport={transport} scope={bootstrap.productSessionToken} selectionToken={detailRow.selectionToken} /></div> : null}
    {historyToken ? <DemandPanel key={historyToken} title="Open price history" className="mt-5 rounded-xl border p-5">
      <MarketHistoryRead historyToken={historyToken} bootstrap={bootstrap} transport={transport} />
    </DemandPanel> : null}
    <MarketSessionPanel key={bootstrap.productSessionToken} bootstrap={bootstrap} transport={transport} />
  </Page>
}

function MarketHistoryRead({ historyToken, bootstrap, transport }: {
  historyToken: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const generation = React.useRef<string | undefined>(undefined)
  const [revision, setRevision] = React.useState(0)
  const [viewport, setViewport] = React.useState<MarketHistoryViewportInput>({ pointLimit: 512 })
  const [selectedBar, setSelectedBar] = React.useState<MarketHistoryBar | null>(null)
  const history = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetHistory", { historyToken, ...viewport, revision }),
    gcTime: 0,
    placeholderData: keepPreviousData,
    queryFn: async ({ signal }) => {
      const pinnedGeneration = generation.current
      const result = parseMarketHistoryResult(await transport.query({ query: "marketHistory", historyToken, ...viewport,
        ...(pinnedGeneration === undefined ? {} : { generationToken: pinnedGeneration }) }, { signal }), historyToken, viewport, pinnedGeneration)
      if (signal.aborted) throw new DOMException("The view was closed.", "AbortError")
      if (result.data !== null && generation.current === undefined) generation.current = result.data.generationToken
      return result
    },
    ...queryPolicy,
  })
  const reset = () => { generation.current = undefined; setViewport({ pointLimit: 512 }); setSelectedBar(null); setRevision((current) => current + 1) }
  return <>
    <Button variant="outline" size="sm" onClick={reset}>Refresh saved history</Button>
    {history.isError ? <p role="alert" className="mt-3 text-sm text-destructive">Price history could not be loaded. Refresh saved history to open a new generation. <Button variant="outline" size="sm" onClick={() => void history.refetch()}>Retry</Button></p> : null}
    {history.isFetching ? <p role="status" className="mt-3 text-sm text-muted-foreground">Loading the requested price window…</p> : null}
    {history.data ? <MarketHistoryChart key={history.data.data?.generationToken ?? revision} result={history.data}
      onViewportChange={(next) => { setViewport(next); setSelectedBar(null) }} onObservationSelect={setSelectedBar} /> : null}
    {selectedBar !== null && history.data?.data ? <OriginalMarketBarRead key={`${selectedBar.originalOrdinal}:${history.data.data.generationToken}`}
      bar={selectedBar} historyToken={historyToken} generationToken={history.data.data.generationToken} bootstrap={bootstrap} transport={transport} /> : null}
  </>
}

function OriginalMarketBarRead({ bar, historyToken, generationToken, bootstrap, transport }: {
  bar: MarketHistoryBar; historyToken: string; generationToken: string; bootstrap: DesktopBootstrap; transport: ProductTransport
}) {
  const viewport: MarketHistoryViewportInput = bar.time.precision === "nominal_date"
    ? { startDate: bar.time.date, endDate: bar.time.date, pointLimit: 8 }
    : { startUnixNanos: sourceInstantUnixNanos(bar.time.startsAt), endUnixNanos: sourceInstantUnixNanos(bar.time.startsAt), pointLimit: 8 }
  const original = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetHistory", { historyToken, generationToken, ...viewport }),
    gcTime: 0,
    queryFn: async ({ signal }) => {
      const result = parseMarketHistoryResult(await transport.query({ query: "marketHistory", historyToken, generationToken, ...viewport }, { signal }), historyToken, viewport, generationToken)
      const point = result.data?.bars.find((candidate) => candidate.originalOrdinal === bar.originalOrdinal)
      if (!point) throw new Error("The original price observation could not be verified.")
      return point
    },
  })
  return <div className="mt-4 rounded-lg border border-border p-3 text-xs">
    <p className="font-medium">Exact original price observation</p>
    {original.isPending ? <p role="status" className="mt-2 text-muted-foreground">Checking saved evidence…</p>
      : original.isError ? <p role="alert" className="mt-2 text-destructive">The saved observation could not be verified. <Button size="xs" variant="outline" onClick={() => void original.refetch()}>Retry</Button></p>
        : <dl className="mt-2 grid grid-cols-2 gap-2 sm:grid-cols-5">{([['Open', original.data.open], ['High', original.data.high], ['Low', original.data.low], ['Close', original.data.close], ['Volume', original.data.volume]] as const).map(([label, value]) => <div key={label}><dt className="text-muted-foreground">{label}</dt><dd className="mt-1 font-mono">{value}</dd></div>)}</dl>}
  </div>
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
