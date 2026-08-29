import * as React from "react"
import { useQuery, type UseQueryResult } from "@tanstack/react-query"
import {
  Activity,
  CircleAlert,
  Clock3,
  DatabaseZap,
  RefreshCw,
  Search,
  ShieldCheck,
} from "lucide-react"
import { useSearchParams } from "react-router-dom"

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import { humanize } from "@/lib/formatters"
import type { ApplicationResult, DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  type BookLevel,
  instrumentBooks,
  instrumentComparison,
  instrumentQuotes,
  instrumentTrades,
  resultState,
} from "./market-evidence"
import {
  type ReferenceMarketRow,
  referenceMarketRows,
} from "./reference-market"
import { type UnifiedMarketRow, unifiedMarketRows } from "./unified-market"

const UNIFIED_FEED_POLL_INTERVAL_MS = 5_000

export function MarketsPage() {
  const product = useProduct()

  if (product.status === "loading") return <MarketsLoading />
  if (product.status === "error") {
    return (
      <PageFrame>
        <EmptyState
          title="Markets are unavailable"
          detail="Try again or review Logs & Diagnostics for details."
        />
      </PageFrame>
    )
  }

  return (
    <ReadyMarketsPage
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function ReadyMarketsPage({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const [searchParams, setSearchParams] = useSearchParams()
  const requestedInstrumentId = validInstrumentId(searchParams.get("instrumentId"))
  const requestedReferenceId = validReferenceId(searchParams.get("referenceId"))
  const operationNames = new Set(
    bootstrap.operations.map((operation) => operation.name),
  )
  const feedAvailable = operationNames.has("Market.GetUnifiedFeed")
  const pageVisible = usePageVisibility()
  const feed = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "market",
      "Market.GetUnifiedFeed",
      {},
    ),
    enabled: feedAvailable,
    queryFn: () => transport.query({ query: "marketUnifiedFeed" }),
    refetchInterval:
      feedAvailable && pageVisible ? UNIFIED_FEED_POLL_INTERVAL_MS : false,
    refetchIntervalInBackground: false,
  })
  const feedRead = parseRead(() => unifiedMarketRows(feed.data))
  const rows = feedRead.value ?? []
  const [searchText, setSearchText] = React.useState("")
  const [referenceQuery, setReferenceQuery] = React.useState("")
  React.useEffect(() => {
    const timer = window.setTimeout(() => setReferenceQuery(searchText.trim()), 250)
    return () => window.clearTimeout(timer)
  }, [searchText])
  const universeAvailable = operationNames.has("Market.SearchUniverse")
  const universe = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "market",
      "Market.SearchUniverse",
      { query: referenceQuery },
    ),
    enabled: universeAvailable,
    queryFn: () =>
      transport.query({
        query: "marketUniverse",
        ...(referenceQuery ? { text: referenceQuery } : {}),
      }),
  })
  const universeRead = parseRead(() => referenceMarketRows(universe.data))
  const liveReferenceKeys = new Set(
    rows.map((row) => `${row.symbol.toLocaleUpperCase()}|${row.symbolVenueId}`),
  )
  const referenceRows = (universeRead.value ?? []).filter(
    (row) =>
      !liveReferenceKeys.has(`${row.symbol.toLocaleUpperCase()}|${row.venueId}`),
  )
  const normalizedSearch = searchText.trim().toLocaleLowerCase()
  const visibleRows = normalizedSearch
    ? rows.filter((row) =>
        [
          row.symbol,
          row.instrumentId,
          row.displayName,
          row.assetClass,
          row.quoteCurrency,
        ]
          .filter((value): value is string => Boolean(value))
          .some((value) => value.toLocaleLowerCase().includes(normalizedSearch)),
      )
    : rows
  const instrumentIds = [
    ...new Set([
      ...(requestedInstrumentId ? [requestedInstrumentId] : []),
      ...rows.map((row) => row.instrumentId),
    ]),
  ]
  const [selectedId, setSelectedId] =
    React.useState<string | null>(requestedInstrumentId)
  const [selectedReferenceId, setSelectedReferenceId] =
    React.useState<string | null>(requestedReferenceId)
  React.useEffect(() => {
    if (requestedInstrumentId) {
      setSelectedId(requestedInstrumentId)
      setSelectedReferenceId(null)
    } else if (requestedReferenceId) {
      setSelectedReferenceId(requestedReferenceId)
      setSelectedId(null)
    }
  }, [requestedInstrumentId, requestedReferenceId])
  const selectedInstrument =
    selectedReferenceId === null
      ? instrumentIds.find((instrumentId) => instrumentId === selectedId) ??
        instrumentIds[0] ??
        null
      : null
  const selectedRow = rows.find((row) => row.instrumentId === selectedInstrument) ?? null
  const selectedReference =
    referenceRows.find((row) => row.referenceId === selectedReferenceId) ?? null
  const tradesAvailable = operationNames.has("Market.GetTrades")
  const quotesAvailable = operationNames.has("Market.GetQuotes")
  const booksAvailable = operationNames.has("Market.GetBooks")
  const comparisonsAvailable = operationNames.has("Market.GetComparisons")
  const trades = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "market",
      "Market.GetTrades",
      { instrumentId: selectedInstrument },
    ),
    enabled: selectedInstrument !== null && tradesAvailable,
    queryFn: () =>
      transport.query({
        query: "marketTrades",
        instrumentId: requiredInstrument(selectedInstrument),
      }),
  })
  const quotes = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "market",
      "Market.GetQuotes",
      { instrumentId: selectedInstrument },
    ),
    enabled: selectedInstrument !== null && quotesAvailable,
    queryFn: () =>
      transport.query({
        query: "marketQuotes",
        instrumentId: requiredInstrument(selectedInstrument),
      }),
  })
  const books = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "market",
      "Market.GetBooks",
      { instrumentId: selectedInstrument },
    ),
    enabled: selectedInstrument !== null && booksAvailable,
    queryFn: () =>
      transport.query({
        query: "marketBooks",
        instrumentId: requiredInstrument(selectedInstrument),
      }),
  })
  const comparisons = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "market",
      "Market.GetComparisons",
      { instrumentId: selectedInstrument },
    ),
    enabled: selectedInstrument !== null && comparisonsAvailable,
    queryFn: () =>
      transport.query({
        query: "marketComparisons",
        instrumentId: requiredInstrument(selectedInstrument),
      }),
  })
  const refreshing =
    feed.isFetching ||
    universe.isFetching ||
    trades.isFetching ||
    quotes.isFetching ||
    books.isFetching ||
    comparisons.isFetching
  const feedFailed = feedRead.error !== null || feed.isError
  const universeFailed = universeRead.error !== null || universe.isError
  const live = rows.filter((row) => row.availability === "Live").length
  const verified = rows.filter((row) => row.confidence === "Verified").length

  const refresh = () => {
    void Promise.all([
      feed.refetch(),
      ...(universeAvailable ? [universe.refetch()] : []),
      ...(selectedInstrument && tradesAvailable ? [trades.refetch()] : []),
      ...(selectedInstrument && quotesAvailable ? [quotes.refetch()] : []),
      ...(selectedInstrument && booksAvailable ? [books.refetch()] : []),
      ...(selectedInstrument && comparisonsAvailable
        ? [comparisons.refetch()]
        : []),
    ])
  }

  const selectInstrument = (instrumentId: string) => {
    setSelectedId(instrumentId)
    setSelectedReferenceId(null)
    setSearchParams({ instrumentId }, { replace: true })
  }

  const selectReference = (referenceId: string) => {
    setSelectedReferenceId(referenceId)
    setSelectedId(null)
    setSearchParams({ referenceId }, { replace: true })
  }

  return (
    <PageFrame
      action={
        <Button variant="outline" size="sm" onClick={refresh} disabled={refreshing}>
          <RefreshCw className={refreshing ? "animate-spin" : ""} aria-hidden="true" />
          Refresh evidence
        </Button>
      }
    >
      {!feedAvailable && !universeAvailable ? (
        <EmptyState
          title="Markets are not available in this build"
          detail="Update Market Squawk, then reopen the app."
        />
      ) : feedFailed && universeFailed ? (
        <EmptyState
          title="Market data is temporarily unavailable"
          detail="Try again or review Logs & Diagnostics for details."
        />
      ) : rows.length === 0 &&
        referenceRows.length === 0 &&
        (feed.isLoading || universe.isLoading) ? (
        <MarketGridLoading />
      ) : rows.length === 0 && referenceRows.length === 0 && feedFailed ? (
        <EmptyState
          title="Live market observations are unavailable"
          detail="Check your connections, then try again."
        />
      ) : rows.length === 0 && referenceRows.length === 0 && universeFailed ? (
        <EmptyState
          title="U.S. listing search is unavailable"
          detail="Try again or review Logs & Diagnostics for details."
        />
      ) : rows.length === 0 && referenceRows.length === 0 ? (
        <EmptyState
          title="No markets are available yet"
          detail="Market Squawk could not load either an active public market or the official U.S. listing reference. Check Sources, then retry."
        />
      ) : (
        <>
          {feedFailed ? (
            <Notice
              text="Live prices are unavailable. Official listing search remains usable."
            />
          ) : null}
          {universeFailed ? (
            <Notice
              text="U.S. listing search is unavailable. Current markets remain usable."
            />
          ) : null}
          <div className="grid gap-3 sm:grid-cols-3">
            <Summary
              label="Markets in view"
              value={rows.length + referenceRows.length}
              icon={Activity}
            />
            <Summary label="Live now" value={live} icon={Clock3} />
            <Summary label="High confidence" value={verified} icon={ShieldCheck} />
          </div>
          <section className="mt-4 rounded-xl border border-border bg-card/35 p-4">
            <label htmlFor="market-search" className="text-xs font-semibold">
              Find a stock, ETF, or crypto market
            </label>
            <div className="relative mt-2 max-w-xl">
              <Search className="pointer-events-none absolute left-3 top-2.5 size-4 text-muted-foreground" aria-hidden="true" />
              <Input
                id="market-search"
                className="pl-9"
                value={searchText}
                onChange={(event) => setSearchText(event.target.value)}
                placeholder="Try AAPL, Microsoft, SPY, QQQ, or BTC"
              />
            </div>
            <p className="mt-2 text-[11px] leading-5 text-muted-foreground">
              Market Squawk shows the best available current information and clearly labels delays,
              coverage limits, and confidence.
            </p>
          </section>
          {visibleRows.length === 0 && referenceRows.length === 0 ? (
            <Notice text="No admitted market or official listing matches that search." />
          ) : (
            <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {visibleRows.map((row) => (
                <MarketCard
                  key={row.instrumentId}
                  row={row}
                  selected={row.instrumentId === selectedInstrument}
                  onSelect={() => selectInstrument(row.instrumentId)}
                />
              ))}
              {referenceRows.map((row) => (
                <ReferenceMarketCard
                  key={row.referenceId}
                  row={row}
                  selected={row.referenceId === selectedReferenceId}
                  onSelect={() => selectReference(row.referenceId)}
                />
              ))}
            </div>
          )}
          {selectedInstrument && !selectedRow ? (
            <Notice text="The selected instrument has no active unified market observation." />
          ) : null}
          {selectedInstrument ? (
            <details className="mt-5 rounded-xl border border-border bg-card/30 p-4">
              <summary className="cursor-pointer text-sm font-semibold">
                Show detailed trades, quotes, order book, and data agreement
              </summary>
              <InstrumentWorkspace
                instrumentId={selectedInstrument}
                market={selectedRow}
                trades={{ available: tradesAvailable, query: trades }}
                quotes={{ available: quotesAvailable, query: quotes }}
                books={{ available: booksAvailable, query: books }}
                comparisons={{
                  available: comparisonsAvailable,
                  query: comparisons,
                }}
              />
            </details>
          ) : null}
          {selectedReference ? (
            <ReferenceWorkspace row={selectedReference} />
          ) : selectedReferenceId ? (
            <Notice
              text="That reference listing is no longer present in the current search result."
            />
          ) : null}
        </>
      )}
    </PageFrame>
  )
}

function validInstrumentId(value: string | null): string | null {
  return value &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      value,
    )
    ? value
    : null
}

function validReferenceId(value: string | null): string | null {
  return value && value.length <= 256 && /^[a-z0-9._:-]+$/i.test(value)
    ? value
    : null
}

function ReferenceMarketCard({ row, selected, onSelect }: {
  row: ReferenceMarketRow
  selected: boolean
  onSelect: () => void
}) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`rounded-xl border p-5 text-left transition-colors ${
        selected
          ? "border-primary/50 bg-primary/5"
          : "border-border bg-card/45 hover:border-primary/25 hover:bg-card/70"
      }`}
      aria-pressed={selected}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
            {row.isEtf ? "ETF" : "Stock"} · {row.venueId}
          </p>
          <h2 className="mt-2 truncate text-xl font-semibold">{row.symbol}</h2>
          <p className="mt-1 line-clamp-2 text-[11px] leading-5 text-muted-foreground">
            {row.name}
          </p>
        </div>
        <EvidenceBadge label="Reference" tone="neutral" />
      </div>
      <div className="mt-5 rounded-lg border border-border/70 bg-background/35 p-3">
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Current price
        </p>
        <p className="mt-1 text-sm font-medium">Connect an account-backed market feed</p>
      </div>
      <dl className="mt-5 grid grid-cols-2 gap-x-4 gap-y-3 border-t border-border/70 pt-4">
        <Fact label="Identity" value="Official listing" />
        <Fact label="Price coverage" value="Live data connection required" />
        <Fact label="Round lot" value={`${row.roundLotSize.toLocaleString()} shares`} />
        <Fact label="Updated" value={dateTime(row.availableAt)} />
      </dl>
      <p className="mt-4 text-[10px] leading-5 text-muted-foreground">
        This proves the listing identity only. It is not a quote, order book, or trading-status
        claim.
      </p>
    </button>
  )
}

function ReferenceWorkspace({ row }: { row: ReferenceMarketRow }) {
  return (
    <section className="mt-5 rounded-xl border border-border bg-card/30 p-5">
      <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
        Official listing reference
      </p>
      <div className="mt-2 flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold">
            {row.symbol} · {row.name}
          </h2>
          <p className="mt-2 max-w-2xl text-xs leading-5 text-muted-foreground">
            Market Squawk found this U.S. listing without inventing a price. Connect an eligible
            live market-data account in Connections to add a current observation.
          </p>
        </div>
        <EvidenceBadge label="Live data connection required" tone="neutral" />
      </div>
      <dl className="mt-5 grid gap-4 border-t border-border/70 pt-4 sm:grid-cols-2 lg:grid-cols-4">
        <Fact label="Listing venue" value={row.venueId} />
        <Fact label="Asset type" value={row.isEtf ? "ETF" : "Stock"} />
        <Fact label="Reference quality" value="Official delayed" />
        <Fact label="Observed" value={dateTime(row.availableAt)} />
      </dl>
      <p className="mt-5 text-xs leading-5 text-muted-foreground">
        Listing details were effective {dateTime(row.effectiveAt)}. Technical provenance is
        available in Logs &amp; Diagnostics.
      </p>
    </section>
  )
}

type RuntimeDisplayMark = {
  value: string
  currency: string
  basis: "fresh_last_trade" | "fresh_bid_ask_midpoint"
  sourceValidUntil: string | null
}

function runtimeDisplayMark(row: UnifiedMarketRow): RuntimeDisplayMark | null {
  const source = row.selectedSource
  if (!source) return null
  if (
    row.quote.lastPrice !== null &&
    row.quote.lastFreshAtSelection === true &&
    source.freshness.freshAtSelection
  ) {
    return {
      value: row.quote.lastPrice,
      currency: row.quoteCurrency,
      basis: "fresh_last_trade",
      sourceValidUntil: source.freshness.sourceValidUntil,
    }
  }
  if (row.quote.midPrice !== null && source.freshness.freshAtSelection) {
    return {
      value: row.quote.midPrice,
      currency: row.quoteCurrency,
      basis: "fresh_bid_ask_midpoint",
      sourceValidUntil: source.freshness.sourceValidUntil,
    }
  }
  return null
}

function runtimeDisplayMarkSummary(
  row: UnifiedMarketRow,
  mark: RuntimeDisplayMark | null,
): string {
  if (!mark) {
    return row.selectedSource
      ? "No fresh completed trade or bid-and-ask midpoint is available."
      : marketObservationUnavailableName(row.marketObservation.reason)
  }
  const validThrough = mark.sourceValidUntil
    ? ` · current through ${dateTime(mark.sourceValidUntil)}`
    : " · no precise freshness deadline reported"
  return `${markBasisName(mark.basis)}${validThrough}`
}

function MarketCard({ row, selected, onSelect }: {
  row: UnifiedMarketRow
  selected: boolean
  onSelect: () => void
}) {
  const source = row.selectedSource
  const mark = runtimeDisplayMark(row)
  const markSummary = runtimeDisplayMarkSummary(row, mark)
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`rounded-xl border p-5 text-left transition-colors ${
        selected
          ? "border-primary/50 bg-primary/5"
          : "border-border bg-card/45 hover:border-primary/25 hover:bg-card/70"
      }`}
      aria-pressed={selected}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
            {humanize(row.assetClass)} · {row.quoteCurrency}
          </p>
          <h2 className="mt-2 truncate text-xl font-semibold">{row.symbol}</h2>
          <p className="mt-1 truncate text-[11px] text-muted-foreground">
            {row.displayName ?? "Current market"}
          </p>
        </div>
        <EvidenceBadge
          label={row.availability}
          tone={
            row.availability === "Live" && source?.health === "healthy"
              ? "good"
              : row.availability === "Unavailable" || row.availability === "Stale"
                ? "bad"
                : "neutral"
          }
        />
      </div>
      <div className="mt-5">
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Current display price
        </p>
        <p className="mt-1 font-mono text-2xl font-semibold">
          {mark ? `${mark.value} ${mark.currency}` : "Not available"}
        </p>
        <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
          {markSummary}
        </p>
        <p className="mt-2 text-[10px] font-medium text-amber-200">
          {analyticalReadinessName(row.analyticalReadiness)}
        </p>
      </div>
      <dl className="mt-5 grid grid-cols-2 gap-x-4 gap-y-3 border-t border-border/70 pt-4">
        <Fact label="Bid" value={row.quote.bidPrice ?? "Not available"} />
        <Fact label="Ask" value={row.quote.askPrice ?? "Not available"} />
        <Fact label="Confidence" value={row.confidence} />
        <Fact label="Coverage" value={source ? humanize(source.coverage) : "Not available"} />
        <Fact label="Market depth" value={source?.depthLabel ?? "Not available"} />
        <Fact
          label="Individual orders"
          value={
            row.orderBook
              ? `${row.orderBook.returnedOrderCount.toLocaleString()} of ${row.orderBook.totalOrderCount.toLocaleString()}`
              : "Not available"
          }
        />
        <Fact label="Updated" value={dateTime(source?.freshness.availableAt ?? null)} />
      </dl>
      {source &&
      (source.integrity.generationCurrent === false || !source.integrity.snapshotInitialized) ? (
        <div className="mt-4 flex gap-2 rounded-lg border border-amber-400/20 bg-amber-400/5 p-3 text-xs text-amber-200">
          <CircleAlert className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
          <span>Market data is still updating and is not treated as current yet.</span>
        </div>
      ) : null}
    </button>
  )
}

type InstrumentQuery = {
  available: boolean
  query: UseQueryResult<ApplicationResult, Error>
}

function InstrumentWorkspace({
  instrumentId,
  market,
  trades,
  quotes,
  books,
  comparisons,
}: {
  instrumentId: string
  market: UnifiedMarketRow | null
  trades: InstrumentQuery
  quotes: InstrumentQuery
  books: InstrumentQuery
  comparisons: InstrumentQuery
}) {
  const tradeRead = parseRead(() => instrumentTrades(trades.query.data, instrumentId))
  const quoteRead = parseRead(() => instrumentQuotes(quotes.query.data, instrumentId))
  const bookRead = parseRead(() => instrumentBooks(books.query.data, instrumentId))
  const comparisonRead = parseRead(() =>
    instrumentComparison(comparisons.query.data, instrumentId),
  )
  const tradeRows = tradeRead.value ?? []
  const quoteRows = quoteRead.value ?? []
  const bookRows = bookRead.value ?? []
  const comparison = comparisonRead.value ?? null

  return (
    <section className="mt-4" aria-labelledby="instrument-workspace-title">
      <div className="mb-3 flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Instrument-scoped reads
          </p>
          <h2 id="instrument-workspace-title" className="mt-1 text-lg font-semibold">
            Trades, quotes, book, and data agreement
          </h2>
        </div>
      </div>
      {market ? <SelectedSourceSummary row={market} /> : null}
      {market?.orderBook ? <IndividualOrderBook book={market.orderBook} /> : null}
      <div className="grid gap-4 xl:grid-cols-2">
        <InstrumentPanel
          title="Last trades"
          available={trades.available}
          query={trades.query}
          parseError={tradeRead.error}
          empty="No current trade observation exists for this instrument. Quotes or book state may still be available."
        >
          {tradeRows.length ? (
            <ul className="divide-y divide-border">
              {tradeRows.map((trade) => (
                <li key={trade.key} className="grid gap-2 py-3 first:pt-0 last:pb-0 sm:grid-cols-[1fr_auto]">
                  <div>
                    <p className="text-xs font-medium">Trade observation · {trade.venueId}</p>
                    <p className="mt-1 font-mono text-[10px] text-muted-foreground">{trade.stableTradeId}</p>
                  </div>
                  <div className="text-left sm:text-right">
                    <p className="font-mono text-xs">{trade.priceTicks.toLocaleString()} ticks</p>
                    <p className="mt-1 text-[10px] text-muted-foreground">
                      {trade.quantityLots.toLocaleString()} lots
                      {trade.takerOrderType ? ` · ${trade.takerOrderType === "market" ? "Market" : "Limit"} taker` : ""}
                      {` · ${dateTime(trade.availableAt)}`}
                    </p>
                  </div>
                  <p className="text-[10px] text-muted-foreground sm:col-span-2">
                    {qualityName(trade.currentQuality)} · {truth(trade.fresh, "fresh", "stale")}
                  </p>
                </li>
              ))}
            </ul>
          ) : null}
        </InstrumentPanel>

        <InstrumentPanel
          title="Top quotes"
          available={quotes.available}
          query={quotes.query}
          parseError={quoteRead.error}
          empty="No current bid or ask was returned for this instrument."
        >
          {quoteRows.length ? (
            <ul className="space-y-3">
              {quoteRows.map((quote) => (
                <li key={quote.key} className="rounded-lg border border-border bg-background/35 p-3">
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-xs font-medium">Quote observation · {quote.venueId}</p>
                    <span className="text-[10px] text-muted-foreground">{qualityName(quote.currentQuality)}</span>
                  </div>
                  <div className="mt-3 grid grid-cols-2 gap-2">
                    <PriceLevel label="Bid" level={quote.bid} />
                    <PriceLevel label="Ask" level={quote.ask} />
                  </div>
                  <p className="mt-2 text-[10px] text-muted-foreground">
                    As of {dateTime(quote.asOf)} · evaluated {dateTime(quote.stateEvaluatedAt)}
                    {quote.crossed === true ? " · crossed quote" : ""}
                  </p>
                </li>
              ))}
            </ul>
          ) : null}
        </InstrumentPanel>

        <InstrumentPanel
          title="Order-book depth"
          available={books.available}
          query={books.query}
          parseError={bookRead.error}
          empty="No current order book was returned for this instrument."
        >
          {bookRows.length ? (
            <div className="space-y-3">
              {bookRows.map((book) => (
                <div key={book.key} className="rounded-lg border border-border bg-background/35 p-3">
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-xs font-medium">Order book · {book.venueId}</p>
                    <span className="text-[10px] text-muted-foreground">{qualityName(book.currentQuality)}</span>
                  </div>
                  <div className="mt-3 grid grid-cols-2 gap-3 text-[10px]">
                    <DepthSide label="Bids" levels={book.bids} completeness={book.bidCompleteness} />
                    <DepthSide label="Asks" levels={book.asks} completeness={book.askCompleteness} />
                  </div>
                  <p className="mt-2 text-[10px] text-muted-foreground">As of {dateTime(book.asOf)} · evaluated {dateTime(book.stateEvaluatedAt)}</p>
                </div>
              ))}
            </div>
          ) : null}
        </InstrumentPanel>

        <InstrumentPanel
          title="Independent data agreement"
          available={comparisons.available}
          query={comparisons.query}
          parseError={comparisonRead.error}
          empty="No comparable current observation was returned for this instrument."
        >
          {comparison ? (
            <div>
              <p className="text-xs font-medium">
                {comparison.comparable
                  ? `${comparison.observationCount} current observations can be compared.`
                  : "Only one current observation is available, so independent agreement cannot be measured."}
              </p>
              <ul className="mt-3 divide-y divide-border">
                {comparison.observations.map((observation, index) => (
                  <li key={observation.key} className="flex items-start justify-between gap-3 py-3 first:pt-0 last:pb-0">
                    <div>
                      <p className="text-xs font-medium">
                        Observation {index + 1} · {observation.venueId}
                      </p>
                      <p className="mt-1 text-[10px] text-muted-foreground">{qualityName(observation.currentQuality)} · {dateTime(observation.asOf)}</p>
                    </div>
                    <p className="font-mono text-[10px] text-muted-foreground">
                      {levelSummary(observation.bid)} / {levelSummary(observation.ask)}
                    </p>
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </InstrumentPanel>
      </div>
    </section>
  )
}

function SelectedSourceSummary({ row }: { row: UnifiedMarketRow }) {
  const source = row.selectedSource
  const observation = row.marketObservation
  const mark = runtimeDisplayMark(row)
  const receipt = row.selectionReceipt

  return (
    <section className="mb-4 rounded-xl border border-border bg-card/35 p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Current market data
          </p>
          <h3 className="mt-1 text-sm font-semibold">
            {source ? row.symbol : `${row.symbol} · current data unavailable`}
          </h3>
          <p className="mt-1 max-w-2xl text-[11px] leading-5 text-muted-foreground">
            {source
              ? mark
                ? "This is the best current market observation available. Historical analysis data is not ready yet."
                : "Current market data is available, but it has no fresh completed trade or bid-and-ask midpoint to display."
              : marketObservationUnavailableName(observation.reason)}
          </p>
          <p className="mt-2 text-[10px] font-medium text-amber-200">
            {analyticalReadinessName(row.analyticalReadiness)}
          </p>
        </div>
        <div className="flex flex-wrap justify-end gap-2">
          <EvidenceBadge
            label={mark ? "Current display price" : "Display price unavailable"}
            tone={mark ? "good" : "bad"}
          />
        </div>
      </div>
      <dl className="mt-4 grid gap-3 border-t border-border/70 pt-4 sm:grid-cols-2 lg:grid-cols-4">
        <Fact
          label="Current display price"
          value={mark ? `${mark.value} ${mark.currency}` : "Not available"}
        />
        <Fact label="Price basis" value={mark ? markBasisName(mark.basis) : "Not available"} />
        <Fact
          label="Price current through"
          value={
            mark
              ? mark.sourceValidUntil
                ? dateTime(mark.sourceValidUntil)
                : "No precise deadline reported"
              : "Not available"
          }
        />
        <Fact label="Updated" value={dateTime(receipt.selectedAt)} />
      </dl>
      <details className="mt-4 rounded-lg border border-border bg-background/30 p-3">
        <summary className="cursor-pointer text-xs font-semibold">
          Data confidence
        </summary>
        <dl className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Fact label="Venue" value={source?.venueId ?? "Not reported"} />
          <Fact label="Timing" value={source ? humanize(source.timing) : "Not available"} />
          <Fact label="Data health" value={source ? humanize(source.health) : "Not available"} />
          <Fact
            label="Quality"
            value={source ? humanize(source.quality) : "Not available"}
          />
          <Fact
            label="Market depth"
            value={
              source
                ? source.depth
                  ? humanize(source.depth)
                  : "No market book"
                : "Not available"
            }
          />
          <Fact
            label="Coverage"
            value={source ? humanize(source.coverage) : "Not available"}
          />
          <Fact
            label="Integrity"
            value={source ? humanize(source.integrity.state) : "Not available"}
          />
          <Fact
            label="Independent observations"
            value={receipt.eligibleCount.toLocaleString()}
          />
          <Fact label="Analytical use" value={analyticalReadinessName(row.analyticalReadiness)} />
        </dl>
      </details>
    </section>
  )
}

function IndividualOrderBook({
  book,
}: {
  book: NonNullable<UnifiedMarketRow["orderBook"]>
}) {
  const bids = book.orders.filter((order) => order.side === "bid")
  const asks = book.orders.filter((order) => order.side === "ask")
  return (
    <section className="mb-4 rounded-xl border border-border bg-card/35 p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Individual-order depth
          </p>
          <h3 className="mt-1 text-sm font-semibold">Orders behind the visible market</h3>
          <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
            Each row is a distinct live order. Market Squawk keeps them separate when several
            orders share the same price.
          </p>
        </div>
        <EvidenceBadge
          label={book.usableForSelection ? "Current order-level book" : humanize(book.phase)}
          tone={book.usableForSelection ? "good" : "bad"}
        />
      </div>
      <div className="mt-4 grid gap-4 sm:grid-cols-2">
        <IndividualOrderSide label="Buy orders" orders={bids} />
        <IndividualOrderSide label="Sell orders" orders={asks} />
      </div>
      <p className="mt-3 text-[10px] leading-5 text-muted-foreground">
        Showing {book.returnedOrderCount.toLocaleString()} of {book.totalOrderCount.toLocaleString()}
        {book.sampleTruncated ? " distinct orders; more are available." : " distinct orders."}
        {book.lastMarketAt ? ` Last market update ${dateTime(book.lastMarketAt)}.` : ""}
        {` Available to this installation ${dateTime(book.availableAt)}.`}
      </p>
    </section>
  )
}

function IndividualOrderSide({
  label,
  orders,
}: {
  label: string
  orders: NonNullable<UnifiedMarketRow["orderBook"]>["orders"]
}) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-3">
      <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      {orders.length ? (
        <ol className="mt-2 divide-y divide-border font-mono text-[10px]">
          {orders.slice(0, 10).map((order) => (
            <li key={order.orderId} className="grid grid-cols-[1fr_auto] gap-3 py-2 first:pt-0 last:pb-0">
              <p>{order.price}</p>
              <p className="text-right">{order.quantity}</p>
            </li>
          ))}
        </ol>
      ) : (
        <p className="mt-2 text-[10px] text-muted-foreground">No current orders available</p>
      )}
    </div>
  )
}

function InstrumentPanel({
  title,
  available,
  query,
  parseError,
  empty,
  children,
}: {
  title: string
  available: boolean
  query: UseQueryResult<ApplicationResult, Error>
  parseError: string | null
  empty: string
  children: React.ReactNode
}) {
  const stateRead = parseRead(() => resultState(query.data))
  const state = stateRead.value ?? null
  const boundaryError = parseError ?? stateRead.error
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <div className="mb-4 flex items-start justify-between gap-3">
        <h3 className="text-sm font-semibold">{title}</h3>
        {state ? (
          <span className="font-mono text-[9px] text-muted-foreground">
            {state.returned}/{state.available} · {humanize(state.completeness)}
          </span>
        ) : null}
      </div>
      {!available ? (
        <NoInstrumentData text="This market detail is unavailable in the current build." />
      ) : query.isPending ? (
        <Skeleton className="h-28 rounded-lg" />
      ) : query.isError ? (
        <NoInstrumentData text="This market detail is unavailable right now." />
      ) : boundaryError ? (
        <NoInstrumentData text="This market detail could not be loaded safely." />
      ) : query.data.metadata.returnedItems === 0 || !children ? (
        <NoInstrumentData text={empty} />
      ) : (
        children
      )}
    </section>
  )
}

function DepthSide({
  label,
  levels,
  completeness,
}: {
  label: string
  levels: BookLevel[]
  completeness: string | null
}) {
  return (
    <div>
      <p className="font-medium">{label} · {completeness ? humanize(completeness) : "Not reported"}</p>
      {levels.length ? (
        <ol className="mt-2 space-y-1 font-mono text-muted-foreground">
          {levels.slice(0, 6).map((level, index) => (
            <li key={`${level.priceTicks}:${level.quantityLots}:${index}`} className="flex justify-between gap-2">
              <span>{level.priceTicks.toLocaleString()}</span>
              <span>{level.quantityLots.toLocaleString()}</span>
            </li>
          ))}
        </ol>
      ) : (
        <p className="mt-2 text-muted-foreground">No levels returned</p>
      )}
    </div>
  )
}

function NoInstrumentData({ text }: { text: string }) {
  return (
    <div className="rounded-lg border border-dashed border-border p-4 text-xs leading-5 text-muted-foreground">
      {text}
    </div>
  )
}

function PriceLevel({ label, level }: { label: string; level: BookLevel | null }) {
  return (
    <div className="rounded-lg border border-border bg-background/45 p-3">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-2 font-mono text-lg font-semibold">
        {level ? `${level.priceTicks.toLocaleString()} ticks` : "Not reported"}
      </p>
      <p className="mt-1 text-[10px] text-muted-foreground">
        {level ? `${level.quantityLots.toLocaleString()} lots` : "No book level returned"}
      </p>
    </div>
  )
}

function Summary({
  label,
  value,
  icon: Icon,
}: {
  label: string
  value: number
  icon: typeof Activity
}) {
  return (
    <div className="rounded-xl border border-border bg-card/35 p-4">
      <Icon className="size-4 text-primary" aria-hidden="true" />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 font-mono text-2xl font-semibold">{value}</p>
    </div>
  )
}

function PageFrame({ children, action }: { children: React.ReactNode; action?: React.ReactNode }) {
  return (
    <div className="mx-auto w-full max-w-[1180px] p-5 lg:p-7">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
            Market Squawk · Live market view
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Markets</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Current market prices, trading activity, depth, freshness, and plain-language data
            confidence.
          </p>
        </div>
        {action}
      </div>
      <div className="mt-6">{children}</div>
    </div>
  )
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-6">
      <DatabaseZap className="size-5 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-4 text-base font-semibold">{title}</h2>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">{detail}</p>
    </section>
  )
}

function MarketsLoading() {
  return (
    <PageFrame>
      <MarketGridLoading />
    </PageFrame>
  )
}

function MarketGridLoading() {
  return (
    <div className="grid gap-4 xl:grid-cols-2">
      <Skeleton className="h-80 rounded-xl" />
      <Skeleton className="h-80 rounded-xl" />
    </div>
  )
}

function Notice({ text }: { text: string }) {
  return (
    <div className="mb-4 flex gap-2 rounded-lg border border-amber-400/20 bg-amber-400/5 p-3 text-xs text-amber-100">
      <CircleAlert className="size-4 shrink-0" aria-hidden="true" />
      {text}
    </div>
  )
}

function EvidenceBadge({ label, tone }: { label: string; tone: "good" | "bad" | "neutral" }) {
  const toneClass =
    tone === "good"
      ? "border-emerald-400/25 bg-emerald-400/10 text-emerald-300"
      : tone === "bad"
        ? "border-amber-400/25 bg-amber-400/10 text-amber-200"
        : "border-border bg-background/50 text-muted-foreground"
  return (
    <span className={`shrink-0 rounded-full border px-2.5 py-1 text-[10px] font-medium ${toneClass}`}>
      {label}
    </span>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-words text-xs text-foreground/85">{value}</dd>
    </div>
  )
}

function markBasisName(
  value: RuntimeDisplayMark["basis"],
): string {
  switch (value) {
    case "fresh_last_trade":
      return "Last completed trade"
    case "fresh_bid_ask_midpoint":
      return "Midpoint of current bid and ask"
  }
}

function marketObservationUnavailableName(
  value: Extract<
    UnifiedMarketRow["marketObservation"],
    { availability: "unavailable" }
  >["reason"],
): string {
  switch (value) {
    case "no_eligible_source":
      return "No current observation met the data requirements."
    case "durable_pit_evidence_not_established":
      return "Current market data is available, but historical analysis data is not ready yet."
  }
}

function analyticalReadinessName(
  _readiness: UnifiedMarketRow["analyticalReadiness"],
) {
  return "Current price only · unavailable for historical analysis"
}

function qualityName(value: string | null) {
  return value ? humanize(value) : "Not reported"
}

function truth(value: boolean | null, yes: string, no: string) {
  return value === null ? "Not reported" : value ? yes : no
}

function dateTime(value: string | null) {
  if (!value) return "Not reported"
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? "Not reported" : parsed.toLocaleString()
}

function requiredInstrument(value: string | null) {
  if (!value) throw new Error("Select a market instrument first.")
  return value
}

function levelSummary(value: BookLevel | null) {
  return value
    ? `${value.priceTicks.toLocaleString()} × ${value.quantityLots.toLocaleString()}`
    : "No level"
}

function parseRead<T>(read: () => T): { value: T | null; error: string | null } {
  try {
    return { value: read(), error: null }
  } catch (error) {
    return { value: null, error: messageFrom(error) }
  }
}

function usePageVisibility(): boolean {
  const [visible, setVisible] = React.useState(
    () => document.visibilityState === "visible",
  )
  React.useEffect(() => {
    const update = () => setVisible(document.visibilityState === "visible")
    document.addEventListener("visibilitychange", update)
    return () => document.removeEventListener("visibilitychange", update)
  }, [])
  return visible
}
