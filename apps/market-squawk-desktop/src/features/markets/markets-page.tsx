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
          title="Market service is unavailable"
          detail={product.error}
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
          row.displayName,
          row.permanentFigi,
          row.assetClass,
          row.quoteCurrency,
          row.selectedSource?.providerId,
          row.selectedSource?.providerSymbol,
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
  const feedError =
    feedRead.error ?? (feed.isError ? messageFrom(feed.error) : null)
  const universeError =
    universeRead.error ?? (universe.isError ? messageFrom(universe.error) : null)
  const feedFailed = feedError !== null
  const universeFailed = universeError !== null
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
          title="Unified Markets is not available in this service build"
          detail="Update the installed Market Squawk service, then reopen the app."
        />
      ) : feedFailed && universeFailed ? (
        <EmptyState
          title="Market data is temporarily unavailable"
          detail={feedError ?? universeError ?? "Market data could not be read."}
        />
      ) : rows.length === 0 &&
        referenceRows.length === 0 &&
        (feed.isLoading || universe.isLoading) ? (
        <MarketGridLoading />
      ) : rows.length === 0 && referenceRows.length === 0 && feedFailed ? (
        <EmptyState
          title="Live market observations are unavailable"
          detail={feedError ?? "Live market observations could not be read."}
        />
      ) : rows.length === 0 && referenceRows.length === 0 && universeFailed ? (
        <EmptyState
          title="U.S. listing search is unavailable"
          detail={universeError ?? "U.S. listing search could not be read."}
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
              text={`Live observations are unavailable. Official listing search remains usable. ${feedError}`}
            />
          ) : null}
          {universeFailed ? (
            <Notice
              text={`U.S. listing search is unavailable. Active live markets remain usable. ${universeError}`}
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
              Market Squawk chooses the best available source for each instrument and keeps any
              delay, coverage limit, or confidence downgrade visible.
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
                Show detailed trades, quotes, order book, and source comparison
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
          <UnifiedResultBoundary feed={resultState(feed.data)} />
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
        <Fact label="Price coverage" value="Provider credentials required" />
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
            Market Squawk found this U.S. listing without inventing a price. The supported no-cost
            U.S. IEX option still requires an Alpaca Paper account and API credentials. When an
            admitted runtime is available, this workspace can show its current source-bound market
            observation.
          </p>
        </div>
        <EvidenceBadge label="Provider account required for prices" tone="neutral" />
      </div>
      <dl className="mt-5 grid gap-4 border-t border-border/70 pt-4 sm:grid-cols-2 lg:grid-cols-4">
        <Fact label="Listing venue" value={row.venueId} />
        <Fact label="Asset type" value={row.isEtf ? "ETF" : "Stock"} />
        <Fact label="Reference quality" value="Official delayed" />
        <Fact label="Observed" value={dateTime(row.availableAt)} />
      </dl>
      <details className="mt-5 rounded-lg border border-border bg-background/30 p-3">
        <summary className="cursor-pointer text-xs font-semibold">Data confidence</summary>
        <dl className="mt-3 grid gap-3 sm:grid-cols-2">
          <Fact label="Provider" value={row.providerId} />
          <Fact label="Source" value={row.sourceId} />
          <Fact label="Source effective time" value={dateTime(row.effectiveAt)} />
          <Fact label="Payload SHA-256" value={row.sourcePayloadSha256} />
        </dl>
      </details>
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
      ? "The selected runtime source has no fresh completed trade or bid-and-ask midpoint."
      : marketObservationUnavailableName(row.marketObservation.reason)
  }
  const validThrough = mark.sourceValidUntil
    ? ` · source valid through ${dateTime(mark.sourceValidUntil)}`
    : " · no precise source deadline reported"
  return `${markBasisName(mark.basis)} · runtime display only${validThrough}`
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
            {row.displayName ?? source?.providerId ?? "No source available"}
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
          <span>Source synchronization is incomplete. This observation is not treated as current.</span>
        </div>
      ) : null}
      <p className="mt-4 truncate font-mono text-[9px] text-muted-foreground">
        {row.instrumentId}
      </p>
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
            Trades, quotes, book, and cross-source comparison
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
                    <p className="text-xs font-medium">{trade.sourceId} · {trade.venueId}</p>
                    <p className="mt-1 font-mono text-[10px] text-muted-foreground">{trade.stableTradeId}</p>
                  </div>
                  <div className="text-left sm:text-right">
                    <p className="font-mono text-xs">{trade.priceTicks.toLocaleString()} ticks</p>
                    <p className="mt-1 text-[10px] text-muted-foreground">{trade.quantityLots.toLocaleString()} lots · {dateTime(trade.availableAt)}</p>
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
                    <p className="text-xs font-medium">{quote.sourceId} · {quote.venueId}</p>
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
                    <p className="text-xs font-medium">{book.sourceId} · {book.venueId}</p>
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
          title="Cross-source comparison"
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
                  : "Only one current observation is available; a cross-source comparison is not possible."}
              </p>
              <ul className="mt-3 divide-y divide-border">
                {comparison.observations.map((observation) => (
                  <li key={observation.key} className="flex items-start justify-between gap-3 py-3 first:pt-0 last:pb-0">
                    <div>
                      <p className="text-xs font-medium">{observation.sourceId} · {observation.venueId}</p>
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
            Selected market evidence
          </p>
          <h3 className="mt-1 text-sm font-semibold">
            {source
              ? `${humanize(source.providerId)} · ${source.providerSymbol ?? row.symbol}`
              : `${row.symbol} · no eligible source`}
          </h3>
          <p className="mt-1 max-w-2xl text-[11px] leading-5 text-muted-foreground">
            {source
              ? mark
                ? "This current display price comes from the exact selected runtime source. This live-feed response does not establish durable point-in-time evidence for investment analysis."
                : "The runtime source remains selected, but it has no fresh completed trade or bid-and-ask midpoint to display."
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
          <EvidenceBadge
            label="Runtime display only"
            tone="neutral"
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
          label="Source valid through"
          value={
            mark
              ? mark.sourceValidUntil
                ? dateTime(mark.sourceValidUntil)
                : "No precise deadline reported"
              : "Not available"
          }
        />
        <Fact label="Selected at" value={dateTime(receipt.selectedAt)} />
      </dl>
      <details className="mt-4 rounded-lg border border-border bg-background/30 p-3">
        <summary className="cursor-pointer text-xs font-semibold">
          Source, quality, and evidence details
        </summary>
        <dl className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Fact label="Provider" value={source?.providerId ?? "Not selected"} />
          <Fact label="Source" value={source?.sourceId ?? "Not selected"} />
          <Fact label="Venue" value={source?.venueId ?? "Not reported"} />
          <Fact label="Provider channel" value={source?.providerChannel ?? "Not selected"} />
          <Fact label="Timing" value={source ? humanize(source.timing) : "Not available"} />
          <Fact label="Source health" value={source ? humanize(source.health) : "Not available"} />
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
            label="Connection generation"
            value={source?.integrity.connectionGeneration ?? "Not available"}
          />
          <Fact
            label="Eligible sources"
            value={`${receipt.eligibleCount.toLocaleString()} eligible · ${receipt.rejectedCount.toLocaleString()} rejected`}
          />
          <Fact
            label="Selection result"
            value={receipt.selectionClass ? humanize(receipt.selectionClass) : "No source selected"}
          />
          <Fact
            label="Selection downgrades"
            value={marketDowngradeSummary(receipt.downgradeDimensions)}
          />
          <Fact
            label="Selection receipt"
            value={digestName(receipt.selectionDigest)}
          />
          <Fact
            label="Selection policy"
            value={`Revision ${receipt.policyRevision.toLocaleString()} · up to ${receipt.policyCandidateLimit.toLocaleString()} sources`}
          />
          <Fact label="Policy digest" value={digestName(receipt.policyDigest)} />
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
            Each row is a distinct provider order. Market Squawk keeps these separate even when
            several orders share the same price.
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
        {book.sampleTruncated ? " distinct orders in a bounded identity-stable sample." : " distinct orders."}
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
              <div>
                <p>{order.price}</p>
                <p className="mt-0.5 text-muted-foreground">{shortOrderId(order.orderId)}</p>
              </div>
              <p className="text-right">{order.quantity}</p>
            </li>
          ))}
        </ol>
      ) : (
        <p className="mt-2 text-[10px] text-muted-foreground">No orders in this bounded sample</p>
      )}
    </div>
  )
}

function shortOrderId(orderId: string) {
  return orderId.length <= 18 ? orderId : `${orderId.slice(0, 8)}…${orderId.slice(-6)}`
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
        <NoInstrumentData text="This installed service does not expose this bounded market read." />
      ) : query.isPending ? (
        <Skeleton className="h-28 rounded-lg" />
      ) : query.isError ? (
        <NoInstrumentData text={messageFrom(query.error)} />
      ) : boundaryError ? (
        <NoInstrumentData text={boundaryError} />
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

function UnifiedResultBoundary({ feed }: { feed: ReturnType<typeof resultState> }) {
  return (
    <p className="mt-4 text-[10px] leading-relaxed text-muted-foreground">
      Unified market result: {boundary(feed)}. Each displayed price uses the exact canonical
      instrument scale, while source, coverage, freshness, and confidence remain attached.
    </p>
  )
}

function PageFrame({ children, action }: { children: React.ReactNode; action?: React.ReactNode }) {
  return (
    <div className="mx-auto w-full max-w-[1180px] p-5 lg:p-7">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
            Market Squawk · Current runtime
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Markets</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Current market state with its source, venue, time, freshness, quality, and integrity
            evidence kept visible.
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
      return "No source met the current data requirements."
    case "durable_pit_evidence_not_established":
      return "The live source is usable for current display, but this live-feed response does not establish durable point-in-time evidence for investment analysis."
  }
}

function analyticalReadinessName(
  _readiness: UnifiedMarketRow["analyticalReadiness"],
) {
  return "Live runtime display · this feed is not PIT evidence"
}

function marketDowngradeSummary(
  values: UnifiedMarketRow["selectionReceipt"]["downgradeDimensions"],
): string {
  if (values.length === 0) return "None"
  return values
    .map((value) => {
      switch (value.dimension) {
        case "timing":
          return `Timing: ${humanize(value.required)} to ${humanize(value.selected)}`
        case "depth":
          return `Depth: ${humanize(value.minimum)} to ${value.selected ? humanize(value.selected) : "no market book"}`
        case "quality":
          return `Quality: ${humanize(value.minimum)} to ${humanize(value.selected)}`
        case "coverage":
          return `Coverage: ${humanize(value.required)} to ${humanize(value.selected)}`
        case "freshness":
          return "Freshness: older than requested"
      }
    })
    .join(" · ")
}

function digestName(value: { algorithm: "sha256" | "blake3"; bytes: string }): string {
  const algorithm = value.algorithm === "sha256" ? "SHA-256" : "BLAKE3"
  return `${algorithm} · ${value.bytes}`
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

function boundary(value: ReturnType<typeof resultState>) {
  return value
    ? `${humanize(value.completeness)}, ${value.returned} of ${value.available} rows`
    : "unavailable"
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
