import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import {
  Activity,
  CircleAlert,
  Clock3,
  DatabaseZap,
  RefreshCw,
  Search,
} from "lucide-react"
import { useSearchParams } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  marketInstrumentRow,
  marketOverviewRows,
  type MarketProductRow,
} from "./market-product"
import {
  type ReferenceMarketRow,
  referenceMarketRows,
} from "./reference-market"

const MARKET_POLL_INTERVAL_MS = 5_000

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
  const overviewAvailable = operationNames.has("Market.GetOverview")
  const instrumentAvailable = operationNames.has("Market.GetInstrument")
  const universeAvailable = operationNames.has("Market.SearchUniverse")
  const pageVisible = usePageVisibility()
  const overview = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "market",
      "Market.GetOverview",
      {},
    ),
    enabled: overviewAvailable,
    queryFn: () => transport.query({ query: "marketOverview" }),
    refetchInterval:
      overviewAvailable && pageVisible ? MARKET_POLL_INTERVAL_MS : false,
    refetchIntervalInBackground: false,
  })
  const overviewRead = parseRead(() => marketOverviewRows(overview.data))
  const rows = overviewRead.value ?? []
  const [searchText, setSearchText] = React.useState("")
  const [referenceQuery, setReferenceQuery] = React.useState("")
  React.useEffect(() => {
    const timer = window.setTimeout(() => setReferenceQuery(searchText.trim()), 250)
    return () => window.clearTimeout(timer)
  }, [searchText])
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
  const currentSymbols = new Set(
    rows.flatMap((row) =>
      row.displaySymbol ? [row.displaySymbol.toLocaleUpperCase()] : [],
    ),
  )
  const references = (universeRead.value ?? []).filter(
    (row) => !currentSymbols.has(row.symbol.toLocaleUpperCase()),
  )
  const normalizedSearch = searchText.trim().toLocaleLowerCase()
  const visibleRows = normalizedSearch
    ? rows.filter((row) =>
        [row.displaySymbol, row.name, assetClassName(row.assetClass), row.currency]
          .filter((value): value is string => Boolean(value))
          .some((value) => value.toLocaleLowerCase().includes(normalizedSearch)),
      )
    : rows
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
      ? selectedId ?? rows[0]?.instrumentId ?? null
      : null
  const overviewSelection =
    rows.find((row) => row.instrumentId === selectedInstrument) ?? null
  const selectedReference =
    references.find((row) => row.referenceId === selectedReferenceId) ?? null
  const instrument = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "market",
      "Market.GetInstrument",
      { instrumentId: selectedInstrument },
    ),
    enabled: instrumentAvailable && selectedInstrument !== null,
    queryFn: () =>
      transport.query({
        query: "marketInstrument",
        instrumentId: requiredInstrument(selectedInstrument),
      }),
    refetchInterval:
      instrumentAvailable && selectedInstrument !== null && pageVisible
        ? MARKET_POLL_INTERVAL_MS
        : false,
    refetchIntervalInBackground: false,
  })
  const instrumentRead = parseRead(() =>
    selectedInstrument
      ? marketInstrumentRow(instrument.data, selectedInstrument)
      : null,
  )
  const selectedMarket = instrumentRead.value ?? overviewSelection
  const overviewFailed = overviewRead.error !== null || overview.isError
  const universeFailed = universeRead.error !== null || universe.isError
  const instrumentFailed = instrumentRead.error !== null || instrument.isError
  const refreshing =
    overview.isFetching || universe.isFetching || instrument.isFetching
  const live = rows.filter((row) => row.availability === "live").length
  const currentPrices = rows.filter((row) => row.currentPrice !== null).length

  const refresh = () => {
    void Promise.all([
      ...(overviewAvailable ? [overview.refetch()] : []),
      ...(universeAvailable ? [universe.refetch()] : []),
      ...(instrumentAvailable && selectedInstrument ? [instrument.refetch()] : []),
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
          Refresh markets
        </Button>
      }
    >
      {!overviewAvailable && !universeAvailable ? (
        <EmptyState
          title="Markets are unavailable"
          detail="Reopen Market Squawk, then try again."
        />
      ) : overviewFailed && universeFailed ? (
        <EmptyState
          title="Market information is temporarily unavailable"
          detail="Try again or review Logs & Diagnostics for details."
        />
      ) : rows.length === 0 &&
        references.length === 0 &&
        (overview.isLoading || universe.isLoading) ? (
        <MarketGridLoading />
      ) : rows.length === 0 && references.length === 0 && overviewFailed ? (
        <EmptyState
          title="Current prices are unavailable"
          detail="Check Connections & Sources, then try again."
        />
      ) : rows.length === 0 && references.length === 0 && universeFailed ? (
        <EmptyState
          title="Investment search is unavailable"
          detail="Try again or review Logs & Diagnostics for details."
        />
      ) : rows.length === 0 && references.length === 0 ? (
        <EmptyState
          title="No markets are available yet"
          detail="Check Connections & Sources, then refresh this page."
        />
      ) : (
        <>
          {overviewFailed ? (
            <Notice text="Current prices are unavailable. Investment search remains usable." />
          ) : null}
          {universeFailed ? (
            <Notice text="Investment search is unavailable. Current markets remain usable." />
          ) : null}
          <div className="grid gap-3 sm:grid-cols-3">
            <Summary
              label="Markets in view"
              value={rows.length + references.length}
              icon={Activity}
            />
            <Summary label="Live now" value={live} icon={Clock3} />
            <Summary label="Current prices" value={currentPrices} icon={DatabaseZap} />
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
              Market Squawk uses the best available information and clearly labels delays,
              coverage limits, and confidence.
            </p>
          </section>
          {visibleRows.length === 0 && references.length === 0 ? (
            <Notice text="No market or listed investment matches that search." />
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
              {references.map((row) => (
                <ReferenceMarketCard
                  key={row.referenceId}
                  row={row}
                  selected={row.referenceId === selectedReferenceId}
                  onSelect={() => selectReference(row.referenceId)}
                />
              ))}
            </div>
          )}
          {selectedInstrument ? (
            !instrumentAvailable ? (
              <Notice text="Detailed market information is unavailable right now." />
            ) : instrument.isPending && !overviewSelection ? (
              <Skeleton className="mt-5 h-64 rounded-xl" />
            ) : instrumentFailed && !overviewSelection ? (
              <Notice text="Detailed market information could not be loaded. Try refreshing the page." />
            ) : selectedMarket ? (
              <MarketWorkspace row={selectedMarket} detailLimited={instrumentFailed} />
            ) : (
              <Notice text="Current information is unavailable for the selected investment." />
            )
          ) : null}
          {selectedReference ? (
            <ReferenceWorkspace row={selectedReference} />
          ) : selectedReferenceId ? (
            <Notice text="That listing is no longer present in the current search result." />
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

function MarketCard({ row, selected, onSelect }: {
  row: MarketProductRow
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
            {assetClassName(row.assetClass)} · {row.currency}
          </p>
          <h2 className="mt-2 truncate text-xl font-semibold">
            {row.displaySymbol ?? row.name ?? assetClassName(row.assetClass)}
          </h2>
          <p className="mt-1 truncate text-[11px] text-muted-foreground">
            {row.name ?? assetClassName(row.assetClass)}
          </p>
        </div>
        <EvidenceBadge
          label={availabilityName(row.availability)}
          tone={
            row.availability === "live" && row.marketState.health === "healthy"
              ? "good"
              : row.availability === "unavailable" || row.availability === "stale"
                ? "bad"
                : "neutral"
          }
        />
      </div>
      <div className="mt-5 rounded-lg border border-border/70 bg-background/35 p-3">
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Current price
        </p>
        <p className="mt-1 font-mono text-2xl font-semibold">
          {price(row.currentPrice?.value ?? null, row.currency)}
        </p>
        <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
          {row.currentPrice
            ? `${priceBasisName(row.currentPrice.basis)}${
                row.currentPrice.currentThrough
                  ? ` · current through ${dateTime(row.currentPrice.currentThrough)}`
                  : ""
              }`
            : "No current trade or bid-and-ask midpoint is available."}
        </p>
      </div>
      <dl className="mt-5 grid grid-cols-2 gap-x-4 gap-y-3 border-t border-border/70 pt-4">
        <Fact label="Bid" value={price(row.quote.bidPrice, row.currency)} />
        <Fact label="Ask" value={price(row.quote.askPrice, row.currency)} />
        <Fact label="Confidence" value={confidenceName(row.confidence)} />
        <Fact label="Coverage" value={coverageName(row.marketState.coverage)} />
        <Fact label="Market depth" value={depthName(row.depthSummary.kind)} />
        <Fact label="Updated" value={dateTime(row.marketState.updatedAt)} />
      </dl>
      {row.analysisUse === "current_only" ? (
        <p className="mt-4 text-[10px] leading-5 text-amber-200">
          Current-market view only. Historical analysis is not available yet.
        </p>
      ) : null}
    </button>
  )
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
            {row.isEtf ? "ETF" : "Stock"}
          </p>
          <h2 className="mt-2 truncate text-xl font-semibold">{row.symbol}</h2>
          <p className="mt-1 line-clamp-2 text-[11px] leading-5 text-muted-foreground">
            {row.name}
          </p>
        </div>
        <EvidenceBadge label="Listed" tone="neutral" />
      </div>
      <div className="mt-5 rounded-lg border border-border/70 bg-background/35 p-3">
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Current price
        </p>
        <p className="mt-1 text-sm font-medium">Not available</p>
      </div>
      <dl className="mt-5 grid grid-cols-2 gap-x-4 gap-y-3 border-t border-border/70 pt-4">
        <Fact label="Identity" value="U.S. listed investment" />
        <Fact label="Asset type" value={row.isEtf ? "ETF" : "Stock"} />
        <Fact label="Price coverage" value="Current price unavailable" />
        <Fact label="Updated" value={dateTime(row.availableAt)} />
      </dl>
    </button>
  )
}

function MarketWorkspace({
  row,
  detailLimited,
}: {
  row: MarketProductRow
  detailLimited: boolean
}) {
  return (
    <section className="mt-5 rounded-xl border border-border bg-card/30 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Current market information
          </p>
          <h2 className="mt-2 text-lg font-semibold">
            {row.displaySymbol ?? row.name ?? assetClassName(row.assetClass)}
            {row.displaySymbol && row.name ? ` · ${row.name}` : ""}
          </h2>
          <p className="mt-2 max-w-2xl text-xs leading-5 text-muted-foreground">
            {row.currentPrice
              ? "This view uses the best current information available for this investment."
              : "A current price is not available, but the remaining market information may still be useful."}
          </p>
        </div>
        <EvidenceBadge
          label={confidenceName(row.confidence)}
          tone={row.confidence === "high" ? "good" : row.confidence === "unavailable" ? "bad" : "neutral"}
        />
      </div>
      {detailLimited ? (
        <Notice text="Some detailed market information could not be refreshed. The latest summary remains visible." />
      ) : null}
      <dl className="mt-5 grid gap-4 border-t border-border/70 pt-4 sm:grid-cols-2 lg:grid-cols-4">
        <Fact
          label="Current price"
          value={price(row.currentPrice?.value ?? null, row.currency)}
        />
        <Fact
          label="Price basis"
          value={row.currentPrice ? priceBasisName(row.currentPrice.basis) : "Not available"}
        />
        <Fact label="Bid" value={priceWithSize(row.quote.bidPrice, row.quote.bidSize, row.currency)} />
        <Fact label="Ask" value={priceWithSize(row.quote.askPrice, row.quote.askSize, row.currency)} />
        <Fact label="Last trade" value={priceWithSize(row.quote.lastPrice, row.quote.lastSize, row.currency)} />
        <Fact label="Availability" value={availabilityName(row.availability)} />
        <Fact label="Market depth" value={depthName(row.depthSummary.kind)} />
        <Fact label="Updated" value={dateTime(row.marketState.updatedAt)} />
      </dl>
      <details className="mt-5 rounded-lg border border-border bg-background/30 p-4">
        <summary className="cursor-pointer text-xs font-semibold">Data confidence</summary>
        <dl className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Fact label="Confidence" value={confidenceName(row.confidence)} />
          <Fact label="Freshness" value={freshnessName(row.marketState.freshness)} />
          <Fact label="Timing" value={timingName(row.marketState.timing)} />
          <Fact label="Quality" value={qualityName(row.marketState.quality)} />
          <Fact label="Coverage" value={coverageName(row.marketState.coverage)} />
          <Fact
            label="Current through"
            value={dateTime(row.marketState.currentThrough)}
          />
          <Fact
            label="Usable observations"
            value={row.observations.admittedCount.toLocaleString()}
          />
          <Fact label="Independent agreement" value="Not established" />
        </dl>
        <p className="mt-4 text-[11px] leading-5 text-muted-foreground">
          The observation count shows usable current information. It does not prove that the
          observations are independent or that they agree.
        </p>
      </details>
      <DepthWorkspace row={row} />
      {row.analysisUse === "current_only" ? (
        <p className="mt-5 text-xs leading-5 text-amber-200">
          This current-market view is not yet available for forecasts, backtests, or other
          historical analysis.
        </p>
      ) : null}
    </section>
  )
}

function DepthWorkspace({ row }: { row: MarketProductRow }) {
  const details = row.depthDetails
  if (!details || details.kind === "none") {
    return (
      <div className="mt-5 rounded-lg border border-dashed border-border p-4 text-xs text-muted-foreground">
        Detailed market depth is not available for this investment.
      </div>
    )
  }
  return (
    <section className="mt-5 rounded-lg border border-border bg-background/30 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold">Market depth</h3>
          <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
            Current buy and sell interest at the available price levels.
          </p>
        </div>
        <EvidenceBadge label={depthName(details.kind)} tone="neutral" />
      </div>
      <div className="mt-4 grid gap-4 sm:grid-cols-2">
        <DepthSide label="Buy interest" levels={details.bids} currency={row.currency} />
        <DepthSide label="Sell interest" levels={details.asks} currency={row.currency} />
      </div>
      {details.individualOrders ? (
        <div className="mt-4 grid gap-4 sm:grid-cols-2">
          <DepthSide
            label="Individual buy orders"
            levels={details.individualOrders.bidOrders}
            currency={row.currency}
          />
          <DepthSide
            label="Individual sell orders"
            levels={details.individualOrders.askOrders}
            currency={row.currency}
          />
        </div>
      ) : null}
      <p className="mt-4 text-[10px] leading-5 text-muted-foreground">
        {depthLimitation(row)}
      </p>
    </section>
  )
}

function DepthSide({
  label,
  levels,
  currency,
}: {
  label: string
  levels: Array<{ price: string; quantity: string }>
  currency: string
}) {
  return (
    <div className="rounded-lg border border-border bg-card/30 p-3">
      <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      {levels.length ? (
        <ol className="mt-3 divide-y divide-border font-mono text-[10px]">
          {levels.map((level, index) => (
            <li key={`${level.price}:${level.quantity}:${index}`} className="flex justify-between gap-3 py-2 first:pt-0 last:pb-0">
              <span>{price(level.price, currency)}</span>
              <span className="text-right">{level.quantity}</span>
            </li>
          ))}
        </ol>
      ) : (
        <p className="mt-2 text-[10px] text-muted-foreground">No current levels</p>
      )}
    </div>
  )
}

function ReferenceWorkspace({ row }: { row: ReferenceMarketRow }) {
  return (
    <section className="mt-5 rounded-xl border border-border bg-card/30 p-5">
      <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
        Listed investment
      </p>
      <div className="mt-2 flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold">
            {row.symbol} · {row.name}
          </h2>
          <p className="mt-2 max-w-2xl text-xs leading-5 text-muted-foreground">
            This investment is listed, but a current price is not available. Check Connections
            &amp; Sources if you expected current market coverage.
          </p>
        </div>
        <EvidenceBadge label="Current price unavailable" tone="neutral" />
      </div>
      <dl className="mt-5 grid gap-4 border-t border-border/70 pt-4 sm:grid-cols-2 lg:grid-cols-3">
        <Fact label="Asset type" value={row.isEtf ? "ETF" : "Stock"} />
        <Fact label="Listing effective" value={dateTime(row.effectiveAt)} />
        <Fact label="Updated" value={dateTime(row.availableAt)} />
      </dl>
    </section>
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
            Market Squawk · Current market view
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Markets</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Current prices, quotes, market depth, freshness, and plain-language data confidence.
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
    <div className="my-4 flex gap-2 rounded-lg border border-amber-400/20 bg-amber-400/5 p-3 text-xs text-amber-100">
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

function assetClassName(value: MarketProductRow["assetClass"]): string {
  switch (value) {
    case "equity": return "Stock"
    case "fixed_income": return "Fixed income"
    case "option": return "Option"
    case "future": return "Futures"
    case "foreign_exchange": return "Foreign exchange"
    case "crypto": return "Crypto"
    case "commodity": return "Commodity"
    case "fund": return "Fund"
    case "index": return "Index"
    case "cash": return "Cash"
  }
}

function availabilityName(value: MarketProductRow["availability"]): string {
  switch (value) {
    case "live": return "Live"
    case "delayed": return "Delayed"
    case "end_of_day": return "End of day"
    case "stored": return "Stored"
    case "stale": return "Out of date"
    case "unavailable": return "Unavailable"
  }
}

function confidenceName(value: MarketProductRow["confidence"]): string {
  switch (value) {
    case "high": return "High confidence"
    case "moderate": return "Moderate confidence"
    case "limited": return "Limited confidence"
    case "unavailable": return "Confidence unavailable"
  }
}

function coverageName(value: MarketProductRow["marketState"]["coverage"]): string {
  switch (value) {
    case "broad": return "Broad"
    case "partial": return "Partial"
    case "single_market": return "One market"
    case "benchmark": return "Benchmark"
    case "reference": return "Reference information"
    case "account_owned": return "Connected account"
    case "unavailable": return "Unavailable"
  }
}

function depthName(value: MarketProductRow["depthSummary"]["kind"]): string {
  switch (value) {
    case "top_of_book": return "Best bid and ask"
    case "price_level": return "Multiple price levels"
    case "order_level": return "Individual orders"
    case "none": return "Unavailable"
  }
}

function freshnessName(value: MarketProductRow["marketState"]["freshness"]): string {
  switch (value) {
    case "fresh": return "Current"
    case "stale": return "Out of date"
    case "unavailable": return "Unavailable"
  }
}

function timingName(value: MarketProductRow["marketState"]["timing"]): string {
  switch (value) {
    case "real_time": return "Real time"
    case "delayed": return "Delayed"
    case "end_of_day": return "End of day"
    case "historical": return "Historical"
    case "stored": return "Stored"
    case null: return "Not reported"
  }
}

function qualityName(value: MarketProductRow["marketState"]["quality"]): string {
  switch (value) {
    case "verified": return "Verified"
    case "direct": return "Direct observation"
    case "official_delayed": return "Official, delayed"
    case "aggregated": return "Combined observation"
    case "indicative": return "Indicative"
    case "modeled": return "Modeled"
    case "estimated": return "Estimated"
    case "stale": return "Out of date"
    case "unavailable": return "Unavailable"
  }
}

function price(value: string | null, currency: string): string {
  return value ? `${value} ${currency}` : "Not available"
}

function priceWithSize(
  priceValue: string | null,
  size: string | null,
  currency: string,
): string {
  if (!priceValue) return "Not available"
  return size ? `${priceValue} ${currency} · size ${size}` : `${priceValue} ${currency}`
}

function priceBasisName(value: NonNullable<MarketProductRow["currentPrice"]>["basis"]): string {
  return value === "last_trade" ? "Last completed trade" : "Midpoint of current bid and ask"
}

function depthLimitation(row: MarketProductRow): string {
  const details = row.depthDetails?.individualOrders
  if (details) {
    return details.truncated
      ? `Showing ${details.returnedCount.toLocaleString()} of ${details.totalCount.toLocaleString()} available individual orders.`
      : `${details.totalCount.toLocaleString()} individual orders are available.`
  }
  if (row.depthSummary.truncated) {
    return "Only part of the available market depth is shown."
  }
  return "All available price levels are shown."
}

function dateTime(value: string | null) {
  if (!value) return "Not reported"
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? "Not reported" : parsed.toLocaleString()
}

function requiredInstrument(value: string | null) {
  if (!value) throw new Error("Select an investment first.")
  return value
}

function parseRead<T>(read: () => T): { value: T | null; error: string | null } {
  try {
    return { value: read(), error: null }
  } catch {
    return {
      value: null,
      error: "This market information is unavailable. Try refreshing the page.",
    }
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
