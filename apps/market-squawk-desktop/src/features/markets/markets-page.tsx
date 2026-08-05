import * as React from "react"
import { useQuery, type UseQueryResult } from "@tanstack/react-query"
import {
  Activity,
  CircleAlert,
  Clock3,
  DatabaseZap,
  RefreshCw,
  ShieldCheck,
} from "lucide-react"
import { useSearchParams } from "react-router-dom"

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { humanize } from "@/lib/formatters"
import type { ApplicationResult, DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  instrumentLookupDetailSchema,
  lookupResultSchema,
} from "../lookup/schemas"

import {
  type BookLevel,
  instrumentBooks,
  instrumentComparison,
  instrumentQuotes,
  instrumentTrades,
  marketEvidence,
  resultState,
} from "./market-evidence"

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
  const snapshot = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Market",
      "Market.GetSnapshot",
      {},
    ),
    queryFn: () => transport.query({ query: "marketSnapshot" }),
  })
  const quality = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Market",
      "Market.GetQuality",
      {},
    ),
    queryFn: () => transport.query({ query: "marketQuality" }),
  })
  const evidenceRead = parseRead(() => marketEvidence(snapshot.data, quality.data))
  const evidence = evidenceRead.value ?? []
  const instrumentIds = [
    ...new Set([
      ...(requestedInstrumentId ? [requestedInstrumentId] : []),
      ...evidence.map((row) => row.instrumentId),
    ]),
  ]
  const [selectedId, setSelectedId] =
    React.useState<string | null>(requestedInstrumentId)
  React.useEffect(() => {
    if (requestedInstrumentId) setSelectedId(requestedInstrumentId)
  }, [requestedInstrumentId])
  const selectedInstrument =
    instrumentIds.find((instrumentId) => instrumentId === selectedId) ??
    instrumentIds[0] ??
    null
  const operationNames = new Set(
    bootstrap.operations.map((operation) => operation.name),
  )
  const identity = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Analysis",
      "Analysis.Lookup",
      { instrumentId: selectedInstrument },
    ),
    enabled:
      selectedInstrument !== null && operationNames.has("Analysis.Lookup"),
    queryFn: () =>
      transport.query({
        query: "lookup",
        text: requiredInstrument(selectedInstrument),
        categories: ["instrument"],
      }),
    staleTime: 30_000,
  })
  const selectedInstrumentLabel = lookupInstrumentLabel(
    identity.data,
    selectedInstrument,
  )
  const tradesAvailable = operationNames.has("Market.GetTrades")
  const quotesAvailable = operationNames.has("Market.GetQuotes")
  const booksAvailable = operationNames.has("Market.GetBooks")
  const comparisonsAvailable = operationNames.has("Market.GetComparisons")
  const trades = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Market",
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
      "Market",
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
      "Market",
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
      "Market",
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
    snapshot.isFetching ||
    quality.isFetching ||
    trades.isFetching ||
    quotes.isFetching ||
    books.isFetching ||
    comparisons.isFetching
  const failed =
    evidenceRead.error !== null ||
    (evidence.length === 0 && (snapshot.isError || quality.isError))
  const partialFailure = snapshot.isError !== quality.isError
  const fresh = evidence.filter((row) => row.fresh === true).length
  const verified = evidence.filter(
    (row) => row.currentQuality === "direct_verified",
  ).length

  const refresh = () => {
    void Promise.all([
      snapshot.refetch(),
      quality.refetch(),
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
    setSearchParams({ instrumentId }, { replace: true })
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
      {failed ? (
        <EmptyState
          title={
            partialFailure
              ? "Market evidence is incomplete"
              : "Market evidence is unavailable"
          }
          detail={evidenceRead.error ?? messageFrom(snapshot.error ?? quality.error)}
        />
      ) : evidence.length === 0 && (snapshot.isLoading || quality.isLoading) ? (
        <MarketGridLoading />
      ) : evidence.length === 0 && selectedInstrument === null ? (
        <EmptyState
          title="No active market observations"
          detail="Connect and start a supported market source. Market Squawk will only show a market as current when the installed service returns timestamped runtime evidence."
        />
      ) : (
        <>
          {partialFailure ? (
            <Notice text="One market evidence view could not be read. The cards below show only the fields returned by the remaining view." />
          ) : null}
          <div className="grid gap-3 sm:grid-cols-3">
            <Summary label="Observed streams" value={evidence.length} icon={Activity} />
            <Summary label="Fresh at check" value={fresh} icon={Clock3} />
            <Summary label="Direct verified" value={verified} icon={ShieldCheck} />
          </div>
          <section className="mt-4 rounded-xl border border-border bg-card/35 p-4">
            <label htmlFor="market-instrument" className="text-xs font-semibold">
              Selected instrument
            </label>
            <select
              id="market-instrument"
              className="mt-2 h-9 w-full max-w-xl rounded-md border border-input bg-background px-3 font-mono text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring"
              value={selectedInstrument ?? ""}
              onChange={(event) => selectInstrument(event.target.value)}
            >
              {instrumentIds.map((instrumentId) => (
                <option key={instrumentId} value={instrumentId}>
                  {instrumentId === selectedInstrument && selectedInstrumentLabel
                    ? selectedInstrumentLabel
                    : instrumentId}
                </option>
              ))}
            </select>
            <p className="mt-2 text-[10px] leading-4 text-muted-foreground">
              Every detail read below is bounded to this exact instrument identifier. Values remain
              in authoritative ticks and lots because no display scale is present in this contract.
            </p>
          </section>
          {selectedInstrument &&
          !evidence.some((row) => row.instrumentId === selectedInstrument) ? (
            <Notice text="This instrument is selected from the reference catalog, but no active live market observation is available for it. The instrument-scoped reads below remain bound to this exact identity." />
          ) : null}
          <div className="mt-4 grid gap-4 xl:grid-cols-2">
            {evidence
              .filter((row) => row.instrumentId === selectedInstrument)
              .map((row) => (
                <MarketCard
                  key={row.key}
                  evidence={row}
                  instrumentLabel={selectedInstrumentLabel}
                />
              ))}
          </div>
          {selectedInstrument ? (
            <InstrumentWorkspace
              instrumentId={selectedInstrument}
              trades={{ available: tradesAvailable, query: trades }}
              quotes={{ available: quotesAvailable, query: quotes }}
              books={{ available: booksAvailable, query: books }}
              comparisons={{
                available: comparisonsAvailable,
                query: comparisons,
              }}
            />
          ) : null}
          <ResultBoundary
            snapshot={resultState(snapshot.data)}
            quality={resultState(quality.data)}
          />
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

function MarketCard({
  evidence,
  instrumentLabel,
}: {
  evidence: ReturnType<typeof marketEvidence>[number]
  instrumentLabel: string | null
}) {
  const integrityIssues = [
    evidence.snapshotInitialized === false ? "Snapshot not initialized" : null,
    evidence.generationCurrent === false ? "Connection generation is not current" : null,
    evidence.crossedBook === true ? "Crossed book" : null,
  ].filter((value): value is string => value !== null)

  return (
    <article className="rounded-xl border border-border bg-card/45 p-5">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
            {evidence.venueId} · {evidence.sourceId}
          </p>
          <h2 className="mt-2 truncate text-lg font-semibold">
            {instrumentLabel ?? evidence.instrumentId}
          </h2>
          {instrumentLabel ? (
            <p className="mt-1 truncate font-mono text-[9px] text-muted-foreground">
              {evidence.instrumentId}
            </p>
          ) : null}
        </div>
        <EvidenceBadge
          label={qualityName(evidence.currentQuality)}
          tone={
            evidence.currentQuality === "direct_verified" &&
            evidence.fresh === true &&
            integrityIssues.length === 0
              ? "good"
              : evidence.fresh === false || integrityIssues.length > 0
                ? "bad"
                : "neutral"
          }
        />
      </div>

      <div className="mt-5 grid grid-cols-2 gap-3">
        <PriceLevel label="Best bid" level={evidence.bestBid} />
        <PriceLevel label="Best ask" level={evidence.bestAsk} />
      </div>

      <dl className="mt-5 grid gap-x-4 gap-y-3 border-t border-border/70 pt-4 sm:grid-cols-2">
        <Fact label="As of" value={dateTime(evidence.asOf)} />
        <Fact label="Freshness" value={truth(evidence.fresh, "Fresh", "Stale")} />
        <Fact label="Valid until" value={dateTime(evidence.sourceValidUntil)} />
        <Fact label="Feed phase" value={name(evidence.phase)} />
        <Fact label="Recorded quality" value={qualityName(evidence.recordedQuality)} />
        <Fact
          label="Book depth"
          value={depth(evidence.bidDepth, evidence.askDepth)}
        />
        <Fact
          label="Last sequence"
          value={evidence.lastSequence?.toLocaleString() ?? "Not reported"}
        />
        <Fact label="Provider product" value={name(evidence.providerProduct)} />
        <Fact label="Channel" value={name(evidence.providerChannel)} />
      </dl>

      {integrityIssues.length > 0 ? (
        <div className="mt-4 flex gap-2 rounded-lg border border-amber-400/20 bg-amber-400/5 p-3 text-xs text-amber-200">
          <CircleAlert className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
          <span>{integrityIssues.join(" · ")}</span>
        </div>
      ) : null}
    </article>
  )
}

type InstrumentQuery = {
  available: boolean
  query: UseQueryResult<ApplicationResult, Error>
}

function InstrumentWorkspace({
  instrumentId,
  trades,
  quotes,
  books,
  comparisons,
}: {
  instrumentId: string
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
      <div className="mb-3">
        <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
          Instrument-scoped reads
        </p>
        <h2 id="instrument-workspace-title" className="mt-1 text-lg font-semibold">
          Trades, quotes, book, and cross-source comparison
        </h2>
      </div>
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

function ResultBoundary({
  snapshot,
  quality,
}: {
  snapshot: ReturnType<typeof resultState>
  quality: ReturnType<typeof resultState>
}) {
  return (
    <p className="mt-4 text-[10px] leading-relaxed text-muted-foreground">
      Snapshot result: {boundary(snapshot)}. Quality result: {boundary(quality)}. Prices and
      quantities remain in instrument ticks and lots because this view does not receive an
      authoritative display scale.
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

function qualityName(value: string | null) {
  return value ? humanize(value) : "Not reported"
}

function name(value: string | null) {
  return value ? humanize(value) : "Not reported"
}

function truth(value: boolean | null, yes: string, no: string) {
  return value === null ? "Not reported" : value ? yes : no
}

function depth(bids: number | string | null, asks: number | string | null) {
  return bids === null || asks === null ? "Not reported" : `${bids} bid / ${asks} ask levels`
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

function lookupInstrumentLabel(
  result: ApplicationResult | undefined,
  instrumentId: string | null,
): string | null {
  if (!result || !instrumentId) return null
  const lookup = lookupResultSchema.safeParse(result.data)
  if (!lookup.success) return null
  const match = lookup.data.matches.find(
    (candidate) =>
      candidate.category === "instrument" && candidate.id === instrumentId,
  )
  if (!match) return null
  const detail = instrumentLookupDetailSchema.safeParse(match.detail)
  return detail.success
    ? detail.data.companyName ?? detail.data.displayName
    : match.label
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
