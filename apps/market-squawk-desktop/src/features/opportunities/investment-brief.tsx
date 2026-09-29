import { CircleAlert, RefreshCw } from "lucide-react"
import { useRef, useState, type ReactNode } from "react"
import { Link } from "react-router-dom"

import { MarketPriceChart, formatChartTimestamp, type ChartViewport, type ObservedPricePoint } from "@/components/charts/market-price-chart"

import { productKeys, type ProductScope } from "@/app/query-client"
import { keepPreviousData, useQuery } from "@tanstack/react-query"
import { DemandPanel } from "../shared/demand-panel"
import { parseInvestmentChart, parseRecommendationTrackRecord } from "./contracts"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { RecommendationStudyPanel } from "@/features/backtests/backtests-page"
import type { ProductTransport } from "@/lib/transport"

import type {
  InvestmentAnalysis,
  InvestmentChart,
  InvestmentAnalysisLocator,
  RecommendationTrackRecord,
  StudyQualification,
} from "./contracts"
import { formatLosslessInteger } from "./format"
import { SavedBenchmarkChart } from "./saved-benchmark-chart"

type Money = NonNullable<InvestmentAnalysis["priceSummary"]["current"]>
type PriceRange = NonNullable<
  InvestmentAnalysis["priceSummary"]["scenarios"]
>["base"]

export function InvestmentBrief({
  analysis,
  transport,
  scope,
  trackRecordAvailable,
  refreshing,
  onRefresh,
}: {
  analysis: InvestmentAnalysis
  transport: ProductTransport
  scope: ProductScope
  trackRecordAvailable: boolean
  refreshing: boolean
  onRefresh: () => void
}) {
  const recommendation = analysis.recommendation
  return (
    <section
      aria-labelledby="investment-brief-title"
      className="rounded-xl border border-border bg-card/45 p-5"
    >
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Saved investment analysis
          </p>
          <h2 id="investment-brief-title" className="mt-1 text-xl font-semibold">
            {investmentTitle(analysis)}
          </h2>
          <p className="mt-1 text-xs text-muted-foreground">{analysis.portfolioLabel}</p>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Review the action, price ranges, supporting reasons, risks, and uncertainty saved with
            this analysis. Research ranges do not place a trade or promise a profit.
          </p>
        </div>
        <div className="flex flex-col items-end gap-3">
          <OutcomeBadge recommendation={recommendation} />
          {recommendation.kind === "action" && recommendation.action !== "hold" ? (
            <Button asChild size="sm">
              <Link to={`/paper-execution?analysis=${encodeURIComponent(analysis.actionToken)}`}>
                Practice this recommendation
              </Link>
            </Button>
          ) : null}
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={onRefresh}
            disabled={refreshing}
          >
            <RefreshCw
              className={refreshing ? "animate-spin" : undefined}
              aria-hidden="true"
            />
            Refresh brief
          </Button>
        </div>
      </div>

      <div className="mt-5 rounded-lg border border-border bg-background/35 p-4">
        <p className="text-sm font-semibold">
          {recommendation.kind === "action"
            ? actionLabel(recommendation.action)
            : recommendation.kind === "abstain"
              ? "Abstain"
              : "Analysis unavailable"}
        </p>
        <p className="mt-1 text-sm leading-6 text-muted-foreground">
          {recommendation.summary}
        </p>
      </div>

      <dl className="mt-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact
          label="Information current through"
          value={formatProductTimestamp(analysis.horizon.informationCurrentThrough)}
        />
        <Fact label="Analysis horizon" value={formatProductTimestamp(analysis.horizon.endsAt)} />
        <Fact label="Brief expires" value={formatProductTimestamp(analysis.horizon.expiresAt)} />
        <Fact
          label="Current price"
          value={nullableMoney(analysis.priceSummary.current)}
        />
        <Fact
          label="Estimated fair value"
          value={nullableMoney(analysis.priceSummary.fairValue)}
        />
        <Fact label="Reporting currency" value={analysis.currency} />
      </dl>

      {analysis.chartAvailable ? <DemandPanel title="Open saved charts" className="mt-5">
        <SavedInvestmentChartRead actionToken={analysis.actionToken} currency={analysis.currency} transport={transport} scope={scope} />
      </DemandPanel> : <p className="mt-5 text-xs text-muted-foreground">Saved chart evidence is unavailable for this analysis.</p>}
      <SavedProbabilities analysis={analysis} />
      <PriceRanges analysis={analysis} />
      <SignalExplanation key={analysis.actionToken} analysis={analysis} />
      <ProductLists analysis={analysis} />
      <EvidenceSummary analysis={analysis} />
      {analysis.evidenceSummary.historicalTest !== null ? (
        <RecommendationStudyPanel
          key={analysis.actionToken}
          actionToken={analysis.actionToken}
          transport={transport}
          scope={scope}
        />
      ) : null}
      <PortfolioAndLiquidity analysis={analysis} />
      <OutcomeProjection analysis={analysis} />
      <Disclosure title="Expected gross price return">
        <p className="text-xs leading-5">{analysis.expectedReturn.state === "available"
          ? analysis.expectedReturn.grossPriceReturnPercent === null
            ? `Exact ratio: ${money(analysis.expectedReturn.exactRatio.numerator)} / ${money(analysis.expectedReturn.exactRatio.denominator)}`
            : formatPercent(analysis.expectedReturn.grossPriceReturnPercent)
          : "Unavailable"}</p>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">{analysis.expectedReturn.summary}</p>
      </Disclosure>
      <SizingSummary analysis={analysis} />
      <RealizedOutcome analysis={analysis} />
      <DemandPanel title="Comparable history" className="mt-5 rounded-lg border border-border bg-background/30 p-4">
        <TrackRecordRead transport={transport} scope={scope} actionToken={analysis.trackRecordActionToken} available={trackRecordAvailable} />
      </DemandPanel>
    </section>
  )
}

const harmonicNames = {
  ab_cd: "AB=CD", gartley: "Gartley", bat: "Bat", butterfly: "Butterfly", crab: "Crab",
  deep_crab: "Deep Crab", cypher: "Cypher", shark: "Shark",
} as const
const harmonicStatus = {
  unavailable: "Pattern evidence unavailable", confirmed: "Confirmed at the saved cutoff",
  insufficient_bars: "Too little price history", insufficient_pivots: "Too few confirmed turning points",
  no_matching_pattern: "No matching pattern", expired: "Expired at the saved cutoff", invalidated: "Invalidated at the saved cutoff",
} as const

type OriginalChartSelection = { layer: "history" | "benchmark"; timeUnixNanos: string; originalOrdinal: string }

function SavedInvestmentChartRead({ actionToken, currency, transport, scope }: {
  actionToken: string
  currency: string
  transport: ProductTransport
  scope: ProductScope
}) {
  const [viewport, setViewport] = useState<ChartViewport | null>(null)
  const [viewReset, setViewReset] = useState(0)
  const [layer, setLayer] = useState<InvestmentChart["viewport"]["layer"]>("all")
  const [selectedPoint, setSelectedPoint] = useState<OriginalChartSelection | null>(null)
  const input = { actionToken, pointLimit: viewport?.pointLimit ?? 512, layer,
    ...(viewport === null ? {} : { startUnixNanos: viewport.fromUnixNanos, endUnixNanos: viewport.throughUnixNanos }) }
  const chart = useQuery({
    queryKey: productKeys.operation(scope, "decision", "Decision.GetInvestmentChart", input),
    gcTime: 0,
    placeholderData: keepPreviousData,
    queryFn: async ({ signal }) => parseInvestmentChart(await transport.query({ query: "decisionInvestmentChart", ...input }, { signal }), input),
  })
  const updateViewport = (next: ChartViewport) => {
    if (viewport === null && next.fromUnixNanos === chart.data?.viewport.fullStartUnixNanos
      && next.throughUnixNanos === chart.data?.viewport.fullEndUnixNanos) return
    setViewport((current) => current?.fromUnixNanos === next.fromUnixNanos
      && current.throughUnixNanos === next.throughUnixNanos && current.pointLimit === next.pointLimit ? current : next)
    setSelectedPoint(null)
  }
  return <div className="space-y-3">
    <label className="flex items-center gap-2 text-xs">Saved chart evidence
      <select className="rounded-md border border-input bg-background px-2 py-1.5" value={layer}
        onChange={(event) => { setLayer(event.target.value as typeof layer); setSelectedPoint(null) }}>
        <option value="all">All layers</option><option value="history">Observed history</option>
        <option value="forecast">Original forecast</option><option value="benchmark">Saved comparisons</option>
        <option value="price_pattern">Price pattern</option><option value="action_ranges">Action references</option>
      </select>
    </label>
    <Button size="sm" variant="outline" onClick={() => { setViewport(null); setSelectedPoint(null); setViewReset((value) => value + 1) }}>Reset saved view</Button>
    {chart.isError ? <Alert variant="destructive">
      <CircleAlert aria-hidden="true" /><AlertTitle>Saved charts could not be loaded</AlertTitle>
      <AlertDescription><Button size="sm" variant="outline" onClick={() => void chart.refetch()}>Try again</Button></AlertDescription>
    </Alert> : null}
    {chart.data ? <>
      {chart.isFetching ? <p role="status" className="text-xs text-muted-foreground">Loading the requested evidence window…</p> : null}
      <SavedInvestmentChart key={viewReset} chart={chart.data} currency={currency} onViewportChange={updateViewport} onObservationSelect={(point) => {
        if (point.originalOrdinal !== undefined) setSelectedPoint({ layer: "history", timeUnixNanos: String(point.timeUnixNanos), originalOrdinal: point.originalOrdinal })
      }} onBenchmarkSelect={(point) => setSelectedPoint({ layer: "benchmark", timeUnixNanos: point.coordinate.sessionCloseUnixNanos, originalOrdinal: point.originalOrdinal })} />
    </> : chart.isPending ? <Skeleton className="h-80 rounded-lg" /> : null}
    {selectedPoint !== null ? <OriginalChartObservationRead key={String(selectedPoint.timeUnixNanos)} point={selectedPoint} actionToken={actionToken} currency={currency} transport={transport} scope={scope} /> : null}
  </div>
}

function OriginalChartObservationRead({ point, actionToken, currency, transport, scope }: {
  point: OriginalChartSelection; actionToken: string; currency: string; transport: ProductTransport; scope: ProductScope
}) {
  const time = String(point.timeUnixNanos)
  const input = { actionToken, startUnixNanos: time, endUnixNanos: time, pointLimit: 8, layer: point.layer }
  const read = useQuery({
    queryKey: productKeys.operation(scope, "decision", "Decision.GetInvestmentChart", input),
    gcTime: 0,
    queryFn: async ({ signal }) => {
      const chart = parseInvestmentChart(await transport.query({ query: "decisionInvestmentChart", ...input }, { signal }), input)
      const original = point.layer === "history"
        ? chart.history.state === "available" ? chart.history.points.find((entry) =>
          (entry.coordinate.kind === "timestamp" ? entry.coordinate.timeUnixNanos : entry.coordinate.sessionCloseUnixNanos) === time
          && entry.originalOrdinal === point.originalOrdinal) : undefined
        : chart.benchmark.state === "available" ? chart.benchmark.points.find((entry) =>
          entry.coordinate.sessionCloseUnixNanos === time && entry.originalOrdinal === point.originalOrdinal) : undefined
      if (!original) throw new Error("The original saved observation could not be verified.")
      return original
    },
  })
  return <div className="rounded-lg border border-border bg-background/25 p-3 text-xs">
    <p className="font-medium">Original saved observation · {formatChartTimestamp(time)}</p>
    {read.isPending ? <p role="status" className="mt-2 text-muted-foreground">Checking the exact original evidence…</p>
      : read.isError ? <p role="alert" className="mt-2 text-destructive">The original observation could not be verified. <Button size="xs" variant="outline" onClick={() => void read.refetch()}>Retry</Button></p>
        : "observations" in read.data ? <dl className="mt-2 grid gap-2 sm:grid-cols-3">{read.data.observations.map((entry, index) => <div key={index}>
          <dt className="text-muted-foreground">Comparison {index + 1}</dt><dd className="mt-1 font-mono">{entry === null ? "Observation gap" : `${entry.priceIndex} index · ${entry.close} ${currency}`}</dd>
        </div>)}</dl>
          : <p className="mt-2 font-mono">{read.data.value === null ? "No recorded price" : `${read.data.value} ${currency}`} · {read.data.quality ?? "Observation gap"}</p>}
  </div>
}

function SavedInvestmentChart({ chart, currency, onViewportChange, onObservationSelect, onBenchmarkSelect }: {
  chart: InvestmentChart; currency: string
  onViewportChange: (viewport: ChartViewport) => void
  onObservationSelect: (point: ObservedPricePoint) => void
  onBenchmarkSelect: (point: Extract<InvestmentChart["benchmark"], { state: "available" }>["points"][number]) => void
}) {
  const pattern = chart.pricePattern
  const patternDetails = useRef<HTMLDetailsElement>(null)
  const patternSummary = useRef<HTMLElement>(null)
  const [selectedPivot, setSelectedPivot] = useState<string | null>(null)
  const confirmed = pattern.status === "confirmed"
  const patternLabel = pattern.kind ? `${harmonicNames[pattern.kind]} pattern` : "Price pattern"
  const patternWindow = pattern.confirmationCutoffUnixNanos !== null && pattern.expiresAtUnixNanos !== null
    ? { fromUnixNanos: pattern.confirmationCutoffUnixNanos, throughExclusiveUnixNanos: pattern.expiresAtUnixNanos } : {}
  const actionRanges = chart.actionRanges
  const forecastOrigin = chart.forecast.origin
  return <section className="mt-5 space-y-4" aria-label="Saved investment charts">
    <div>
      <h3 className="text-base font-semibold">History, forecasts and price patterns</h3>
      <p className="mt-2 text-xs leading-5 text-muted-foreground">{chart.basisExplanation}</p>
    </div>
    {chart.history.state === "available" || chart.forecast.state === "available" || confirmed ? <MarketPriceChart
      title="Saved split-adjusted history, forecast and price patterns"
      viewportBounds={chart.viewport.fullStartUnixNanos !== null && chart.viewport.fullEndUnixNanos !== null
        ? { fromUnixNanos: chart.viewport.fullStartUnixNanos, throughUnixNanos: chart.viewport.fullEndUnixNanos } : undefined}
      viewportPointLimit={chart.viewport.pointLimit}
      onViewportChange={onViewportChange}
      onObservationSelect={onObservationSelect}
      displayResolution={chart.history.state === "available" ? chart.history.display : undefined}
      unit={currency}
      cutoffUnixNanos={chart.forecast.state === "available"
        ? chart.forecast.observedThroughUnixNanos : chart.informationCurrentThroughUnixNanos}
      cutoffLabel={chart.forecast.state === "available" ? "Original forecast cutoff" : "Saved information cutoff"}
      observed={chart.history.state === "available" ? chart.history.points.map((point) => ({
        timeUnixNanos: point.coordinate.kind === "timestamp"
          ? point.coordinate.timeUnixNanos : point.coordinate.sessionCloseUnixNanos,
        ...(point.coordinate.kind === "session_date" ? { sessionDate: point.coordinate.date } : {}),
        value: point.value, quality: point.quality, originalOrdinal: point.originalOrdinal, breakBefore: point.breakBefore[0],
      })) : forecastOrigin.state === "available" ? [{
        timeUnixNanos: forecastOrigin.coordinate.kind === "timestamp"
          ? forecastOrigin.coordinate.timeUnixNanos : forecastOrigin.coordinate.sessionCloseUnixNanos,
        ...(forecastOrigin.coordinate.kind === "session_date" ? { sessionDate: forecastOrigin.coordinate.date } : {}),
        value: forecastOrigin.value, quality: forecastOrigin.quality,
      }] : []}
      forecast={chart.forecast.points.map((point) => ({
        timeUnixNanos: point.timeUnixNanos, central: point.central,
        ...(point.interval50 ? { interval50: [point.interval50.lower, point.interval50.upper] as const } : {}),
        ...(point.interval80 ? { interval80: [point.interval80.lower, point.interval80.upper] as const } : {}),
        ...(point.interval95 ? { interval95: [point.interval95.lower, point.interval95.upper] as const } : {}),
      }))}
      pattern={confirmed ? {
        label: patternLabel,
        points: pattern.pivots.map((pivot) => ({ name: pivot.name, timeUnixNanos: pivot.observedAtUnixNanos, value: pivot.value,
          availableAtUnixNanos: pivot.availableAtUnixNanos, confirmedAtUnixNanos: pivot.confirmedAtUnixNanos })),
      } : undefined}
      onPatternSelect={confirmed ? (pivot) => {
        setSelectedPivot(pivot.name)
        if (patternDetails.current) patternDetails.current.open = true
        patternSummary.current?.focus({ preventScroll: true })
        patternSummary.current?.scrollIntoView({ block: "nearest" })
      } : undefined}
      ranges={[
        ...(confirmed && pattern.reversalZone ? [{
        id: "reversal-zone", label: "Pattern completion / reversal zone",
        lower: pattern.reversalZone.lower, upper: pattern.reversalZone.upper, ...patternWindow,
        }] : []),
        ...(actionRanges.state === "available" ? actionRanges.ranges.map((range) => ({
          id: `action-${range.kind}`, label: range.label, lower: range.lower, upper: range.upper,
          fromUnixNanos: range.startAtUnixNanos, throughExclusiveUnixNanos: range.endAtUnixNanos,
        })) : []),
      ]}
      targets={confirmed ? [
        ...(pattern.invalidation === null ? [] : [{ id: "pattern-invalidation", label: "Pattern invalidation level", value: pattern.invalidation, status: harmonicStatus[pattern.status], ...patternWindow }]),
        ...pattern.targets.map((value, index) => ({ id: `pattern-target-${index}`, label: `Pattern target ${index + 1}`, value, status: "Research level; not a prediction", ...patternWindow })),
      ] : []}
    /> : null}
    <p className="text-xs leading-5 text-muted-foreground">{chart.history.summary}</p>
    <p className="text-xs leading-5 text-muted-foreground">Original observed price: {forecastOrigin.summary}</p>
    <p className="text-xs leading-5 text-muted-foreground">{chart.forecast.summary} A single forecast point is shown as a point, without an invented path from today's price.</p>
    <p className="text-xs leading-5 text-muted-foreground">{actionRanges.summary}</p>
    {actionRanges.state === "available" ? <details className="rounded-lg border border-border bg-background/25 p-4">
      <summary className="cursor-pointer text-sm font-medium">Saved action reference evidence</summary>
      <dl className="mt-4 grid gap-3 sm:grid-cols-3">
        <Fact label="Original information cutoff" value={formatChartTimestamp(actionRanges.informationCurrentThroughUnixNanos)} />
        <Fact label="Originally admitted" value={formatChartTimestamp(actionRanges.admittedAtUnixNanos)} />
        <Fact label="Reference expiry (exclusive)" value={formatChartTimestamp(actionRanges.expiresAtUnixNanos)} />
      </dl>
      <ul className="mt-4 space-y-3 text-xs leading-5">
        {actionRanges.ranges.map((range) => <li key={range.kind}>
          <p className="font-medium">{range.label}: <span className="font-mono">{range.lower} – {range.upper} {currency}</span></p>
          <p className="mt-1 text-muted-foreground">{range.summary}</p>
        </li>)}
      </ul>
    </details> : null}
    <details ref={patternDetails} className="rounded-lg border border-border bg-background/25 p-4">
      <summary ref={patternSummary} className="cursor-pointer text-sm font-medium">{patternLabel} · {harmonicStatus[pattern.status]}</summary>
      {selectedPivot ? <p className="mt-3 text-xs font-medium" role="status">Selected {selectedPivot} pivot. Its original observation, availability and confirmation are highlighted below.</p> : null}
      <p className="mt-3 text-xs leading-5 text-muted-foreground">{pattern.summary}</p>
      <dl className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Direction" value={pattern.direction === null ? "Unavailable" : pattern.direction === "bullish" ? "Bullish" : "Bearish"} />
        <Fact label="Original observation cutoff" value={formatChartTimestamp(pattern.observationCutoffUnixNanos)} />
        <Fact label="Originally confirmed through" value={formatChartTimestamp(pattern.confirmationCutoffUnixNanos)} />
        <Fact label="Pattern expiry" value={formatChartTimestamp(pattern.expiresAtUnixNanos)} />
        <Fact label="Completion / reversal zone" value={pattern.reversalZone
          ? `${pattern.reversalZone.lower} – ${pattern.reversalZone.upper} ${currency}` : "Unavailable"} />
        <Fact label="Invalidation level" value={pattern.invalidation === null
          ? "Unavailable" : `${pattern.invalidation} ${currency}`} />
        {pattern.targets.map((value, index) => <Fact key={index} label={`Research target ${index + 1}`}
          value={`${value} ${currency}`} />)}
      </dl>
      {pattern.pivots.length ? <div className="mt-4 overflow-x-auto">
        <table className="w-full min-w-[760px] text-left text-xs">
          <caption className="mb-2 text-left text-muted-foreground">Pivot positions use their original observation times. Availability and confirmation are separate; the pattern was not known at the earlier pivot time.</caption>
          <thead><tr><th className="p-2 font-medium">Pivot</th><th className="p-2 font-medium">Price</th><th className="p-2 font-medium">Observed</th><th className="p-2 font-medium">Available</th><th className="p-2 font-medium">Confirmed</th></tr></thead>
          <tbody>{pattern.pivots.map((pivot) => <tr key={pivot.name} aria-current={pivot.name === selectedPivot ? "true" : undefined}
            className={`border-t border-border ${pivot.name === selectedPivot ? "bg-primary/10" : ""}`}>
            <td className="p-2">{pivot.name} · {pivot.kind}</td><td className="p-2 font-mono">{pivot.value} {currency}</td>
            <td className="p-2">{formatChartTimestamp(pivot.observedAtUnixNanos)}</td><td className="p-2">{formatChartTimestamp(pivot.availableAtUnixNanos)}</td>
            <td className="p-2">{formatChartTimestamp(pivot.confirmedAtUnixNanos)}</td>
          </tr>)}</tbody>
        </table>
      </div> : null}
      {pattern.ratios.length ? <dl className="mt-4 grid gap-3 sm:grid-cols-3">
        {pattern.ratios.map((ratio) => <Fact key={ratio.name} label={ratio.name} value={`${ratio.numerator} / ${ratio.denominator}`} />)}
      </dl> : null}
      <ul className="mt-4 list-disc space-y-2 pl-4 text-xs leading-5 text-muted-foreground">
        {pattern.interpretation.map((text) => <li key={text}>{text}</li>)}
      </ul>
    </details>
    <SavedBenchmarkChart benchmark={chart.benchmark} currency={currency} onViewportChange={onViewportChange} onObservationSelect={onBenchmarkSelect} />
  </section>
}
const signalChoices = [
  ["currentMarket", "Current market"], ["broaderResearch", "Broader research"],
  ["pricePattern", "Harmonic price patterns"], ["forecast", "Forecast"],
  ["financialModel", "Financial model"], ["valuation", "Valuation"],
  ["historicalTest", "Historical test"], ["outOfSample", "Held-out evidence"],
  ["liquidity", "Trading conditions"], ["portfolioRisk", "Portfolio risk"],
] as const

function SignalExplanation({ analysis }: { analysis: InvestmentAnalysis }) {
  const [selected, setSelected] = useState<typeof signalChoices[number][0]>("forecast")
  const evidence = analysis.analyticalEvidence[selected]
  return <Disclosure title="Explore the signals">
    <label className="grid gap-2 text-xs">Choose an evidence layer
      <select className="rounded-md border border-input bg-background px-3 py-2" value={selected}
        onChange={(event) => {
          const choice = signalChoices.find(([key]) => key === event.target.value)
          if (choice) setSelected(choice[0])
        }}>
        {signalChoices.map(([key, label]) => <option key={key} value={key}>{label}</option>)}
      </select>
    </label>
    <div className="mt-3 text-xs leading-5" aria-live="polite">
      <p className="mb-2 font-medium">{evidence.state === "available" ? "Evidence available" : "Evidence unavailable"}</p>
      {selected === "pricePattern" ? <PricePatternDetails evidence={analysis.analyticalEvidence.pricePattern} />
        : <p className="text-muted-foreground">{evidence.summary}</p>}
    </div>
  </Disclosure>
}

function SavedProbabilities({ analysis }: { analysis: InvestmentAnalysis }) {
  return <Disclosure title="Three different chances over the saved horizon">
    <p className="text-xs leading-5 text-muted-foreground">
      Each chance answers a different question. These are event probabilities, not expected returns or guarantees.
    </p>
    <div className="mt-4 grid gap-4 xl:grid-cols-3">
      {([
        ["priceHigher", "Price finishes higher"],
        ["benchmarkOutperformance", "Beats the selected benchmark"],
        ["profitAfterCosts", "Profit after modeled costs"],
      ] as const).map(([key, label]) => {
        const event = analysis.probabilities[key]
        return <section key={key} className="rounded-lg border border-border p-4" aria-label={label}>
          <h4 className="text-sm font-medium">{label}</h4>
          <p className="mt-2 font-mono text-lg">{event.state === "available" ? formatPercent(event.probabilityPercent) : "Unavailable"}</p>
          {event.state === "unavailable" ? <p className="mt-2 text-xs leading-5 text-muted-foreground">{event.summary}</p> : <>
            <dl className="mt-3 space-y-3">
              <Fact label="Starting observation" value={formatProductTimestamp(event.observedAt)} />
              <Fact label="Forecast ends" value={formatProductTimestamp(event.endsAt)} />
              <Fact label="Original forecast expires" value={formatProductTimestamp(event.expiresAt)} />
            </dl>
            <details className="mt-4 text-xs leading-5">
              <summary className="cursor-pointer font-medium">Held-out calibration evidence</summary>
              <p className="mt-2 text-muted-foreground">Evaluated on observations kept separate from training and calibration. Lower error scores indicate better predictions on those observations; they are not confidence percentages.</p>
              <dl className="mt-3 space-y-3">
                <Fact label="Completed outcomes" value={event.calibration.completedOutcomes.toLocaleString("en-US")} />
                <Fact label="Evaluated from" value={formatProductTimestamp(event.calibration.evaluatedFrom)} />
                <Fact label="Evaluated through" value={formatProductTimestamp(event.calibration.evaluatedThrough)} />
                <Fact label="Brier score" value={event.calibration.brierScore.toString()} />
                <Fact label="Log loss" value={event.calibration.logLoss.toString()} />
              </dl>
            </details>
          </>}
          {event.benchmark ? <details className="mt-4 text-xs leading-5">
            <summary className="cursor-pointer font-medium">Saved benchmark identity</summary>
            <p className="mt-2 text-muted-foreground">This forecast retains the selected benchmark's original definition. Its display name was not saved.</p>
            <dl className="mt-3 space-y-3 break-all">
              <Fact label="Instrument" value={event.benchmark.instrumentId} />
              <Fact label="Definition fingerprint" value={`${event.benchmark.definitionAlgorithm}: ${event.benchmark.definitionDigest}`} />
            </dl>
          </details> : null}
          {event.assumptions.length ? <ul className="mt-4 list-disc space-y-2 pl-4 text-xs leading-5 text-muted-foreground">
            {event.assumptions.map((text) => <li key={text}>{text}</li>)}
          </ul> : null}
        </section>
      })}
    </div>
  </Disclosure>
}

function PriceRanges({ analysis }: { analysis: InvestmentAnalysis }) {
  const scenarios = analysis.priceSummary.scenarios
  const actionRanges = analysis.priceSummary.actionRanges
  if (!scenarios && !actionRanges) return null
  return (
    <Disclosure title="Price ranges">
      <p className="text-xs leading-5 text-muted-foreground">
        These are saved research ranges, not guaranteed prices.
      </p>
      {scenarios ? (
        <dl className="mt-4 grid gap-4 sm:grid-cols-3">
          <Fact label="Downside scenario" value={priceRange(scenarios.downside)} />
          <Fact label="Base scenario" value={priceRange(scenarios.base)} />
          <Fact label="Upside scenario" value={priceRange(scenarios.upside)} />
        </dl>
      ) : null}
      {actionRanges ? (
        <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <Fact label="Entry range" value={priceRange(actionRanges.entry)} />
          <Fact label="Add range" value={priceRange(actionRanges.add)} />
          <Fact label="Trim range" value={priceRange(actionRanges.trim)} />
          <Fact label="Exit range" value={priceRange(actionRanges.exit)} />
        </dl>
      ) : null}
    </Disclosure>
  )
}

function ProductLists({ analysis }: { analysis: InvestmentAnalysis }) {
  return (
    <div className="grid gap-4 xl:grid-cols-2">
      <TextList title="Why" values={analysis.reasons} empty="No additional reason was saved." />
      <TextList title="Risks" values={analysis.risks} empty="No additional risk was saved." />
      <TextList
        title="Assumptions"
        values={analysis.assumptions}
        empty="No additional assumption was saved."
      />
      <TextList
        title="What would invalidate it"
        values={analysis.invalidators}
        empty="No additional invalidator was saved."
      />
    </div>
  )
}

function EvidenceSummary({ analysis }: { analysis: InvestmentAnalysis }) {
  const evidence = analysis.evidenceSummary
  const historical = evidence.historicalTest
  const uncertainty = evidence.uncertainty
  return (
    <Disclosure title="Evidence and uncertainty">
      <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Evidence coverage" value={evidence.coverage.summary} />
        <Fact label="Forecast calibration" value={evidence.calibration.summary} />
        <Fact label="Out-of-sample evidence" value={<>
          <p>{evidence.outOfSample.summary}</p>
          {evidence.outOfSample.state === "available" ? <StudyQualificationDetails
            qualification={evidence.outOfSample.studyQualification} /> : null}
        </>} />
        <Fact label="Cost treatment" value={evidence.costs.summary} />
        <Fact label="Uncertainty" value={<EvidenceReliabilityDetails uncertainty={uncertainty} />} />
      </dl>
      <p className="mt-4 text-xs leading-5 text-muted-foreground">{analysis.analyticalEvidence.combination.summary}</p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-2">
        {([
          ["Broader research", "broaderResearch"],
          ["Financial model", "financialModel"],
          ["Governed valuation", "valuation"],
        ] as const).map(([label, family]) => <Fact key={family} label={label}
          value={analysis.analyticalEvidence[family].summary} />)}
        <Fact label="Price patterns" value={<PricePatternDetails evidence={analysis.analyticalEvidence.pricePattern} />} />
      </dl>
      {evidence.calibration.state === "available" ? (
        <dl className="mt-4 grid gap-4 sm:grid-cols-3">
          <Fact
            label="Target range coverage"
            value={formatPercent(evidence.calibration.nominalCoveragePercent)}
          />
          <Fact
            label="Realized range coverage"
            value={formatPercent(evidence.calibration.realizedCoveragePercent)}
          />
          <Fact
            label="Completed outcomes"
            value={evidence.calibration.completedOutcomes.toLocaleString("en-US")}
          />
        </dl>
      ) : null}
      {evidence.outOfSample.state === "available" ? <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Completed held-out observations" value={evidence.outOfSample.completedObservations.toLocaleString("en-US")} />
        <Fact label="All held-out signals" value={evidence.outOfSample.totalSignals.toLocaleString("en-US")} />
        <Fact label="Historical evaluation windows" value={evidence.outOfSample.folds.toLocaleString("en-US")} />
        <Fact label="Held-out completion coverage" value={formatPercent(evidence.outOfSample.completionCoveragePercent)} />
        <Fact label="Evaluated from" value={formatProductTimestamp(evidence.outOfSample.evaluatedFrom)} />
        <Fact label="Evaluated through" value={formatProductTimestamp(evidence.outOfSample.evaluatedThrough)} />
      </dl> : null}
      {historical ? (
        <section className="mt-4 border-t border-border pt-4" aria-label="Historical test">
        <p className="text-xs leading-5 text-muted-foreground">{historical.summary}</p>
        <StudyQualificationDetails qualification={historical.studyQualification} />
        <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          <Fact
            label="Historical net return"
            value={formatPercent(historical.netReturnPercent)}
          />
          <Fact
            label="Historical maximum drawdown"
            value={negativePercent(historical.maximumDrawdownPercent)}
          />
          <Fact
            label="Historical observations"
            value={historical.observations.toLocaleString("en-US")}
          />
          <Fact label="Historical trials" value={historical.trials.toLocaleString("en-US")} />
          <Fact label="Stability" value={formatPercent(historical.stabilityPercent)} />
          <Fact
            label="Historical information through"
            value={formatProductTimestamp(historical.evaluatedThrough)}
          />
        </dl>
        </section>
      ) : null}
      {evidence.costs.state === "modeled" ? (
        <dl className="mt-4 grid gap-4 sm:grid-cols-3">
          <Fact
            label="Modeled fee per fill"
            value={formatPercent(evidence.costs.feePercent)}
          />
          <Fact
            label="Modeled slippage"
            value={formatPercent(evidence.costs.slippagePercent)}
          />
          <Fact
            label="Maximum random slippage"
            value={formatPercent(evidence.costs.maximumRandomSlippagePercent)}
          />
        </dl>
      ) : null}
    </Disclosure>
  )
}

type EvidenceUncertainty = InvestmentAnalysis["evidenceSummary"]["uncertainty"]
type ReliabilityComponent = Extract<EvidenceUncertainty, { components: unknown }>["components"][number]
const reliabilityComponentLabels: Record<ReliabilityComponent["kind"], string> = {
  forecast_calibration: "Forecast calibration",
  valuation_agreement: "Valuation agreement",
  backtest_stability: "Historical-test stability",
  market_integrity: "Market integrity",
  liquidity_capacity: "Trading capacity",
  portfolio_risk_capacity: "Portfolio risk capacity",
}

export function EvidenceReliabilityDetails({ uncertainty }: { uncertainty: EvidenceUncertainty }) {
  return <div className="space-y-2">
    <p>{uncertainty.summary}</p>
    <p>Evidence reliability: {uncertainty.state === "available"
      ? formatPercent(uncertainty.evidenceReliabilityPercent) : "Unavailable"}</p>
    {"components" in uncertainty ? <>
      <StudyQualificationDetails qualification={uncertainty.studyQualification} />
      <details className="rounded-md border border-border p-3">
        <summary className="cursor-pointer font-medium">Reliability components</summary>
        <p className="mt-2 text-muted-foreground">Configured weight included in this score: {formatPolicyWeight(uncertainty.applicablePolicyWeightPpm)}.</p>
        <dl className="mt-3 space-y-3">
          {uncertainty.components.map((component) => <div key={component.kind}>
            <dt className="font-medium">{reliabilityComponentLabels[component.kind]}</dt>
            <dd className="mt-1 text-muted-foreground">
              <p>{component.state === "available" ? formatPercent(component.reliabilityPercent)
                : component.state === "not_applicable" ? "Not needed for Hold; excluded from this score."
                  : liquidityReliabilityReason(component.reason)}</p>
              <p>Configured weight: {formatPolicyWeight(component.configuredWeightPpm)}.</p>
            </dd>
          </div>)}
        </dl>
      </details>
    </> : null}
  </div>
}

function liquidityReliabilityReason(reason: Extract<ReliabilityComponent, { state: "unavailable" }>["reason"]): string {
  switch (reason) {
    case "buy_add_capacity_unavailable": return "Buy/Add capacity is unavailable."
    case "trim_sell_capacity_unavailable": return "Trim/Sell capacity is unavailable."
    case "action_side_not_established": return "The evidence did not establish an action direction."
  }
}

/** Exact display-unit conversion; the backend supplies all weights and the aggregate score. */
function formatPolicyWeight(value: number): string {
  const digits = value.toString().padStart(5, "0")
  const whole = digits.slice(0, -4)
  const fraction = digits.slice(-4).replace(/0+$/, "")
  return `${whole}${fraction ? `.${fraction}` : ""}%`
}

type PricePatternEvidence = InvestmentAnalysis["analyticalEvidence"]["pricePattern"]
const pricePatternLabels: Record<PricePatternEvidence["outcome"], string> = {
  pattern_detected: "Pattern detected",
  no_matching_pattern: "No matching pattern",
  insufficient_bars: "Too little complete price history",
  insufficient_turning_points: "Too few confirmed turning points",
  history_unavailable: "Price history unavailable",
  adjustment_unavailable: "Adjusted price history unavailable",
  trading_activity_unavailable: "Trading activity unavailable",
  price_precision_unavailable: "Price precision unavailable",
  pattern_expired: "Pattern expired",
  pattern_invalidated: "Pattern invalidated",
  assessment_unavailable: "Pattern assessment unavailable",
  not_evaluated: "Patterns not evaluated",
}

export function PricePatternDetails({ evidence }: { evidence: PricePatternEvidence }) {
  return <div className="space-y-1">
    <p className="font-medium">{pricePatternLabels[evidence.outcome]}</p>
    <p className="text-muted-foreground">{evidence.summary}</p>
  </div>
}

export function StudyQualificationDetails({ qualification }: { qualification: StudyQualification }) {
  return <div className="mt-3 space-y-2 text-xs leading-5">
    <p className="font-medium">{qualification.basis === "historical_as_known"
      ? "Information known at the time" : "Historical simulation using later data"}</p>
    <p className="text-muted-foreground">{qualification.summary}</p>
    {qualification.limitations.length ? <ul className="list-disc space-y-1 pl-4 text-muted-foreground">
      {qualification.limitations.map((limitation) => <li key={limitation}>{limitation}</li>)}
    </ul> : null}
  </div>
}

function PortfolioAndLiquidity({ analysis }: { analysis: InvestmentAnalysis }) {
  return <Disclosure title="Portfolio fit and trading conditions">
    <p className="text-xs leading-5 text-muted-foreground">{analysis.portfolioContext.summary}</p>
    {analysis.portfolioContext.state === "available" ? <dl className="mt-4 grid gap-4 sm:grid-cols-2">
      <Fact label="Saved position" value={analysis.portfolioContext.positionState === "current_position" ? "Position held" : "No position held"} />
      <Fact label="Remaining risk capacity" value={formatPercent(analysis.portfolioContext.riskCapacityPercent)} />
    </dl> : null}
    <p className="mt-4 text-xs leading-5 text-muted-foreground">{analysis.liquidity.summary}</p>
    {analysis.liquidity.state === "available" ? <dl className="mt-4 grid gap-4 sm:grid-cols-2">
      <Fact label="Quoted spread" value={formatPercent(analysis.liquidity.quotedSpreadPercent)} />
      <Fact label="Buy/Add capacity under saved limits" value={analysis.liquidity.buyAddCapacityPercent === null
        ? "Unavailable" : formatPercent(analysis.liquidity.buyAddCapacityPercent)} />
      <Fact label="Trim/Sell capacity under saved limits" value={analysis.liquidity.trimSellCapacityPercent === null
        ? "Unavailable" : formatPercent(analysis.liquidity.trimSellCapacityPercent)} />
    </dl> : null}
    <p className="mt-4 text-xs leading-5 text-muted-foreground">{analysis.virtualPaperEligibility.summary}</p>
  </Disclosure>
}

function OutcomeProjection({ analysis }: { analysis: InvestmentAnalysis }) {
  const projection = analysis.outcomeProjection
  if (!projection) return null
  return (
    <Disclosure title="Projected gross price change">
      {projection.positionScale ? <p className="mb-4 text-xs leading-5 text-muted-foreground">
        {formatLosslessInteger(projection.positionScale.quantityLots)} lots. {projection.positionScale.summary}
      </p> : null}
      <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Starting price" value={money(projection.startingPrice)} />
        <Fact label="Projection horizon" value={formatProductTimestamp(projection.endsAt)} />
        <Fact label="Downside range" value={priceRange(projection.downside.priceRange)} />
        <Fact label="Base range" value={priceRange(projection.base.priceRange)} />
        <Fact label="Upside range" value={priceRange(projection.upside.priceRange)} />
        {projection.downside.priceChangePercent ? (
          <Fact
            label="Downside price change"
            value={percentRange(projection.downside.priceChangePercent)}
          />
        ) : null}
        {projection.base.priceChangePercent ? (
          <Fact
            label="Base price change"
            value={percentRange(projection.base.priceChangePercent)}
          />
        ) : null}
        {projection.upside.priceChangePercent ? (
          <Fact
            label="Upside price change"
            value={percentRange(projection.upside.priceChangePercent)}
          />
        ) : null}
      </dl>
      <dl className="mt-4 grid gap-4 sm:grid-cols-3">
        {(["downside", "base", "upside"] as const).map((scenario) => {
          const pnl = projection[scenario].grossPricePnl
          const change = projection[scenario].absolutePriceChange
          return <div key={scenario} className="space-y-4"><Fact
            label={`${scenario.charAt(0).toUpperCase() + scenario.slice(1)} price change`}
            value={`${money(change.lower)} – ${money(change.upper)}`} /><Fact
            label={`${scenario.charAt(0).toUpperCase() + scenario.slice(1)} gross profit or loss`}
            value={<>{pnl.state === "available" ? <p>{money(pnl.range.lower)} – {money(pnl.range.upper)}</p> : null}
              <p>{pnl.summary}</p></>} /></div>
        })}
      </dl>
      <dl className="mt-4 grid gap-4 sm:grid-cols-2">
        <Fact label="Expected gross profit or loss" value={<>
          {projection.expectedGrossPricePnl.state === "available" ? <p>{money(projection.expectedGrossPricePnl.amount)}</p> : null}
          <p>{projection.expectedGrossPricePnl.summary}</p>
        </>} />
        <Fact label="Net profit or loss" value={projection.netPnl.summary} />
        <Fact label="Comparison with the market" value={projection.benchmarkReturn.summary} />
        <Fact label="After-tax profit or loss" value={projection.afterTaxPnl.summary} />
      </dl>
      <TextList title="Projection limitations" values={projection.limitations} empty="" />
    </Disclosure>
  )
}

const sizingLabels = {
  cash_reserve: "Cash reserve", downside_loss: "Downside loss", liquidity: "Trading capacity",
  portfolio_risk: "Portfolio risk", forward_cost: "Trading costs", preferred_weight: "Preferred allocation",
} as const
function notionalRange(value: Extract<InvestmentAnalysis["sizing"], { state: "evaluated" }>["hardFeasibleTargetNotional"]): string {
  return value.kind === "available" ? `${money(value.lower)} – ${money(value.upper)}` : value.reasons.join(" ")
}

function SizingSummary({ analysis }: { analysis: InvestmentAnalysis }) {
  const sizing = analysis.sizing
  if (sizing.state === "unavailable") return <Disclosure title="Research sizing range">
    <p className="text-xs leading-5 text-muted-foreground">{sizing.summary}</p>
  </Disclosure>
  return (
    <Disclosure title="Research sizing range">
      <p className="text-xs leading-5 text-muted-foreground">{sizing.summary}</p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-3">
        <Fact label="Evaluated" value={formatProductTimestamp(sizing.evaluatedAt)} />
        <Fact label="Marked portfolio value" value={money(sizing.markedEquity)} />
        <Fact label="Cash available for settlement" value={sizing.settlementAvailableCash === null ? "Unavailable" : money(sizing.settlementAvailableCash)} />
        <Fact label="Value per lot" value={money(sizing.perLotNotional)} />
        <Fact label="Downside loss per lot" value={money(sizing.perLotDownsideLoss)} />
        <Fact label="Current lots" value={formatLosslessInteger(sizing.currentLots)} />
        <Fact label="Mandatory range" value={lotRange(sizing.hardFeasibleLots)} />
        <Fact label="Preferred range" value={lotRange(sizing.preferredFeasibleLots)} />
        <Fact label="Mandatory position value" value={notionalRange(sizing.hardFeasibleTargetNotional)} />
        <Fact label="Preferred position value" value={notionalRange(sizing.preferredFeasibleTargetNotional)} />
        <Fact label="Mandatory binding limits" value={sizing.hardBindingCaps.map((kind) => sizingLabels[kind]).join(", ") || "None"} />
        <Fact label="Preferred binding limits" value={sizing.preferredBindingCaps.map((kind) => sizingLabels[kind]).join(", ") || "None"} />
        <Fact label="Lower weight rounding excess" value={money(sizing.preferredWeightRounding.lowerRoundUpExcess)} />
        <Fact label="Upper weight rounding remainder" value={money(sizing.preferredWeightRounding.upperRoundDownRemainder)} />
        {sizing.constraintCaps.map((cap) => <Fact key={cap.kind} label={sizingLabels[cap.kind]}
          value={cap.state === "available" ? `${formatLosslessInteger(cap.lower)}–${formatLosslessInteger(cap.upper)} lots` : cap.summary} />)}
      </dl>
    </Disclosure>
  )
}

function RealizedOutcome({ analysis }: { analysis: InvestmentAnalysis }) {
  const outcome = analysis.realizedOutcome
  if (!outcome) return null
  const result = outcome.result
  return (
    <Disclosure title="Realized outcome">
      {result.kind === "completed" ? (
        <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          <Fact label="Starting price" value={money(result.startMark)} />
          <Fact label="Ending price" value={money(result.endpointPrice)} />
          <Fact
            label="Gross price return"
            value={formatPercent(result.grossPriceReturnPercent)}
          />
          <Fact label="Observed" value={formatProductTimestamp(result.observedAt)} />
          <Fact label="Available" value={formatProductTimestamp(result.availableAt)} />
        </dl>
      ) : (
        <p className="text-xs leading-5 text-muted-foreground">{result.summary}</p>
      )}
      {result.kind === "completed" ? (
        <TextList title="Outcome limitations" values={result.limitations} empty="" />
      ) : null}
    </Disclosure>
  )
}

function TrackRecordRead({ transport, scope, actionToken, available }: {
  transport: ProductTransport
  scope: ProductScope
  actionToken: string | null
  available: boolean
}) {
  const record = useQuery({
    queryKey: productKeys.operation(scope, "decision", "Decision.GetRecommendationTrackRecord", { actionToken }),
    enabled: available && actionToken !== null,
    gcTime: 0,
    queryFn: async ({ signal }) => parseRecommendationTrackRecord(await transport.query({ query: "decisionRecommendationTrackRecord", actionToken: actionToken! }, { signal }), actionToken!),
  })
  return <TrackRecord record={record.data ?? null} pending={available && actionToken !== null && record.isPending} unavailable={!available || actionToken === null || record.isError} />
}

function TrackRecord({
  record,
  pending,
  unavailable,
}: {
  record: RecommendationTrackRecord | null
  pending: boolean
  unavailable: boolean
}) {
  if (pending) {
    return <Skeleton className="mt-5 h-32 w-full" aria-label="Loading comparable history" />
  }
  if (unavailable || record === null) {
    return (
      <div>
        <p className="text-xs leading-5 text-muted-foreground">
          Comparable saved outcomes are unavailable right now.
        </p>
      </div>
    )
  }
  const represented = record.groups.filter((group) => group.recommendationCount > 0)
  return (
    <div>
      <p className="text-xs leading-5 text-muted-foreground">{record.summary}</p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Fact label="Evaluated through" value={formatProductTimestamp(record.evaluatedAt)} />
        <Fact
          label="Minimum completed outcomes"
          value={record.minimumCompletedSamples.toLocaleString("en-US")}
        />
        <Fact
          label="Minimum outcome coverage"
          value={formatPercent(record.minimumCoveragePercent)}
        />
        <Fact
          label="Analyses without enough evidence"
          value={record.unavailableAnalysisCount.toLocaleString("en-US")}
        />
      </dl>
      {represented.length ? (
        <div className="mt-4 grid gap-3 lg:grid-cols-2">
          {represented.map((group) => (
            <div key={group.action} className="rounded-lg border border-border p-3">
              <p className="text-xs font-semibold">{trackRecordLabel(group.action)}</p>
              <p className="mt-1 text-xs text-muted-foreground">
                {group.completedCount.toLocaleString("en-US")} completed · {formatPercent(
                  group.coveragePercent,
                )} coverage
              </p>
              <p className="mt-2 text-xs leading-5 text-muted-foreground">
                {group.performance.kind === "available"
                  ? `Mean gross price return: ${formatPercent(group.performance.meanGrossPriceReturnPercent)}.`
                  : group.performance.summary}
              </p>
            </div>
          ))}
        </div>
      ) : (
        <p className="mt-3 text-xs text-muted-foreground">
          No comparable saved outcomes are available yet.
        </p>
      )}
    </div>
  )
}

function TextList({
  title,
  values,
  empty,
}: {
  title: string
  values: string[]
  empty: string
}) {
  return (
    <div className="mt-5 rounded-lg border border-border bg-background/30 p-4">
      <h3 className="text-xs font-semibold">{title}</h3>
      {values.length ? (
        <ul className="mt-3 space-y-2 text-xs leading-5 text-muted-foreground">
          {values.map((value) => (
            <li key={value}>• {value}</li>
          ))}
        </ul>
      ) : empty ? (
        <p className="mt-2 text-xs text-muted-foreground">{empty}</p>
      ) : null}
    </div>
  )
}

function Disclosure({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="mt-5 rounded-lg border border-border bg-background/25 p-4">
      <h3 className="text-sm font-semibold">{title}</h3>
      <div className="mt-3">{children}</div>
    </section>
  )
}

function Fact({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 text-xs leading-5">{value}</dd>
    </div>
  )
}

function OutcomeBadge({
  recommendation,
}: {
  recommendation: InvestmentAnalysis["recommendation"]
}) {
  const tone = analysisOutcomeTone(recommendation)
  const className =
    tone === "good"
      ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-200"
      : tone === "attention"
        ? "border-amber-400/30 bg-amber-400/10 text-amber-100"
        : "border-border bg-muted/40 text-muted-foreground"
  return (
    <span className={`rounded-full border px-3 py-1 text-xs ${className}`}>
      {recommendation.kind === "action"
        ? actionLabel(recommendation.action)
        : recommendation.kind === "abstain"
          ? "Abstain"
          : "Unavailable"}
    </span>
  )
}

export function locatorOutcomeLabel(
  recommendation: InvestmentAnalysisLocator["recommendation"],
): string {
  return recommendation.kind === "action"
    ? actionLabel(recommendation.action)
    : recommendation.summary
}

export function analysisOutcomeTone(
  recommendation: InvestmentAnalysisLocator["recommendation"],
): "good" | "attention" | "muted" {
  if (recommendation.kind !== "action") {
    return recommendation.kind === "abstain" ? "attention" : "muted"
  }
  return recommendation.action === "buy" || recommendation.action === "add"
    ? "good"
    : recommendation.action === "trim" || recommendation.action === "sell"
      ? "attention"
      : "muted"
}

function actionLabel(action: "buy" | "add" | "hold" | "trim" | "sell") {
  return action.charAt(0).toUpperCase() + action.slice(1)
}

function money(value: Money): string {
  return `${value.amount} ${value.currency}`
}

function nullableMoney(value: Money | null): string {
  return value ? money(value) : "Unavailable"
}

function priceRange(value: PriceRange): string {
  return `${money(value.lower)} – ${money(value.upper)}`
}

function lotRange(value: Extract<InvestmentAnalysis["sizing"], { state: "evaluated" }>["hardFeasibleLots"]): string {
  return value.kind === "available"
    ? `${formatLosslessInteger(value.lower)}–${formatLosslessInteger(value.upper)} lots`
    : value.reasons.join(" ")
}

function investmentTitle(analysis: InvestmentAnalysis): string {
  const { symbol, name } = analysis.investment
  if (symbol && name) return `${symbol} · ${name}`
  return symbol ?? name ?? "Investment Brief"
}

function formatPercent(value: string): string {
  return `${value}%`
}

export function formatProductTimestamp(value: string): string {
  const date = value.slice(0, 10)
  const time = value.slice(11, -1)
  return `${date} ${time} UTC`
}

function negativePercent(value: string): string {
  return value === "0" ? "0%" : `-${value}%`
}

function percentRange(value: { lower: string; upper: string }): string {
  return `${formatPercent(value.lower)} – ${formatPercent(value.upper)}`
}

function trackRecordLabel(
  action: RecommendationTrackRecord["groups"][number]["action"],
): string {
  if (action === "abstain") return "Abstain"
  return actionLabel(action)
}

export function BriefLoading() {
  return (
    <div className="space-y-3" aria-label="Loading investment brief">
      <Skeleton className="h-36 w-full" />
      <Skeleton className="h-56 w-full" />
    </div>
  )
}

export function BriefError({ onRetry }: { onRetry: () => void }) {
  return (
    <Alert variant="destructive">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>The Investment Brief could not be loaded</AlertTitle>
      <AlertDescription>
        Try again later.
        <Button type="button" variant="outline" size="sm" className="mt-2" onClick={onRetry}>
          Retry analysis
        </Button>
      </AlertDescription>
    </Alert>
  )
}
