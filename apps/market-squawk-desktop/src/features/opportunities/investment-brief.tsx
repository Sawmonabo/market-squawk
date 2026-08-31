import { CircleAlert, RefreshCw } from "lucide-react"
import type { ReactNode } from "react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"

import type {
  InvestmentAnalysis,
  InvestmentAnalysisLocator,
  RecommendationTrackRecord,
} from "./contracts"
import { formatLosslessInteger, formatProductTimestamp } from "./format"

type Money = NonNullable<InvestmentAnalysis["priceSummary"]["current"]>
type PriceRange = NonNullable<
  InvestmentAnalysis["priceSummary"]["scenarios"]
>["base"]
type Projection = NonNullable<InvestmentAnalysis["outcomeProjection"]>
type ProjectedScenario = Projection["downside"]
type EvidenceFamily = InvestmentAnalysis["analyticalEvidence"]["forecast"]

export function InvestmentBrief({
  analysis,
  trackRecord,
  trackRecordPending,
  trackRecordUnavailable,
  refreshing,
  onRefresh,
}: {
  analysis: InvestmentAnalysis
  trackRecord: RecommendationTrackRecord | null
  trackRecordPending: boolean
  trackRecordUnavailable: boolean
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

      <PriceRanges analysis={analysis} />
      <ProductLists analysis={analysis} />
      <AnalyticalEvidence analysis={analysis} />
      <EvidenceSummary analysis={analysis} />
      <OutcomeProjection analysis={analysis} />
      <PortfolioContext analysis={analysis} />
      <SizingSummary analysis={analysis} />
      <VirtualPaperEligibility analysis={analysis} />
      <RealizedOutcome analysis={analysis} />
      <TrackRecord
        record={trackRecord}
        pending={trackRecordPending}
        unavailable={trackRecordUnavailable}
      />
    </section>
  )
}

function PriceRanges({ analysis }: { analysis: InvestmentAnalysis }) {
  const scenarios = analysis.priceSummary.scenarios
  const actionRanges = analysis.priceSummary.actionRanges
  if (!scenarios && !actionRanges) return null
  return (
    <Disclosure title="Price targets and action ranges">
      <p className="text-xs leading-5 text-muted-foreground">
        These are saved research ranges, not guaranteed prices.
      </p>
      {scenarios ? (
        <dl className="mt-4 grid gap-4 sm:grid-cols-3">
          <Fact label="Downside target range" value={priceRange(scenarios.downside)} />
          <Fact label="Base target range" value={priceRange(scenarios.base)} />
          <Fact label="Upside target range" value={priceRange(scenarios.upside)} />
        </dl>
      ) : null}
      {actionRanges ? (
        <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <Fact label="Entry range" value={priceRange(actionRanges.entry)} />
          <Fact label="Add range" value={priceRange(actionRanges.add)} />
          <Fact label="Trim range" value={priceRange(actionRanges.trim)} />
          <Fact label="Sell / invalidation range" value={priceRange(actionRanges.exit)} />
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

function AnalyticalEvidence({ analysis }: { analysis: InvestmentAnalysis }) {
  const evidence = analysis.analyticalEvidence
  return (
    <Disclosure title="What the analysis combined">
      <p className="text-xs leading-5 text-muted-foreground">
        No single forecast, valuation, feature, pattern, or data source can set this
        recommendation or its evidence reliability.
      </p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Current market" value={evidenceFamily(evidence.currentMarket)} />
        <Fact
          label="Broader research evidence"
          value={evidenceFamily(evidence.broaderResearch)}
        />
        <Fact
          label="Price-pattern evidence"
          value={evidenceFamily(evidence.pricePattern)}
        />
        <Fact label="Forecast" value={evidenceFamily(evidence.forecast)} />
        <Fact
          label="Financial model"
          value={evidenceFamily(evidence.financialModel)}
        />
        <Fact label="Governed valuation" value={evidenceFamily(evidence.valuation)} />
        <Fact label="Historical test" value={evidenceFamily(evidence.historicalTest)} />
        <Fact label="Independent evaluation" value={evidenceFamily(evidence.outOfSample)} />
        <Fact label="Liquidity" value={evidenceFamily(evidence.liquidity)} />
        <Fact label="Portfolio risk" value={evidenceFamily(evidence.portfolioRisk)} />
        <Fact
          label="Combined evidence"
          value={`${
            evidence.combination.state === "multi_evidence" ? "Combined" : "Insufficient"
          }. ${evidence.combination.summary}`}
        />
      </dl>
    </Disclosure>
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
        <Fact label="Independent evaluation" value={evidence.outOfSample.summary} />
        <Fact label="Historical cost treatment" value={evidence.costs.summary} />
        <Fact label="Current liquidity" value={analysis.liquidity.summary} />
        <Fact label="Uncertainty" value={uncertainty.summary} />
        <Fact
          label="Combined evidence reliability (not profit odds)"
          value={
            uncertainty.state === "available"
              ? formatPercent(uncertainty.evidenceReliabilityPercent)
              : "Unavailable"
          }
        />
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
      {historical ? (
        <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          <Fact label="Historical-test meaning" value={historical.summary} />
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
      {analysis.liquidity.state === "available" ? (
        <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          <Fact
            label="Current quoted spread"
            value={formatPercent(analysis.liquidity.quotedSpreadPercent)}
          />
          <Fact
            label="Usable trading capacity"
            value={formatPercent(analysis.liquidity.policyRelativeCapacityPercent)}
          />
          <Fact label="Liquidity meaning" value={analysis.liquidity.summary} />
        </dl>
      ) : null}
    </Disclosure>
  )
}

function OutcomeProjection({ analysis }: { analysis: InvestmentAnalysis }) {
  const projection = analysis.outcomeProjection
  if (!projection) return null
  return (
    <Disclosure title="Projected outcomes">
      <p className="text-xs leading-5 text-muted-foreground">
        These are gross price projections. Expected values appear only when the saved forecast
        separately admitted a conditional mean; scenario bands are never treated as expected
        returns.
      </p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Starting price" value={money(projection.startingPrice)} />
        <Fact label="Projection horizon" value={formatProductTimestamp(projection.endsAt)} />
        <Fact
          label="Exact position scale"
          value={
            projection.positionScale
              ? `${formatLosslessInteger(
                  projection.positionScale.quantityLots,
                )} lots. ${projection.positionScale.summary}`
              : "Unavailable; exact-quantity gross profit or loss is not shown."
          }
        />
        <Fact label="Expected gross price return" value={expectedReturn(projection)} />
        <Fact
          label="Expected gross price P/L"
          value={
            projection.expectedGrossPricePnl.state === "available"
              ? `${money(projection.expectedGrossPricePnl.amount)}. ${
                  projection.expectedGrossPricePnl.summary
                }`
              : projection.expectedGrossPricePnl.summary
          }
        />
        <Fact label="Net P/L" value={projection.netPnl.summary} />
        <Fact label="Benchmark-relative return" value={projection.benchmarkReturn.summary} />
        <Fact label="After-tax P/L" value={projection.afterTaxPnl.summary} />
      </dl>
      <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Fact label="Downside price range" value={priceRange(projection.downside.priceRange)} />
        <Fact
          label="Downside absolute price change"
          value={signedMoneyRange(projection.downside.absolutePriceChange)}
        />
        {projection.downside.priceChangePercent ? (
          <Fact
            label="Downside price change"
            value={percentRange(projection.downside.priceChangePercent)}
          />
        ) : null}
        <Fact label="Downside gross price P/L" value={grossPricePnl(projection.downside)} />
        <Fact label="Base price range" value={priceRange(projection.base.priceRange)} />
        <Fact
          label="Base absolute price change"
          value={signedMoneyRange(projection.base.absolutePriceChange)}
        />
        {projection.base.priceChangePercent ? (
          <Fact
            label="Base price change"
            value={percentRange(projection.base.priceChangePercent)}
          />
        ) : null}
        <Fact label="Base gross price P/L" value={grossPricePnl(projection.base)} />
        <Fact label="Upside price range" value={priceRange(projection.upside.priceRange)} />
        <Fact
          label="Upside absolute price change"
          value={signedMoneyRange(projection.upside.absolutePriceChange)}
        />
        {projection.upside.priceChangePercent ? (
          <Fact
            label="Upside price change"
            value={percentRange(projection.upside.priceChangePercent)}
          />
        ) : null}
        <Fact label="Upside gross price P/L" value={grossPricePnl(projection.upside)} />
      </dl>
      <TextList title="Projection limitations" values={projection.limitations} empty="" />
    </Disclosure>
  )
}

function PortfolioContext({ analysis }: { analysis: InvestmentAnalysis }) {
  const context = analysis.portfolioContext
  return (
    <Disclosure title="Portfolio and risk context">
      {context.state === "available" ? (
        <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          <Fact label="Selected portfolio" value={context.portfolioLabel} />
          <Fact
            label="Current position"
            value={context.positionState === "current_position" ? "Position held" : "No position"}
          />
          <Fact
            label="Saved risk capacity"
            value={formatPercent(context.riskCapacityPercent)}
          />
          <Fact label="Context meaning" value={context.summary} />
        </dl>
      ) : (
        <p className="text-xs leading-5 text-muted-foreground">{context.summary}</p>
      )}
    </Disclosure>
  )
}

function SizingSummary({ analysis }: { analysis: InvestmentAnalysis }) {
  const sizing = analysis.sizing
  if (!sizing) return null
  return (
    <Disclosure title="Research sizing range">
      <p className="text-xs leading-5 text-muted-foreground">{sizing.summary}</p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-3">
        <Fact label="Current lots" value={formatLosslessInteger(sizing.currentLots)} />
        <Fact label="Mandatory range" value={lotRange(sizing.hardFeasibleLots)} />
        <Fact label="Preferred range" value={lotRange(sizing.preferredFeasibleLots)} />
      </dl>
    </Disclosure>
  )
}

function VirtualPaperEligibility({ analysis }: { analysis: InvestmentAnalysis }) {
  const eligibility = analysis.virtualPaperEligibility
  return (
    <Disclosure title="Virtual-paper eligibility">
      <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Status" value="Not eligible from this brief" />
        <Fact label="Order authority" value="None" />
        <Fact label="Explicit paper approval" value="Required in a separate workflow" />
        <Fact label="Fresh risk check" value="Required before any simulated order" />
        <Fact label="Why" value={eligibility.summary} />
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
      <Disclosure title="Comparable history">
        <p className="text-xs leading-5 text-muted-foreground">
          Comparable saved outcomes are unavailable right now.
        </p>
      </Disclosure>
    )
  }
  const represented = record.groups.filter((group) => group.recommendationCount > 0)
  return (
    <Disclosure title="Comparable history">
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
    </Disclosure>
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

function Fact({ label, value }: { label: string; value: string }) {
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

function signedMoneyRange(value: ProjectedScenario["absolutePriceChange"]): string {
  return `${money(value.lower)} – ${money(value.upper)}`
}

function grossPricePnl(value: ProjectedScenario): string {
  return value.grossPricePnl.state === "available"
    ? `${signedMoneyRange(value.grossPricePnl.range)}. ${value.grossPricePnl.summary}`
    : value.grossPricePnl.summary
}

function expectedReturn(value: Projection): string {
  const expected = value.expectedReturn
  if (expected.state === "unavailable") return expected.summary
  const retainedValue =
    expected.grossPriceReturnPercent === null
      ? `Exact saved ratio: ${money(expected.exactRatio.numerator)} divided by ${money(
          expected.exactRatio.denominator,
        )}; no unrounded decimal display is available`
      : formatPercent(expected.grossPriceReturnPercent)
  return `${retainedValue}. ${expected.summary}`
}

function evidenceFamily(value: EvidenceFamily): string {
  return `${value.state === "available" ? "Available" : "Unavailable"}. ${value.summary}`
}

function lotRange(value: NonNullable<InvestmentAnalysis["sizing"]>["hardFeasibleLots"]): string {
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
