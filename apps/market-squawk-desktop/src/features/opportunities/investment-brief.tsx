import { CircleAlert, FileText, RefreshCw } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"

import type {
  InvestmentAnalysis,
  InvestmentAnalysisLocator,
  InvestmentAnalysisResult,
  RecommendationTrackRecord,
} from "./contracts"
import { formatLosslessInteger, formatUnixNanos } from "./format"

type Evidence = InvestmentAnalysis["evidence"]
type Money = NonNullable<Evidence["market"]>["price"]
type PriceRange = NonNullable<Evidence["priceForecast"]>["ranges"]["base"]
type EvidenceWindow = NonNullable<Evidence["market"]>["window"]

export type TrackRecordPresentation =
  | { state: "loading" }
  | { state: "unavailable"; detail: string }
  | { state: "error"; detail: string; onRetry: () => void }
  | { state: "ready"; value: RecommendationTrackRecord }

export function InvestmentBrief({
  analysis,
  trackRecord,
  refreshing,
  onRefresh,
}: {
  analysis: InvestmentAnalysis
  trackRecord: TrackRecordPresentation
  refreshing: boolean
  onRefresh: () => void
}) {
  const outcome = outcomeSummary(analysis.result)
  const reliability =
    analysis.result.kind === "unavailable"
      ? null
      : analysis.result.evidenceReliability

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
            Investment Brief
          </h2>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            This brief explains the saved assumptions, supporting information, and outcome. It
            does not add a ranking, invent a return forecast, or turn the analysis into an order.
          </p>
        </div>
        <div className="flex flex-col items-end gap-3">
          <OutcomeBadge kind={analysis.result.kind} label={outcome.label} />
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

      <Alert className="mt-5">
        <CircleAlert aria-hidden="true" />
        <AlertTitle>Research only — execution ineligible</AlertTitle>
        <AlertDescription>
          This analysis is {enumLabel(analysis.executionEligibility)} for execution. The projections
          and sizing ranges below cannot create a target or place an order.
        </AlertDescription>
      </Alert>

      <div className="mt-5 rounded-lg border border-border bg-background/35 p-4">
        <p className="text-sm font-semibold">{outcome.title}</p>
        <p className="mt-1 text-sm leading-6 text-muted-foreground">{outcome.detail}</p>
        {analysis.result.kind === "no_action" ? (
          <p className="mt-3 text-xs leading-5 text-muted-foreground">
            Why no action: {invalidatorLabel(analysis.result.invalidators[0]!)}
          </p>
        ) : null}
      </div>

      <dl className="mt-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Instrument record" value={analysis.evidence.instrumentId} mono />
        <Fact label="Account" value={analysis.evidence.accountId} mono />
        <Fact label="Reporting currency" value={analysis.evidence.currency} />
        <Fact label="Information current through" value={formatUnixNanos(analysis.evidence.asOf)} />
        <Fact label="Analysis horizon" value={formatUnixNanos(analysis.result.horizonAt)} />
        <Fact label="Brief expires" value={formatUnixNanos(analysis.result.expiresAt)} />
        <Fact
          label="Information reliability"
          value={
            reliability
              ? formatPpm(reliability.valuePpm)
              : "Not produced for an unavailable analysis"
          }
        />
      </dl>

      {analysis.result.kind === "generated" ? (
        <GeneratedDetails result={analysis.result} />
      ) : null}

      {analysis.result.kind === "unavailable" ? (
        <Alert className="mt-5">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Analysis unavailable</AlertTitle>
          <AlertDescription>
            The saved reason and any partial or mismatched information are shown below. Market
            Squawk does not fill in missing information.
          </AlertDescription>
        </Alert>
      ) : null}

      <ProjectionDetails analysis={analysis} />
      <SizingDetails analysis={analysis} />
      <RealizedOutcomeDetails analysis={analysis} />
      <TrackRecordDetails presentation={trackRecord} />
      <PolicyDetails policy={analysis.policy} />
      {reliability ? <ReliabilityDetails reliability={reliability} /> : null}
      <EvidenceDetails evidence={analysis.evidence} />
    </section>
  )
}

function GeneratedDetails({
  result,
}: {
  result: Extract<InvestmentAnalysisResult, { kind: "generated" }>
}) {
  const rangeRows = [
    ["Downside", result.priceLadder.ranges.downside],
    ["Base", result.priceLadder.ranges.base],
    ["Upside", result.priceLadder.ranges.upside],
    ["Entry", result.priceLadder.ranges.entry],
    ["Add", result.priceLadder.ranges.add],
    ["Trim", result.priceLadder.ranges.trim],
    ["Exit", result.priceLadder.ranges.exit],
  ] as const

  return (
    <>
      <Disclosure title="Price cases and action ranges">
        <p className="text-xs leading-5 text-muted-foreground">
          These are the prices saved with the analysis. They are research ranges, not promised
          gains or losses.
        </p>
        <dl className="mt-4 grid gap-4 sm:grid-cols-3">
          <Fact label="Downside case" value={money(result.priceLadder.cases.downside)} />
          <Fact label="Base case" value={money(result.priceLadder.cases.base)} />
          <Fact label="Upside case" value={money(result.priceLadder.cases.upside)} />
          <Fact label="Add case" value={money(result.priceLadder.addCase)} />
        </dl>
        <dl className="mt-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          {rangeRows.map(([label, range]) => (
            <Fact key={label} label={`${label} range`} value={priceRange(range)} />
          ))}
        </dl>
      </Disclosure>

      <Disclosure title="Action-zone meaning">
        <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          <Fact label="Action" value={actionLabel(result.action)} />
          <Fact
            label="Semantics version"
            value={result.actionZoneSemantics.version.toLocaleString("en-US")}
          />
          <Fact
            label="Reference zone"
            value={nullablePriceRange(result.actionZoneSemantics.referenceZone)}
          />
          <Fact
            label="Exclusive floor"
            value={nullableMoney(result.actionZoneSemantics.triggerFloorExclusive)}
          />
          <Fact
            label="Inclusive floor"
            value={nullableMoney(result.actionZoneSemantics.triggerFloorInclusive)}
          />
          <Fact
            label="Inclusive ceiling"
            value={nullableMoney(result.actionZoneSemantics.triggerCeilingInclusive)}
          />
        </dl>
      </Disclosure>
    </>
  )
}

function ProjectionDetails({ analysis }: { analysis: InvestmentAnalysis }) {
  const projection = analysis.projection
  if (!projection) {
    return (
      <Disclosure title="Gross outcome projection">
        <p className="text-xs leading-5 text-muted-foreground">
          No gross outcome projection was saved for this analysis. Market Squawk does not infer
          one from the price ranges.
        </p>
      </Disclosure>
    )
  }
  const cases = [
    ["Downside", projection.downside],
    ["Base", projection.base],
    ["Upside", projection.upside],
  ] as const
  return (
    <Disclosure title="Gross outcome projection">
      <p className="text-xs leading-5 text-muted-foreground">
        Gross price-change inputs relative to the observed starting price. Fees, taxes, benchmark
        performance, and actual trading results are not included.
      </p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Execution eligible" value="No" />
        <Fact label="Starting price" value={money(projection.mark)} />
        <Fact label="Horizon" value={formatUnixNanos(projection.horizonAt)} />
        <Fact label="Net P/L" value={unavailableLabel(projection.netPnl.reason)} />
        <Fact
          label="Benchmark return"
          value={unavailableLabel(projection.benchmarkReturn.reason)}
        />
        <Fact
          label="After-tax P/L"
          value={unavailableLabel(projection.afterTaxPnl.reason)}
        />
      </dl>
      <div className="mt-5 grid gap-3 xl:grid-cols-3">
        {cases.map(([label, value]) => (
          <div key={label} className="rounded-lg border border-border bg-card/35 p-3">
            <h3 className="text-xs font-semibold">{label}</h3>
            <dl className="mt-3 grid gap-3">
              <Fact label="Price range" value={priceRange(value.priceRange)} />
              <Fact
                label="Lower price change ÷ starting price"
                value={`${money(value.grossReturnFromMark.lowerNumerator)} ÷ ${money(value.grossReturnFromMark.denominator)}`}
              />
              <Fact
                label="Upper price change ÷ starting price"
                value={`${money(value.grossReturnFromMark.upperNumerator)} ÷ ${money(value.grossReturnFromMark.denominator)}`}
              />
            </dl>
          </div>
        ))}
      </div>
    </Disclosure>
  )
}

function SizingDetails({ analysis }: { analysis: InvestmentAnalysis }) {
  const sizing = analysis.sizing
  return (
    <Disclosure title="Sizing feasibility — no selected target">
      {!sizing ? (
        <p className="text-xs leading-5 text-muted-foreground">
          No sizing range was saved. Market Squawk will not invent a quantity from price, cash,
          risk, or portfolio information.
        </p>
      ) : (
        <>
          <p className="text-xs leading-5 text-muted-foreground">
            These ranges show what may fit the recorded limits. No target quantity was selected
            and no order was created.
          </p>
          <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
            <Fact label="Execution eligible" value="No" />
            <Fact label="Evaluated" value={formatUnixNanos(sizing.evaluatedAt)} />
            <Fact
              label="Current lots"
              value={formatLosslessInteger(sizing.currentLots)}
            />
            <Fact
              label="Hard feasible lots"
              value={feasibleLotsLabel(sizing.hardFeasibleLots)}
            />
            <Fact
              label="Preferred feasible lots"
              value={feasibleLotsLabel(sizing.preferredFeasibleLots)}
            />
            <Fact label="Selected target lots" value="Not selected" />
            <Fact label="Order quantity" value="Not created" />
          </dl>
        </>
      )}
    </Disclosure>
  )
}

function RealizedOutcomeDetails({ analysis }: { analysis: InvestmentAnalysis }) {
  const outcome = analysis.realizedOutcome
  return (
    <Disclosure title="Current realized-outcome status">
      {!outcome ? (
        <p className="text-xs leading-5 text-muted-foreground">
          No current result is available. Missing results are not counted as completed or
          profitable.
        </p>
      ) : (
        <>
          <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
            <Fact label="Status" value={enumLabel(outcome.kind)} />
            <Fact label="Revision" value={outcome.revision.toLocaleString("en-US")} />
            <Fact label="Evaluated" value={formatUnixNanos(outcome.evaluatedAt)} />
            <Fact label="Execution eligible" value="No" />
            {outcome.kind === "pending" || outcome.kind === "unavailable" ? (
              <Fact label="Reason" value={enumLabel(outcome.reason)} />
            ) : null}
          </dl>
          {outcome.kind === "completed" ? (
            <>
              <p className="mt-4 text-xs leading-5 text-muted-foreground">
                Gross instrument-price movement only. It is not portfolio profit, execution
                performance, a benchmark-relative result, or an after-tax return.
              </p>
              <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
                <Fact label="Metric" value={enumLabel(outcome.metric)} />
                <Fact label="Start mark" value={money(outcome.startMark)} />
                <Fact label="Endpoint price" value={money(outcome.endpointPrice)} />
                <Fact label="Gross price-return decimal" value={outcome.grossPriceReturn} mono />
                <Fact label="Observed" value={formatUnixNanos(outcome.observedAt)} />
                <Fact label="Available" value={formatUnixNanos(outcome.availableAt)} />
                <Fact label="Net return" value={unavailableLabel(outcome.netReturn.reason)} />
                <Fact
                  label="Benchmark return"
                  value={unavailableLabel(outcome.benchmarkReturn.reason)}
                />
                <Fact
                  label="After-tax return"
                  value={unavailableLabel(outcome.afterTaxReturn.reason)}
                />
                <Fact
                  label="Settlement"
                  value={unavailableLabel(outcome.settlement.reason)}
                />
              </dl>
            </>
          ) : null}
        </>
      )}
    </Disclosure>
  )
}

function TrackRecordDetails({
  presentation,
}: {
  presentation: TrackRecordPresentation
}) {
  return (
    <Disclosure title="Recommendation history">
      {presentation.state === "loading" ? (
        <div className="grid gap-3 sm:grid-cols-2" aria-label="Loading recommendation track record">
          <Skeleton className="h-24 w-full" />
          <Skeleton className="h-24 w-full" />
        </div>
      ) : presentation.state === "unavailable" ? (
        <p className="text-xs leading-5 text-muted-foreground">{presentation.detail}</p>
      ) : presentation.state === "error" ? (
        <Alert variant="destructive">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Recommendation history could not be loaded</AlertTitle>
          <AlertDescription>
            {presentation.detail}
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="mt-2"
              onClick={presentation.onRetry}
            >
              Retry recommendation history
            </Button>
          </AlertDescription>
        </Alert>
      ) : (
        <TrackRecordReady value={presentation.value} />
      )}
    </Disclosure>
  )
}

function TrackRecordReady({ value }: { value: RecommendationTrackRecord }) {
  return (
    <>
      <p className="text-xs leading-5 text-muted-foreground">
        Current results for recommendations made with the same analysis settings and horizon.
        Groups remain separate, and results appear only when sample size and coverage are adequate.
        Forecast calibration and actual trading performance are not included.
      </p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact
          label="Horizon"
          value={formatDuration(value.horizonNanos)}
        />
        <Fact label="Evaluated" value={formatUnixNanos(value.evaluatedAt)} />
        <Fact
          label="Unavailable analyses outside cohorts"
          value={value.analysisUnavailableCount.toLocaleString("en-US")}
        />
        <Fact
          label="Minimum completed samples"
          value={value.minimumCompletedSamples.toLocaleString("en-US")}
        />
        <Fact
          label="Minimum coverage"
          value={formatPpm(value.minimumCoveragePpm)}
        />
        <Fact label="Forecast calibration included" value="No" />
        <Fact label="Execution performance included" value="No" />
      </dl>
      <div className="mt-5 overflow-x-auto">
        <table className="w-full min-w-[900px] border-collapse text-left text-xs">
          <thead className="text-muted-foreground">
            <tr className="border-b border-border">
              <th className="px-2 py-2 font-medium">Cohort</th>
              <th className="px-2 py-2 font-medium">Total</th>
              <th className="px-2 py-2 font-medium">Due</th>
              <th className="px-2 py-2 font-medium">Completed</th>
              <th className="px-2 py-2 font-medium">Pending</th>
              <th className="px-2 py-2 font-medium">Unavailable</th>
              <th className="px-2 py-2 font-medium">Coverage</th>
              <th className="px-2 py-2 font-medium">Historical result</th>
            </tr>
          </thead>
          <tbody>
            {value.groups.map((group) => (
              <tr key={group.cohort} className="border-b border-border/70 last:border-0">
                <td className="px-2 py-3 font-medium">{trackCohortLabel(group.cohort)}</td>
                <td className="px-2 py-3">{group.publicationCount.toLocaleString("en-US")}</td>
                <td className="px-2 py-3">{group.dueCount.toLocaleString("en-US")}</td>
                <td className="px-2 py-3">{group.completedCount.toLocaleString("en-US")}</td>
                <td className="px-2 py-3">{group.pendingCount.toLocaleString("en-US")}</td>
                <td className="px-2 py-3">{group.unavailableCount.toLocaleString("en-US")}</td>
                <td className="px-2 py-3">{formatPpm(group.coveragePpm)}</td>
                <td className="px-2 py-3">{trackPerformanceLabel(group.performance)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

function PolicyDetails({ policy }: { policy: InvestmentAnalysis["policy"] }) {
  return (
    <Disclosure title="Assumptions and limits">
      <PolicyList title="Assumptions" values={policy.assumptions} />
      <PolicyList
        title="Invalidation conditions"
        values={policy.invalidationConditions}
      />
      <PolicyList title="Limitations" values={policy.limitations} />
    </Disclosure>
  )
}

function ReliabilityDetails({
  reliability,
}: {
  reliability: Exclude<
    InvestmentAnalysisResult,
    { kind: "unavailable" }
  >["evidenceReliability"]
}) {
  return (
    <Disclosure title="Supporting-information reliability">
      <p className="text-xs leading-5 text-muted-foreground">
        This measures how complete and dependable the supporting information was under the saved
        rules. It is not a probability of profit or a predicted return.
      </p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-2">
        <Fact label="Meaning" value={enumLabel(reliability.meaning)} />
        <Fact
          label="Aggregate reliability"
          value={formatPpm(reliability.valuePpm)}
        />
      </dl>
      <div className="mt-4 overflow-x-auto">
        <table className="w-full min-w-[560px] border-collapse text-left text-xs">
          <thead className="text-muted-foreground">
            <tr className="border-b border-border">
              <th className="px-2 py-2 font-medium">Component</th>
              <th className="px-2 py-2 font-medium">Reliability</th>
              <th className="px-2 py-2 font-medium">Weight</th>
            </tr>
          </thead>
          <tbody>
            {reliability.components.map((component) => (
              <tr key={component.kind} className="border-b border-border/70 last:border-0">
                <td className="px-2 py-3">{enumLabel(component.kind)}</td>
                <td className="px-2 py-3 font-mono">
                  {formatPpm(component.valuePpm)}
                </td>
                <td className="px-2 py-3 font-mono">
                  {formatPpm(component.weightPpm)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Disclosure>
  )
}

function EvidenceDetails({ evidence }: { evidence: Evidence }) {
  return (
    <Disclosure title="Supporting information">
      <p className="text-xs leading-5 text-muted-foreground">
        Each section shows the information saved with this analysis. Missing or mismatched inputs
        remain visible when they explain why an analysis could not proceed.
      </p>
      <div className="mt-4 space-y-3">
        <EvidenceSection title="Market reference" present={evidence.market !== null}>
          {evidence.market ? <MarketEvidence evidence={evidence.market} /> : null}
        </EvidenceSection>
        <EvidenceSection
          title="Price forecast"
          present={evidence.priceForecast !== null}
        >
          {evidence.priceForecast ? (
            <ForecastEvidence evidence={evidence.priceForecast} />
          ) : null}
        </EvidenceSection>
        <EvidenceSection title="Valuation" present={evidence.valuation !== null}>
          {evidence.valuation ? <ValuationEvidence evidence={evidence.valuation} /> : null}
        </EvidenceSection>
        <EvidenceSection title="Backtest" present={evidence.backtest !== null}>
          {evidence.backtest ? <BacktestEvidence evidence={evidence.backtest} /> : null}
        </EvidenceSection>
        <EvidenceSection title="Liquidity" present={evidence.liquidity !== null}>
          {evidence.liquidity ? <LiquidityEvidence evidence={evidence.liquidity} /> : null}
        </EvidenceSection>
        <EvidenceSection
          title="Portfolio risk"
          present={evidence.portfolioRisk !== null}
        >
          {evidence.portfolioRisk ? (
            <PortfolioRiskEvidence evidence={evidence.portfolioRisk} />
          ) : null}
        </EvidenceSection>
      </div>
    </Disclosure>
  )
}

function MarketEvidence({
  evidence,
}: {
  evidence: NonNullable<Evidence["market"]>
}) {
  return (
    <>
      <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Instrument record" value={evidence.instrumentId} mono />
        <Fact label="Observed price" value={money(evidence.price)} />
        <Fact label="Quality" value={enumLabel(evidence.quality)} />
        <Fact label="Price kind" value={enumLabel(evidence.priceKind)} />
        <Fact label="Adjustment basis" value={enumLabel(evidence.adjustmentBasis)} />
      </dl>
      <EvidenceWindowFacts window={evidence.window} />
    </>
  )
}

function ForecastEvidence({
  evidence,
}: {
  evidence: NonNullable<Evidence["priceForecast"]>
}) {
  const expected = evidence.expectedTerminal
  return (
    <>
      <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Instrument record" value={evidence.instrumentId} mono />
        <Fact label="Horizon" value={formatUnixNanos(evidence.horizonAt)} />
        {expected ? (
          <>
            <Fact
              label="Expected terminal value (conditional mean)"
              value={money(expected.price)}
            />
            <Fact
              label="Expected terminal horizon"
              value={formatUnixNanos(expected.horizonAt)}
            />
            <Fact
              label="Expected terminal statistic"
              value={enumLabel(expected.statistic)}
            />
          </>
        ) : (
          <Fact
            label="Expected terminal value"
            value="Unavailable — no admitted conditional mean"
          />
        )}
        <Fact label="Downside case" value={money(evidence.cases.downside)} />
        <Fact label="Base case" value={money(evidence.cases.base)} />
        <Fact label="Upside case" value={money(evidence.cases.upside)} />
        <Fact label="Downside range" value={priceRange(evidence.ranges.downside)} />
        <Fact label="Base range" value={priceRange(evidence.ranges.base)} />
        <Fact label="Upside range" value={priceRange(evidence.ranges.upside)} />
        <Fact
          label="Nominal coverage"
          value={formatPpm(evidence.calibration.nominalCoveragePpm)}
        />
        <Fact
          label="Realized coverage"
          value={formatPpm(evidence.calibration.realizedCoveragePpm)}
        />
        <Fact
          label="Completed outcomes"
          value={evidence.calibration.completedOutcomes.toLocaleString("en-US")}
        />
      </dl>
      <p className="mt-4 text-xs leading-5 text-muted-foreground">
        {expected
          ? "This conditional mean can support expected-return analysis. It is not a probability, a guaranteed profit, or a net-profit estimate."
          : "Expected return is unavailable because the forecast does not include a sufficiently supported conditional mean."}
      </p>
      <EvidenceWindowFacts window={evidence.window} />
    </>
  )
}

function ValuationEvidence({
  evidence,
}: {
  evidence: NonNullable<Evidence["valuation"]>
}) {
  return (
    <>
      <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Instrument record" value={evidence.instrumentId} mono />
        <Fact
          label="Fair value (per instrument unit)"
          value={money(evidence.fairValue)}
        />
        <Fact label="Valuation basis" value={enumLabel(evidence.basis)} />
        <Fact label="Horizon" value={formatUnixNanos(evidence.horizonAt)} />
      </dl>
      <EvidenceWindowFacts window={evidence.window} />
    </>
  )
}

function BacktestEvidence({
  evidence,
}: {
  evidence: NonNullable<Evidence["backtest"]>
}) {
  return (
    <>
      <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Instrument record" value={evidence.instrumentId} mono />
        <Fact label="Currency" value={evidence.currency} />
        <Fact
          label="Outcome horizon"
          value={formatDuration(evidence.outcomeHorizonNanos)}
        />
        <Fact
          label="Net return"
          value={formatBasisPoints(evidence.netReturnBasisPoints)}
        />
        <Fact
          label="Maximum drawdown"
          value={formatBasisPoints(evidence.maxDrawdownBasisPoints)}
        />
        <Fact
          label="Fee assumption"
          value={formatBasisPoints(evidence.feeBasisPoints)}
        />
        <Fact
          label="Slippage assumption"
          value={formatBasisPoints(evidence.slippageBasisPoints)}
        />
        <Fact
          label="Maximum random slippage"
          value={formatBasisPoints(evidence.maximumRandomSlippageBasisPoints)}
        />
        <Fact label="Observations" value={evidence.observations.toLocaleString("en-US")} />
        <Fact label="Trials" value={evidence.trials.toLocaleString("en-US")} />
        <Fact
          label="Stability"
          value={formatPpm(evidence.stabilityPpm)}
        />
        <Fact
          label="Simulation cutoff"
          value={formatUnixNanos(evidence.simulationCutoffAt)}
        />
      </dl>
      <EvidenceWindowFacts window={evidence.window} />
    </>
  )
}

function LiquidityEvidence({
  evidence,
}: {
  evidence: NonNullable<Evidence["liquidity"]>
}) {
  return (
    <>
      <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Instrument record" value={evidence.instrumentId} mono />
        <Fact label="Currency" value={evidence.currency} />
        <Fact
          label="Quoted spread"
          value={formatBasisPoints(evidence.quotedSpreadBasisPoints)}
        />
        <Fact
          label="Capacity"
          value={formatPpm(evidence.capacityPpm)}
        />
        <Fact label="Quality" value={enumLabel(evidence.quality)} />
      </dl>
      <EvidenceWindowFacts window={evidence.window} />
    </>
  )
}

function PortfolioRiskEvidence({
  evidence,
}: {
  evidence: NonNullable<Evidence["portfolioRisk"]>
}) {
  const position =
    evidence.positionState.kind === "no_position"
      ? "No saved position"
      : [
          "Position available",
          evidence.positionState.addAllowed ? "add allowed" : "add not allowed",
          evidence.positionState.trimAllowed ? "trim allowed" : "trim not allowed",
          evidence.positionState.exitAllowed ? "exit allowed" : "exit not allowed",
        ].join(" · ")

  return (
    <>
      <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Instrument record" value={evidence.instrumentId} mono />
        <Fact label="Account" value={evidence.accountId} mono />
        <Fact label="Currency" value={evidence.currency} />
        <Fact label="Position state" value={position} />
        <Fact
          label="Risk capacity"
          value={formatPpm(evidence.riskCapacityPpm)}
        />
      </dl>
      <EvidenceWindowFacts window={evidence.window} />
    </>
  )
}

function EvidenceWindowFacts({ window }: { window: EvidenceWindow }) {
  return (
    <dl className="mt-4 grid gap-4 border-t border-border/70 pt-4 sm:grid-cols-2 xl:grid-cols-4">
      <Fact label="Observed" value={formatUnixNanos(window.observedAt)} />
      <Fact label="Available" value={formatUnixNanos(window.availableAt)} />
      <Fact label="Expires" value={formatUnixNanos(window.expiresAt)} />
    </dl>
  )
}

function EvidenceSection({
  title,
  present,
  children,
}: {
  title: string
  present: boolean
  children: React.ReactNode
}) {
  return (
    <details className="rounded-lg border border-border bg-background/30 p-3">
      <summary className="cursor-pointer text-xs font-semibold">
        {title} · {present ? "Available" : "Not available"}
      </summary>
      {present ? (
        <div className="mt-4">{children}</div>
      ) : (
        <p className="mt-3 text-xs text-muted-foreground">
          No {title.toLocaleLowerCase()} information is present in this analysis.
        </p>
      )}
    </details>
  )
}

function Disclosure({
  title,
  children,
}: {
  title: string
  children: React.ReactNode
}) {
  return (
    <details className="mt-5 rounded-lg border border-border bg-background/30 p-4">
      <summary className="cursor-pointer text-sm font-semibold">{title}</summary>
      <div className="mt-4">{children}</div>
    </details>
  )
}

function PolicyList({ title, values }: { title: string; values: readonly string[] }) {
  return (
    <div className="mt-5">
      <h3 className="text-xs font-semibold">{title}</h3>
      <ul className="mt-2 list-disc space-y-2 pl-5 text-xs leading-5 text-muted-foreground">
        {values.map((value, index) => (
          <li key={`${index}:${value}`}>{value}</li>
        ))}
      </ul>
    </div>
  )
}

function Fact({
  label,
  value,
  mono = false,
}: {
  label: string
  value: string
  mono?: boolean
}) {
  return (
    <div className="min-w-0">
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd
        className={`mt-1 break-words text-xs leading-5 ${mono ? "font-mono" : "font-medium"}`}
      >
        {value}
      </dd>
    </div>
  )
}

function OutcomeBadge({
  kind,
  label,
}: {
  kind: InvestmentAnalysisResult["kind"]
  label: string
}) {
  const tone =
    kind === "generated"
      ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-200"
      : kind === "no_action"
        ? "border-amber-400/30 bg-amber-400/10 text-amber-100"
        : "border-border bg-muted/40 text-muted-foreground"
  return (
    <span className={`rounded-full border px-2.5 py-1 text-[10px] font-medium ${tone}`}>
      {label}
    </span>
  )
}

function outcomeSummary(result: InvestmentAnalysisResult): {
  label: string
  title: string
  detail: string
} {
  switch (result.kind) {
    case "generated":
      return {
        label: "Generated",
        title: `${actionLabel(result.action)} analysis generated`,
        detail:
          "The analysis includes a research action, price ranges, action boundaries, and a " +
          "reliability assessment under the saved rules.",
      }
    case "no_action":
      return {
        label: "No action",
        title: "The saved rules produced no action",
        detail: noActionReasonLabel(result.reason),
      }
    case "unavailable":
      return {
        label: "Unavailable",
        title: "The analysis could not proceed",
        detail: unavailableReasonLabel(result.reason),
      }
  }
}

export function locatorOutcomeLabel(
  outcome: InvestmentAnalysisLocator["outcome"],
): string {
  switch (outcome.kind) {
    case "generated":
      return `Generated · ${actionLabel(outcome.action)}`
    case "no_action":
      return `No action · ${noActionReasonLabel(outcome.reason)}`
    case "unavailable":
      return `Unavailable · ${unavailableReasonLabel(outcome.reason)}`
  }
}

function actionLabel(
  value: Extract<InvestmentAnalysisResult, { kind: "generated" }>["action"],
): string {
  return {
    buy: "Buy",
    add: "Add",
    hold: "Hold",
    trim: "Trim",
    sell: "Sell",
  }[value]
}

function noActionReasonLabel(
  value: Extract<InvestmentAnalysisResult, { kind: "no_action" }>["reason"],
): string {
  return {
    conflicting_forecast_and_valuation:
      "The forecast and valuation did not support one consistent action.",
    backtest_below_policy: "The backtest did not meet the saved requirements.",
    liquidity_below_policy: "Liquidity did not meet the saved requirements.",
    portfolio_risk_below_policy:
      "Account-specific portfolio risk did not meet the saved requirements.",
    evidence_reliability_below_policy:
      "Supporting-information reliability did not meet the saved requirements.",
    position_state_not_actionable:
      "The saved position did not allow an action under the saved rules.",
    generated_price_order_collapsed:
      "The generated price ordering could not preserve the required action zones.",
  }[value]
}

function invalidatorLabel(
  value: Extract<InvestmentAnalysisResult, { kind: "no_action" }>["invalidators"][number],
): string {
  return {
    forecast_valuation_conflict: "Forecast and valuation conflict",
    backtest_policy_breach: "Backtest requirements not met",
    liquidity_policy_breach: "Liquidity requirements not met",
    portfolio_risk_policy_breach: "Portfolio-risk requirements not met",
    evidence_reliability_policy_breach: "Information reliability requirements not met",
    position_state_incompatible: "Position state incompatible",
    generated_price_order_collapsed: "Generated price order collapsed",
  }[value]
}

function unavailableReasonLabel(
  reason: Extract<InvestmentAnalysisResult, { kind: "unavailable" }>["reason"],
): string {
  switch (reason.kind) {
    case "missing_evidence":
      return `${evidenceKindLabel(reason.evidence)} information is missing.`
    case "instrument_mismatch":
      return `${evidenceKindLabel(reason.evidence)} information belongs to a different investment.`
    case "currency_mismatch":
      return (
        `${evidenceKindLabel(reason.evidence)} information uses ${reason.actual}, ` +
        `not expected currency ${reason.expected}.`
      )
    case "account_mismatch":
      return "Portfolio information belongs to a different account."
    case "not_available_at_cutoff":
      return (
        `${evidenceKindLabel(reason.evidence)} information was not available ` +
        "at the analysis cutoff."
      )
    case "expired_evidence":
      return `${evidenceKindLabel(reason.evidence)} information had expired.`
    case "stale_evidence":
      return `${evidenceKindLabel(reason.evidence)} information was too old for the saved rules.`
    case "rejected_quality":
      return (
        `${evidenceKindLabel(reason.evidence)} information had unacceptable quality: ` +
        `${enumLabel(reason.quality)}.`
      )
    case "forecast_horizon_mismatch":
      return (
        `The forecast horizon was ${formatUnixNanos(reason.actual)}; ` +
        `policy expected ${formatUnixNanos(reason.expected)}.`
      )
    case "valuation_horizon_mismatch":
      return (
        `The valuation horizon was ${formatUnixNanos(reason.actual)}; ` +
        `policy expected ${formatUnixNanos(reason.expected)}.`
      )
    case "backtest_horizon_mismatch":
      return (
        `The backtest horizon was ${formatDuration(reason.actualNanos)}; ` +
        `the saved rules expected ${formatDuration(reason.expectedNanos)}.`
      )
    case "insufficient_forecast_outcomes":
      return (
        `The forecast retained ${reason.actual.toLocaleString("en-US")} completed ` +
        `outcomes; the saved rules required ${reason.required.toLocaleString("en-US")}.`
      )
    case "unsupported_forecast_coverage":
      return (
        `Forecast coverage was ${formatPpm(reason.actualPpm)}; ` +
        `the saved range was ${formatPpm(reason.minimumPpm)}–${formatPpm(reason.maximumPpm)}.`
      )
    case "insufficient_backtest_observations":
      return (
        `The backtest retained ${reason.actual.toLocaleString("en-US")} ` +
        `observations; the saved rules required ${reason.required.toLocaleString("en-US")}.`
      )
    case "insufficient_backtest_trials":
      return (
        `The backtest included ${reason.actual.toLocaleString("en-US")} trials; ` +
        `the saved rules required ${reason.required.toLocaleString("en-US")}.`
      )
    case "reserved_portfolio_revision":
      return "The portfolio revision was reserved and could not authorize analysis."
  }
}

function evidenceKindLabel(
  value: Extract<
    Extract<InvestmentAnalysisResult, { kind: "unavailable" }>["reason"],
    { evidence: unknown }
  >["evidence"],
): string {
  return {
    market: "Market",
    price_forecast: "Price forecast",
    valuation: "Valuation",
    backtest: "Backtest",
    liquidity: "Liquidity",
    portfolio_risk: "Portfolio risk",
  }[value]
}

function feasibleLotsLabel(
  value: NonNullable<InvestmentAnalysis["sizing"]>["hardFeasibleLots"],
): string {
  return value.kind === "available"
    ? `${formatLosslessInteger(value.lower)} through ${formatLosslessInteger(value.upper)} lots`
    : `Unavailable — ${value.reasons.map(enumLabel).join("; ")}`
}

function unavailableLabel(reason: string): string {
  return `Unavailable — ${enumLabel(reason)}`
}

function trackCohortLabel(
  cohort: RecommendationTrackRecord["groups"][number]["cohort"],
): string {
  return cohort === "no_action_control" ? "No-action control" : actionLabel(cohort)
}

function trackPerformanceLabel(
  performance: RecommendationTrackRecord["groups"][number]["performance"],
): string {
  if (performance.kind === "available") {
    return `Mean gross price-return decimal ${performance.meanGrossPriceReturn}; ${performance.positiveOutcomes} positive, ${performance.zeroOutcomes} zero, ${performance.negativeOutcomes} negative`
  }
  switch (performance.reason) {
    case "no_due_outcomes":
      return "Unavailable — no due outcomes"
    case "insufficient_completed_samples":
      return `Unavailable — ${performance.actual} completed; ${performance.required} required`
    case "insufficient_coverage":
      return `Unavailable — ${formatPpm(performance.actualPpm)} coverage; ${formatPpm(performance.requiredPpm)} required`
  }
}

function formatDuration(value: string): string {
  try {
    const totalSeconds = BigInt(value) / 1_000_000_000n
    const days = totalSeconds / 86_400n
    const hours = (totalSeconds % 86_400n) / 3_600n
    const minutes = (totalSeconds % 3_600n) / 60n
    const parts = [
      days > 0n ? `${days.toLocaleString("en-US")} day${days === 1n ? "" : "s"}` : null,
      hours > 0n ? `${hours} hour${hours === 1n ? "" : "s"}` : null,
      minutes > 0n ? `${minutes} minute${minutes === 1n ? "" : "s"}` : null,
    ].filter((part): part is string => part !== null)
    return parts.length > 0 ? parts.slice(0, 2).join(" ") : "Less than one minute"
  } catch {
    return "Unavailable"
  }
}

function formatPpm(value: number): string {
  return `${(value / 10_000).toLocaleString("en-US", {
    maximumFractionDigits: 2,
  })}%`
}

function formatBasisPoints(value: string): string {
  try {
    const basisPoints = BigInt(value)
    const sign = basisPoints < 0n ? "−" : ""
    const absolute = basisPoints < 0n ? -basisPoints : basisPoints
    const whole = absolute / 100n
    const fraction = (absolute % 100n).toString().padStart(2, "0").replace(/0+$/, "")
    return `${sign}${whole.toLocaleString("en-US")}${fraction ? `.${fraction}` : ""}%`
  } catch {
    return "Unavailable"
  }
}

function money(value: Money): string {
  return `${value.amount} ${value.currency}`
}

function nullableMoney(value: Money | null): string {
  return value ? money(value) : "Not used for this action"
}

function priceRange(value: PriceRange): string {
  return `${money(value.lower)} to ${money(value.upper)}`
}

function nullablePriceRange(value: PriceRange | null): string {
  return value ? priceRange(value) : "Not used for this action"
}

function enumLabel(value: string): string {
  const label = value.replaceAll("_", " ")
  return `${label.charAt(0).toLocaleUpperCase()}${label.slice(1)}`
}

export function analysisOutcomeTone(
  kind: InvestmentAnalysisLocator["outcome"]["kind"],
): "good" | "attention" | "neutral" {
  return kind === "generated" ? "good" : kind === "no_action" ? "attention" : "neutral"
}

export function BriefLoading() {
  return (
    <div className="rounded-xl border border-border bg-card/40 p-6" aria-live="polite">
      <div className="flex items-center gap-3 text-sm text-muted-foreground">
        <FileText className="size-4" aria-hidden="true" />
        Loading the selected Investment Brief…
      </div>
    </div>
  )
}
