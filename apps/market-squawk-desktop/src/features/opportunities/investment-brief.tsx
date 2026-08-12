import { CircleAlert, FileText } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

import type {
  InvestmentAnalysis,
  InvestmentAnalysisLocator,
  InvestmentAnalysisResult,
} from "./contracts"
import { formatLosslessInteger, formatUnixNanos } from "./format"

type Evidence = InvestmentAnalysis["evidence"]
type Money = NonNullable<Evidence["market"]>["price"]
type PriceRange = NonNullable<Evidence["priceForecast"]>["ranges"]["base"]
type EvidenceWindow = NonNullable<Evidence["market"]>["window"]
type ContentIdentity = EvidenceWindow["contentIdentity"]

export function InvestmentBrief({ analysis }: { analysis: InvestmentAnalysis }) {
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
            Exact retained analysis
          </p>
          <h2 id="investment-brief-title" className="mt-1 text-xl font-semibold">
            Investment Brief
          </h2>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            This brief restates the stored policy, evidence, and outcome. It does not add a
            ranking, forecast a return, or turn the analysis into an order.
          </p>
        </div>
        <OutcomeBadge kind={analysis.result.kind} label={outcome.label} />
      </div>

      <div className="mt-5 rounded-lg border border-border bg-background/35 p-4">
        <p className="text-sm font-semibold">{outcome.title}</p>
        <p className="mt-1 text-sm leading-6 text-muted-foreground">{outcome.detail}</p>
        {analysis.result.kind === "no_action" ? (
          <p className="mt-3 text-xs leading-5 text-muted-foreground">
            Policy invalidator: {invalidatorLabel(analysis.result.invalidators[0]!)}
          </p>
        ) : null}
      </div>

      <dl className="mt-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Instrument record" value={analysis.evidence.instrumentId} mono />
        <Fact label="Account" value={analysis.evidence.accountId} mono />
        <Fact label="Reporting currency" value={analysis.evidence.currency} />
        <Fact label="Evidence as of" value={formatUnixNanos(analysis.evidence.asOf)} />
        <Fact label="Analysis horizon" value={formatUnixNanos(analysis.result.horizonAt)} />
        <Fact label="Brief expires" value={formatUnixNanos(analysis.result.expiresAt)} />
        <Fact
          label="Evidence reliability"
          value={
            reliability
              ? `${reliability.valuePpm.toLocaleString("en-US")} ppm`
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
            The retained reason and any partial or mismatched evidence are shown below. Missing
            evidence is not filled in by the desktop.
          </AlertDescription>
        </Alert>
      ) : null}

      <PolicyDetails policy={analysis.policy} />
      {reliability ? <ReliabilityDetails reliability={reliability} /> : null}
      <EvidenceDetails evidence={analysis.evidence} />
      <IdentityDetails analysis={analysis} />
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
      <Disclosure title="Price cases and all seven retained ranges">
        <p className="text-xs leading-5 text-muted-foreground">
          These are exact backend-supplied prices. The desktop does not calculate gains, losses,
          or percentages from them.
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

function PolicyDetails({ policy }: { policy: InvestmentAnalysis["policy"] }) {
  return (
    <Disclosure title="Policy, assumptions, and limits">
      <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Fact label="Policy version" value={policy.version.toLocaleString("en-US")} />
        <Fact
          label="Action-zone version"
          value={policy.actionZoneSemanticsVersion.toLocaleString("en-US")}
        />
        <Fact
          label="Policy horizon (nanoseconds)"
          value={formatLosslessInteger(policy.horizonNanos)}
        />
        <Fact
          label="Brief lifetime (nanoseconds)"
          value={formatLosslessInteger(policy.proposalLifetimeNanos)}
        />
        <Fact label="Policy digest" value={policy.digest} mono />
      </dl>
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
    <Disclosure title="Evidence reliability components">
      <p className="text-xs leading-5 text-muted-foreground">
        This is policy-weighted evidence reliability in parts per million. It is not a
        probability of profit or a predicted return.
      </p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-2">
        <Fact label="Meaning" value={enumLabel(reliability.meaning)} />
        <Fact
          label="Aggregate reliability"
          value={`${reliability.valuePpm.toLocaleString("en-US")} ppm`}
        />
      </dl>
      <div className="mt-4 overflow-x-auto">
        <table className="w-full min-w-[560px] border-collapse text-left text-xs">
          <thead className="text-muted-foreground">
            <tr className="border-b border-border">
              <th className="px-2 py-2 font-medium">Component</th>
              <th className="px-2 py-2 font-medium">Reliability</th>
              <th className="px-2 py-2 font-medium">Policy weight</th>
            </tr>
          </thead>
          <tbody>
            {reliability.components.map((component) => (
              <tr key={component.kind} className="border-b border-border/70 last:border-0">
                <td className="px-2 py-3">{enumLabel(component.kind)}</td>
                <td className="px-2 py-3 font-mono">
                  {component.valuePpm.toLocaleString("en-US")} ppm
                </td>
                <td className="px-2 py-3 font-mono">
                  {component.weightPpm.toLocaleString("en-US")} ppm
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
    <Disclosure title="Retained evidence">
      <p className="text-xs leading-5 text-muted-foreground">
        Each section shows only evidence retained by the service. An unavailable analysis may
        intentionally retain absent or mismatched fields to explain why it could not proceed.
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
        <IdentityFact label="Selection receipt" identity={evidence.selectionReceiptIdentity} />
        <IdentityFact
          label="Selected observation"
          identity={evidence.selectedObservationIdentity}
        />
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
            <IdentityFact
              label="Expected terminal statistic binding"
              identity={expected.statisticIdentity}
            />
          </>
        ) : (
          <Fact
            label="Expected terminal value"
            value="Unavailable — no admitted conditional mean"
          />
        )}
        <Fact label="Vintage identity" value={evidence.vintageId} mono />
        <Fact label="Downside case" value={money(evidence.cases.downside)} />
        <Fact label="Base case" value={money(evidence.cases.base)} />
        <Fact label="Upside case" value={money(evidence.cases.upside)} />
        <Fact label="Downside range" value={priceRange(evidence.ranges.downside)} />
        <Fact label="Base range" value={priceRange(evidence.ranges.base)} />
        <Fact label="Upside range" value={priceRange(evidence.ranges.upside)} />
        <Fact
          label="Nominal coverage"
          value={`${evidence.calibration.nominalCoveragePpm.toLocaleString("en-US")} ppm`}
        />
        <Fact
          label="Realized coverage"
          value={`${evidence.calibration.realizedCoveragePpm.toLocaleString("en-US")} ppm`}
        />
        <Fact
          label="Completed outcomes"
          value={evidence.calibration.completedOutcomes.toLocaleString("en-US")}
        />
        <IdentityFact label="Output binding" identity={evidence.outputBindingIdentity} />
        <IdentityFact label="Calibration" identity={evidence.calibrationIdentity} />
        <IdentityFact label="Outcome set" identity={evidence.outcomeSetIdentity} />
      </dl>
      <p className="mt-4 text-xs leading-5 text-muted-foreground">
        {expected
          ? "This admitted conditional mean authorizes exact backend expected-return calculations. It is not a probability, a guaranteed profit, or a net-profit estimate."
          : "Expected return is unavailable: calibration coverage intervals do not establish an admitted conditional mean."}
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
        <Fact label="Measurement identity" value={evidence.measurementId} mono />
        <Fact
          label="Classification decision"
          value={evidence.classificationDecisionId}
          mono
        />
        <Fact
          label="Selection receipt hash"
          value={evidence.selectionReceiptHash}
          mono
        />
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
          label="Outcome horizon (nanoseconds)"
          value={formatLosslessInteger(evidence.outcomeHorizonNanos)}
        />
        <Fact
          label="Supplied net return (basis points)"
          value={formatLosslessInteger(evidence.netReturnBasisPoints)}
        />
        <Fact
          label="Supplied maximum drawdown (basis points)"
          value={formatLosslessInteger(evidence.maxDrawdownBasisPoints)}
        />
        <Fact
          label="Fee assumption (basis points)"
          value={formatLosslessInteger(evidence.feeBasisPoints)}
        />
        <Fact
          label="Slippage assumption (basis points)"
          value={formatLosslessInteger(evidence.slippageBasisPoints)}
        />
        <Fact
          label="Maximum random slippage (basis points)"
          value={formatLosslessInteger(evidence.maximumRandomSlippageBasisPoints)}
        />
        <Fact label="Observations" value={evidence.observations.toLocaleString("en-US")} />
        <Fact label="Trials" value={evidence.trials.toLocaleString("en-US")} />
        <Fact
          label="Stability"
          value={`${evidence.stabilityPpm.toLocaleString("en-US")} ppm`}
        />
        <Fact
          label="Simulation cutoff"
          value={formatUnixNanos(evidence.simulationCutoffAt)}
        />
        <IdentityFact label="Dataset" identity={evidence.datasetIdentity} />
        <IdentityFact label="Command" identity={evidence.commandIdentity} />
        <IdentityFact label="Terminal result" identity={evidence.terminalIdentity} />
        <IdentityFact label="Report" identity={evidence.reportIdentity} />
        <IdentityFact label="Cohort" identity={evidence.cohortIdentity} />
        <IdentityFact label="Cost model" identity={evidence.costModelIdentity} />
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
          label="Quoted spread (basis points)"
          value={formatLosslessInteger(evidence.quotedSpreadBasisPoints)}
        />
        <Fact
          label="Capacity"
          value={`${evidence.capacityPpm.toLocaleString("en-US")} ppm`}
        />
        <Fact label="Quality" value={enumLabel(evidence.quality)} />
        <IdentityFact label="Assessment" identity={evidence.assessmentIdentity} />
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
      ? "No retained position"
      : [
          "Position retained",
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
        <Fact label="Portfolio revision" value={evidence.portfolioRevision} mono />
        <Fact label="Position state" value={position} />
        <Fact
          label="Risk capacity"
          value={`${evidence.riskCapacityPpm.toLocaleString("en-US")} ppm`}
        />
        <IdentityFact label="Risk report" identity={evidence.riskReportIdentity} />
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
      <IdentityFact label="Content" identity={window.contentIdentity} />
    </dl>
  )
}

function IdentityDetails({ analysis }: { analysis: InvestmentAnalysis }) {
  return (
    <Disclosure title="Exact analysis identities">
      <dl className="grid gap-4 sm:grid-cols-2">
        <Fact label="Analysis identity" value={analysis.analysisId} mono />
        <Fact label="Policy digest" value={analysis.policy.digest} mono />
        <Fact label="Evidence digest" value={analysis.evidenceDigest} mono />
        {analysis.result.kind !== "unavailable" ? (
          <>
            <Fact label="Proposal identity" value={analysis.result.proposalId} mono />
            <Fact
              label="Derivation digest"
              value={analysis.result.derivationDigest}
              mono
            />
          </>
        ) : null}
      </dl>
    </Disclosure>
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
        {title} · {present ? "Retained" : "Not retained"}
      </summary>
      {present ? (
        <div className="mt-4">{children}</div>
      ) : (
        <p className="mt-3 text-xs text-muted-foreground">
          No {title.toLocaleLowerCase()} evidence is present in this analysis.
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

function IdentityFact({ label, identity }: { label: string; identity: ContentIdentity }) {
  return (
    <Fact
      label={`${label} identity`}
      value={`${identity.algorithm}: ${identity.digest}`}
      mono
    />
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
          "The service retained a research action, price ladder, action boundaries, and " +
          "evidence reliability under the recorded policy.",
      }
    case "no_action":
      return {
        label: "No action",
        title: "The policy produced no action",
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
      "The retained forecast and valuation did not support one policy-consistent action.",
    backtest_below_policy: "The retained backtest evidence did not meet policy.",
    liquidity_below_policy: "The retained liquidity evidence did not meet policy.",
    portfolio_risk_below_policy:
      "The retained account-specific portfolio risk evidence did not meet policy.",
    evidence_reliability_below_policy:
      "The policy-weighted evidence reliability did not meet policy.",
    position_state_not_actionable:
      "The retained position state did not admit an action under policy.",
    generated_price_order_collapsed:
      "The generated price ordering could not preserve the required action zones.",
  }[value]
}

function invalidatorLabel(
  value: Extract<InvestmentAnalysisResult, { kind: "no_action" }>["invalidators"][number],
): string {
  return {
    forecast_valuation_conflict: "Forecast and valuation conflict",
    backtest_policy_breach: "Backtest policy breach",
    liquidity_policy_breach: "Liquidity policy breach",
    portfolio_risk_policy_breach: "Portfolio-risk policy breach",
    evidence_reliability_policy_breach: "Evidence-reliability policy breach",
    position_state_incompatible: "Position state incompatible",
    generated_price_order_collapsed: "Generated price order collapsed",
  }[value]
}

function unavailableReasonLabel(
  reason: Extract<InvestmentAnalysisResult, { kind: "unavailable" }>["reason"],
): string {
  switch (reason.kind) {
    case "missing_evidence":
      return `${evidenceKindLabel(reason.evidence)} evidence is missing.`
    case "instrument_mismatch":
      return (
        `${evidenceKindLabel(reason.evidence)} evidence names instrument ` +
        `${reason.actual}, not expected instrument ${reason.expected}.`
      )
    case "currency_mismatch":
      return (
        `${evidenceKindLabel(reason.evidence)} evidence uses ${reason.actual}, ` +
        `not expected currency ${reason.expected}.`
      )
    case "account_mismatch":
      return (
        `Portfolio evidence names account ${reason.actual}, ` +
        `not expected account ${reason.expected}.`
      )
    case "not_available_at_cutoff":
      return (
        `${evidenceKindLabel(reason.evidence)} evidence was not available ` +
        "at the analysis cutoff."
      )
    case "expired_evidence":
      return `${evidenceKindLabel(reason.evidence)} evidence had expired.`
    case "stale_evidence":
      return `${evidenceKindLabel(reason.evidence)} evidence was too old for policy.`
    case "rejected_quality":
      return (
        `${evidenceKindLabel(reason.evidence)} evidence had rejected quality: ` +
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
        `The backtest horizon was ${formatLosslessInteger(reason.actualNanos)} ` +
        `nanoseconds; policy expected ${formatLosslessInteger(reason.expectedNanos)}.`
      )
    case "insufficient_forecast_outcomes":
      return (
        `The forecast retained ${reason.actual.toLocaleString("en-US")} completed ` +
        `outcomes; policy required ${reason.required.toLocaleString("en-US")}.`
      )
    case "unsupported_forecast_coverage":
      return (
        `Forecast coverage was ${reason.actualPpm.toLocaleString("en-US")} ppm; ` +
        `policy admitted ${reason.minimumPpm.toLocaleString("en-US")}–` +
        `${reason.maximumPpm.toLocaleString("en-US")} ppm.`
      )
    case "insufficient_backtest_observations":
      return (
        `The backtest retained ${reason.actual.toLocaleString("en-US")} ` +
        `observations; policy required ${reason.required.toLocaleString("en-US")}.`
      )
    case "insufficient_backtest_trials":
      return (
        `The backtest retained ${reason.actual.toLocaleString("en-US")} trials; ` +
        `policy required ${reason.required.toLocaleString("en-US")}.`
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
        Loading the exact retained Investment Brief…
      </div>
    </div>
  )
}
