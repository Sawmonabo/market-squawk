import { DemandPanel } from "../shared/demand-panel"
import { useQuery } from "@tanstack/react-query"
import {
  Activity,
  BriefcaseBusiness,
  CircleAlert,
  Compass,
  Sparkles,
} from "lucide-react"
import { Link } from "react-router-dom"
import type { ReactNode } from "react"

import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { MarketCollection } from "@/features/markets/market-collection"
import { AnalysisLaunch } from "@/features/opportunities/analysis-launch"
import {
  parseInvestmentAnalysis,
  type InvestmentAnalysis,
  type InvestmentAnalysisLocator,
} from "@/features/opportunities/contracts"
import {
  EvidenceReliabilityDetails,
  PricePatternDetails,
  StudyQualificationDetails,
} from "@/features/opportunities/investment-brief"
import { formatMoney } from "@/lib/formatters"
import type { ProductTransport } from "@/lib/transport"
import { formatProductTimestamp } from "@/lib/time"

import { useOverviewQueries } from "./use-overview"

type OverviewQueries = ReturnType<typeof useOverviewQueries>

export function OverviewDashboard({
  transport,
  scope,
}: {
  transport: ProductTransport
  scope: ProductScope
}) {
  const queries = useOverviewQueries(transport, scope)

  return (
    <div className="space-y-5">
      <section className="rounded-xl border border-border bg-card/45 p-5" aria-labelledby="home-find-title">
        <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">Investment discovery</p>
        <h2 id="home-find-title" className="mt-1 text-lg font-semibold">Find opportunities</h2>
        <p className="mb-4 mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">
          Search the available investments using your saved analysis settings. Results include reasons,
          risks, price ranges, and a clear explanation when there is not enough evidence to act.
        </p>
        <AnalysisLaunch transport={transport} scope={scope} />
      </section>
      <DecisionSummary
        analyses={queries.analyses}
        transport={transport}
        scope={scope}
      />

      <section className="grid gap-4 lg:grid-cols-[minmax(0,1.25fr)_minmax(300px,0.75fr)]">
        <MarketCollection state={queries.markets} />
        <NextSteps />
      </section>
    </div>
  )
}

function DecisionSummary({
  analyses,
  transport,
  scope,
}: {
  analyses: OverviewQueries["analyses"]
  transport: ProductTransport
  scope: ProductScope
}) {
  return (
    <section
      className="rounded-xl border border-border bg-card/45 p-5"
      aria-labelledby="home-guidance-title"
    >
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="flex items-start gap-3">
          <span className="rounded-lg border border-border bg-background/70 p-2.5">
            <Sparkles className="size-4 text-primary" aria-hidden="true" />
          </span>
          <div>
            <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
              Saved investment guidance
            </p>
            <h2 id="home-guidance-title" className="mt-1 text-lg font-semibold">
              Decisions to review
            </h2>
            <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
              Review the action, time horizon, price ranges, reasons, risks, expiry,
              and uncertainty before deciding what to do.
            </p>
          </div>
        </div>
        <Button asChild size="sm" variant="outline">
          <Link to="/opportunities">Open all guidance</Link>
        </Button>
      </div>

      {analyses.status === "loading" ? (
        <div className="mt-5 grid gap-4 xl:grid-cols-2">
          <Skeleton className="h-80 rounded-xl" />
          <Skeleton className="h-80 rounded-xl" />
        </div>
      ) : analyses.status === "unavailable" ? (
        <UnavailableGuidance
          title="Investment guidance is unavailable"
          detail="No action should be taken from this page until the guidance can be read again."
        />
      ) : analyses.data.availableCount === 0 ? (
        <UnavailableGuidance
          title="No investment guidance is available yet"
          detail="Market Squawk has not produced a decision with enough evidence to review. Explore investments or start research before acting."
        />
      ) : (
        <div className="mt-5 grid gap-4 xl:grid-cols-2">
          {analyses.data.analyses.map((analysis) => (
            <DecisionCard
              key={analysis.actionToken}
              locator={analysis}
              transport={transport}
              scope={scope}
            />
          ))}
        </div>
      )}

      {analyses.status === "ready" && analyses.data.completeness === "truncated" ? (
        <p className="mt-4 text-[11px] leading-5 text-muted-foreground">
          Additional saved guidance is available in Opportunities.
        </p>
      ) : null}
    </section>
  )
}

function DecisionCard({
  locator,
  transport,
  scope,
}: {
  locator: InvestmentAnalysisLocator
  transport: ProductTransport
  scope: ProductScope
}) {
  const displayed = locator

  return (
    <article className="rounded-xl border border-border bg-background/35 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
            {displayed.portfolioLabel}
          </p>
          <h3 className="mt-1 truncate text-base font-semibold">
            {investmentName(displayed)}
          </h3>
        </div>
        <ActionBadge recommendation={displayed.recommendation} />
      </div>

      <p className="mt-3 text-xs leading-5 text-foreground/85">
        {displayed.recommendation.summary}
      </p>
      <dl className="mt-4 grid gap-3 border-t border-border/70 pt-3 sm:grid-cols-2">
        <Fact label="Horizon ends" value={<time dateTime={displayed.horizon.endsAt} title={displayed.horizon.endsAt}>{formatProductTimestamp(displayed.horizon.endsAt)}</time>} />
        <Fact label="Review by" value={<time dateTime={displayed.horizon.expiresAt} title={displayed.horizon.expiresAt}>{formatProductTimestamp(displayed.horizon.expiresAt)}</time>} />
      </dl>

      <DemandPanel title="Open supporting evidence" className="mt-4 rounded-lg border p-3">
        <DecisionEvidenceRead locator={locator} transport={transport} scope={scope} />
      </DemandPanel>
    </article>
  )
}

function DecisionEvidenceRead({ locator, transport, scope }: {
  locator: InvestmentAnalysisLocator
  transport: ProductTransport
  scope: ProductScope
}) {
  const analysis = useQuery({
    queryKey: productKeys.operation(
      scope,
      "decision",
      "Decision.GetInvestmentAnalysis",
      { actionToken: locator.actionToken },
    ),
    gcTime: 0,
    queryFn: async ({ signal }) =>
      parseInvestmentAnalysis(
        await transport.query({
          query: "decisionInvestmentAnalysis",
          actionToken: locator.actionToken,
        }, { signal }),
        locator.actionToken,
      ),
  })

  if (analysis.isPending) return <Skeleton className="mt-4 h-36 rounded-lg" />
  if (analysis.isError || !analysis.data) return <Alert className="mt-4"><CircleAlert aria-hidden="true" /><AlertTitle>Supporting detail is unavailable</AlertTitle><AlertDescription>Treat this saved action as incomplete until its ranges, reasons, risks, and uncertainty can be reviewed.</AlertDescription></Alert>
  return <DecisionEvidence analysis={analysis.data} />
}

function DecisionEvidence({ analysis }: { analysis: InvestmentAnalysis }) {
  return (
    <div className="mt-4 space-y-4">
      <PriceContext analysis={analysis} />
      <div className="grid gap-4 sm:grid-cols-2">
        <ProductList title="Why this guidance" values={analysis.reasons} />
        <ProductList
          title="What could go wrong"
          values={analysis.risks}
          empty="No specific risk explanation is available."
        />
      </div>
      <details className="rounded-lg border border-border bg-card/30 px-3 py-2.5">
        <summary className="cursor-pointer text-xs font-medium">
          Assumptions, invalidators, and uncertainty
        </summary>
        <div className="mt-3 grid gap-4 sm:grid-cols-2">
          <ProductList
            title="Assumptions"
            values={analysis.assumptions}
            empty="No additional assumptions were stated."
          />
          <ProductList
            title="What would invalidate it"
            values={analysis.invalidators}
            empty="No explicit invalidator is available."
          />
        </div>
        <dl className="mt-4 grid gap-3 border-t border-border/70 pt-3 sm:grid-cols-2">
          <Fact
            label="Evidence coverage"
            value={analysis.evidenceSummary.coverage.summary}
          />
          <Fact
            label="Out-of-sample evidence"
            value={<>
              <p>{analysis.evidenceSummary.outOfSample.summary}</p>
              {analysis.evidenceSummary.outOfSample.state === "available" ? <StudyQualificationDetails
                qualification={analysis.evidenceSummary.outOfSample.studyQualification} /> : null}
            </>}
          />
          <Fact
            label="Calibration"
            value={analysis.evidenceSummary.calibration.summary}
          />
          <Fact label="Costs" value={analysis.evidenceSummary.costs.summary} />
          <Fact
            label="Uncertainty"
            value={<EvidenceReliabilityDetails uncertainty={analysis.evidenceSummary.uncertainty} />}
          />
          <Fact label="Price patterns" value={<PricePatternDetails evidence={analysis.analyticalEvidence.pricePattern} />} />
          <Fact
            label="Historical test"
            value={analysis.evidenceSummary.historicalTest ? <>
              <p>{analysis.evidenceSummary.historicalTest.summary}</p>
              <StudyQualificationDetails qualification={analysis.evidenceSummary.historicalTest.studyQualification} />
            </> : "No suitable historical test is available."}
          />
          <Fact
            label="Information current through"
            value={<time dateTime={analysis.horizon.informationCurrentThrough} title={analysis.horizon.informationCurrentThrough}>{formatProductTimestamp(analysis.horizon.informationCurrentThrough)}</time>}
          />
        </dl>
      </details>
    </div>
  )
}

function PriceContext({ analysis }: { analysis: InvestmentAnalysis }) {
  const ranges = analysis.priceSummary.actionRanges
  const scenarios = analysis.priceSummary.scenarios
  return (
    <div className="rounded-lg border border-border bg-card/30 p-3">
      <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
        Price context
      </p>
      <dl className="mt-3 grid gap-3 sm:grid-cols-2">
        <Fact
          label="Current price"
          value={
            analysis.priceSummary.current
              ? formatMoney(analysis.priceSummary.current)
              : "Not available"
          }
        />
        <Fact
          label="Fair value"
          value={
            analysis.priceSummary.fairValue
              ? formatMoney(analysis.priceSummary.fairValue)
              : "Not available"
          }
        />
        {ranges ? (
          <>
            <Fact label="Buy range" value={formatRange(ranges.entry)} />
            <Fact label="Add range" value={formatRange(ranges.add)} />
            <Fact label="Trim range" value={formatRange(ranges.trim)} />
            <Fact label="Sell range" value={formatRange(ranges.exit)} />
          </>
        ) : (
          <Fact
            label="Action ranges"
            value="Not available for this guidance."
          />
        )}
        {scenarios ? (
          <>
            <Fact label="Downside range" value={formatRange(scenarios.downside)} />
            <Fact label="Expected range" value={formatRange(scenarios.base)} />
            <Fact label="Upside range" value={formatRange(scenarios.upside)} />
            <Fact
              label="Range horizon"
              value={<time dateTime={scenarios.endsAt} title={scenarios.endsAt}>{formatProductTimestamp(scenarios.endsAt)}</time>}
            />
          </>
        ) : null}
      </dl>
    </div>
  )
}

function NextSteps() {
  return (
    <section
      className="rounded-xl border border-border bg-card/45 p-5"
      aria-labelledby="home-next-steps-title"
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Before you act
          </p>
          <h2 id="home-next-steps-title" className="mt-1 text-base font-semibold">
            Complete the decision
          </h2>
        </div>
        <Compass className="size-5 text-primary" aria-hidden="true" />
      </div>
      <div className="mt-5 space-y-3">
        <NextStep
          icon={Sparkles}
          title="Review the full investment case"
          detail="Check ranges, reasons, risks, assumptions, expiry, and uncertainty together."
          path="/opportunities"
          linkLabel="Review guidance"
        />
        <NextStep
          icon={BriefcaseBusiness}
          title="Check portfolio impact"
          detail="Consider concentration, cash, downside, and account fit before making a decision."
          path="/portfolio"
          linkLabel="Open Portfolio"
        />
        <NextStep
          icon={Activity}
          title="Check the latest market context"
          detail="Confirm the current price, freshness, and available depth before acting."
          path="/markets"
          linkLabel="Open Markets"
        />
      </div>
    </section>
  )
}

function NextStep({
  icon: Icon,
  title,
  detail,
  path,
  linkLabel,
}: {
  icon: typeof Sparkles
  title: string
  detail: string
  path: string
  linkLabel: string
}) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-3">
      <div className="flex items-start gap-3">
        <Icon className="mt-0.5 size-4 shrink-0 text-primary" aria-hidden="true" />
        <div>
          <p className="text-xs font-medium">{title}</p>
          <p className="mt-1 text-[10px] leading-4 text-muted-foreground">{detail}</p>
          <Link
            className="mt-2 inline-flex text-[10px] font-medium text-primary underline-offset-4 hover:underline"
            to={path}
          >
            {linkLabel}
          </Link>
        </div>
      </div>
    </div>
  )
}

function UnavailableGuidance({ title, detail }: { title: string; detail: string }) {
  return (
    <Alert className="mt-5">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{detail}</AlertDescription>
    </Alert>
  )
}

function ActionBadge({
  recommendation,
}: {
  recommendation: InvestmentAnalysisLocator["recommendation"]
}) {
  const label =
    recommendation.kind === "action"
      ? `SAVED ${recommendation.action.toUpperCase()}`
      : recommendation.kind === "abstain"
        ? "ABSTAIN"
        : "UNAVAILABLE"
  const tone =
    recommendation.kind === "action"
      ? "border-primary/30 bg-primary/10 text-primary"
      : "border-amber-400/25 bg-amber-400/10 text-amber-200"
  return (
    <span className={`rounded-full border px-2.5 py-1 text-[10px] font-semibold ${tone}`}>
      {label}
    </span>
  )
}

function ProductList({
  title,
  values,
  empty = "Not available.",
}: {
  title: string
  values: string[]
  empty?: string
}) {
  return (
    <div>
      <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
        {title}
      </p>
      {values.length > 0 ? (
        <ul className="mt-2 space-y-1.5 text-[11px] leading-5 text-foreground/80">
          {values.slice(0, 4).map((value, index) => (
            <li key={`${index}:${value}`} className="flex gap-2">
              <span aria-hidden="true">•</span>
              <span>{value}</span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="mt-2 text-[11px] leading-5 text-muted-foreground">{empty}</p>
      )}
    </div>
  )
}

function Fact({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 text-[11px] leading-5 text-foreground/85">{value}</dd>
    </div>
  )
}

function investmentName(
  analysis: Pick<InvestmentAnalysisLocator, "investment">,
): string {
  if (analysis.investment.symbol && analysis.investment.name) {
    return `${analysis.investment.symbol} · ${analysis.investment.name}`
  }
  return analysis.investment.symbol ?? analysis.investment.name ?? "Investment"
}

function formatRange(range: {
  lower: { amount: string; currency: string }
  upper: { amount: string; currency: string }
}): string {
  return `${formatMoney(range.lower)} to ${formatMoney(range.upper)}`
}
