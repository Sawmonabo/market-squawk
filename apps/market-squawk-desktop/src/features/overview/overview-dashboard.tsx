import * as React from "react"
import {
  Activity,
  BriefcaseBusiness,
  CircleAlert,
  Clock3,
  Database,
  Gauge,
  ListChecks,
  ServerCog,
  ShieldAlert,
  Sparkles,
} from "lucide-react"
import { Link } from "react-router-dom"

import type { ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import type { InvestmentAnalysisPage } from "@/features/opportunities/contracts"
import {
  formatPercent,
  formatTimestamp,
} from "@/features/portfolio/portfolio-format"
import type {
  PortfolioAccount,
  PortfolioExposure,
  PortfolioPerformance,
  PortfolioRisk,
} from "@/features/portfolio/portfolio-contracts"
import {
  usePortfolioAccounts,
  usePortfolioDetails,
} from "@/features/portfolio/use-portfolio"
import { formatMoney, humanize } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import type { MarketSnapshot, OverviewJob } from "./schemas"
import { type ReadState, useOverviewQueries } from "./use-overview"

type OverviewQueries = ReturnType<typeof useOverviewQueries>
type PortfolioAccountsRead = ReturnType<typeof usePortfolioAccounts>

export function OverviewDashboard({
  transport,
  scope,
  bootstrap,
}: {
  transport: ProductTransport
  scope: ProductScope
  bootstrap: DesktopBootstrap
}) {
  const queries = useOverviewQueries(transport, scope)
  const accounts = usePortfolioAccounts(transport, bootstrap)
  const analysisJobs =
    queries.jobs.status === "ready"
      ? queries.jobs.data.jobs.filter(
          (job) => !isTerminal(job.state) && isAnalysisJob(job.kind),
        )
      : []

  return (
    <div className="space-y-4">
      <section
        aria-label="Home summary"
        className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3"
      >
        <StatusCard
          icon={Activity}
          label="Live markets"
          value={countValue(queries.markets, "markets")}
          state={queries.markets.status}
          detail={
            queries.markets.status === "ready"
              ? freshMarketDetail(queries.markets.data)
              : "Current market information is unavailable."
          }
        />
        <StatusCard
          icon={Gauge}
          label="Running analyses"
          value={
            queries.jobs.status === "ready"
              ? analysisJobs.length.toLocaleString()
              : "Unavailable"
          }
          state={queries.jobs.status}
          detail={
            queries.jobs.status === "ready"
              ? queries.jobs.data.next
                ? "More active work may be available in Operations & Jobs."
                : "Current research, forecasting, and decision work."
              : "Analysis status is unavailable."
          }
        />
        <StatusCard
          icon={Sparkles}
          label="Retained analyses"
          value={analysisCount(queries.analyses)}
          state={queries.analyses.status}
          detail={
            queries.analyses.status === "ready"
              ? "Saved analyses ready for review."
              : "Saved analyses are unavailable."
          }
        />
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.25fr)_minmax(340px,0.75fr)]">
        <PortfolioSummary
          transport={transport}
          scope={scope}
          bootstrap={bootstrap}
          accounts={accounts}
        />
        <RecommendationSummary analyses={queries.analyses} />
      </section>

      <section className="grid gap-4 xl:grid-cols-2">
        <RunningAnalysisPanel jobs={queries.jobs} analysisJobs={analysisJobs} />
        <SetupGuidance
          accounts={accounts}
          markets={queries.markets}
          analyses={queries.analyses}
        />
      </section>

      <section aria-labelledby="home-evidence-title" className="space-y-3 pt-1">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Current context
          </p>
          <h2 id="home-evidence-title" className="mt-1 text-base font-semibold">
            Market context
          </h2>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            A compact view of current market availability. Open Markets for the full workspace.
          </p>
        </div>
        <LiveMarketPanel markets={queries.markets} />
      </section>
    </div>
  )
}

function PortfolioSummary({
  transport,
  scope,
  bootstrap,
  accounts,
}: {
  transport: ProductTransport
  scope: ProductScope
  bootstrap: DesktopBootstrap
  accounts: PortfolioAccountsRead
}) {
  const accountRows =
    accounts.query.data?.pages.flatMap((page) => page.value) ?? []
  const [selectedAccountId, setSelectedAccountId] = React.useState("")
  const account =
    accountRows.find(
      (candidate) => candidate.accountId === selectedAccountId,
    ) ?? null
  const details = usePortfolioDetails(
    transport,
    scope,
    bootstrap,
    account?.accountId ?? null,
  )

  if (!accounts.available) {
    return (
      <PortfolioUnavailable
        title="Portfolio summary is unavailable"
        detail="Portfolio accounts are unavailable right now."
      />
    )
  }
  if (accounts.query.isPending) {
    return <Skeleton className="h-80 rounded-xl" />
  }
  if (accounts.query.isError) {
    return (
      <PortfolioUnavailable
        title="Portfolio accounts could not be read"
        detail={
          "Portfolio accounts could not be loaded. Try again or review Logs & Diagnostics."
        }
      />
    )
  }
  if (accountRows.length === 0) {
    return (
      <PortfolioUnavailable
        title="No portfolio account is loaded"
        detail="Add an account to see its value, cash, performance, exposure, and risk."
      />
    )
  }

  return (
    <section
      className="rounded-xl border border-border bg-card/45 p-5"
      aria-labelledby="portfolio-summary-title"
    >
      <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
        <div className="flex items-start gap-3">
          <span className="rounded-lg border border-border bg-background/70 p-2.5">
            <BriefcaseBusiness
              className="size-4 text-primary"
              aria-hidden="true"
            />
          </span>
          <div>
            <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
              Financial position
            </p>
            <h2 id="portfolio-summary-title" className="mt-1 text-base font-semibold">
              Your account at a glance
            </h2>
            <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
              Choose an account to review its current financial position.
            </p>
          </div>
        </div>
        <Button asChild variant="outline" size="sm">
          <Link to="/portfolio">Open Portfolio</Link>
        </Button>
      </div>

      <label className="mt-4 block max-w-xl text-xs font-medium">
        Account to summarize
        <select
          className="mt-2 h-9 w-full rounded-md border border-input bg-background px-3 font-mono text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring"
          value={account?.accountId ?? ""}
          onChange={(event) => setSelectedAccountId(event.target.value)}
        >
          <option value="">Choose an account</option>
          {accountRows.map((candidate) => (
            <option key={candidate.accountId} value={candidate.accountId}>
              {candidate.accountId} · {candidate.currency.toUpperCase()}
            </option>
          ))}
        </select>
      </label>
      {accounts.query.hasNextPage ? (
        <p className="mt-2 text-[10px] leading-4 text-muted-foreground">
          Open Portfolio to review more accounts.
        </p>
      ) : null}

      {account === null ? (
        <div className="mt-4 rounded-lg border border-dashed border-border p-5">
          <p className="text-xs font-medium">Choose before values are read</p>
          <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
            Choose an account to view its summary.
          </p>
        </div>
      ) : (
        <SelectedPortfolioSummary account={account} details={details} />
      )}
    </section>
  )
}

function SelectedPortfolioSummary({
  account,
  details,
}: {
  account: PortfolioAccount
  details: ReturnType<typeof usePortfolioDetails>
}) {
  const holdings = details.holdings.data?.value ?? null
  const performance = details.performance.data?.value ?? null
  const exposure = details.exposure.data?.value ?? null
  const risk = details.risk.data?.value ?? null
  const expectedRevisionId = account.currentRevision.revisionId
  const changedWhileReading =
    holdings?.some(
      (holding) =>
        holding.account_id !== account.accountId ||
        holding.revisionId !== expectedRevisionId,
    ) === true ||
    [performance, exposure, risk].some(
      (detail) =>
        detail !== null &&
        (detail.accountId !== account.accountId ||
          detail.revisionId !== expectedRevisionId),
    )

  if (changedWhileReading) {
    return (
      <Alert className="mt-4">
        <CircleAlert aria-hidden="true" />
        <AlertTitle>Portfolio changed while Home was reading it</AlertTitle>
        <AlertDescription>
          The account changed while this page was loading. Refresh to view one consistent update.
        </AlertDescription>
      </Alert>
    )
  }

  const failures = [
    details.holdings.error,
    details.performance.error,
    details.exposure.error,
    details.risk.error,
  ].filter((value): value is Error => value instanceof Error)
  const currentValue = factFromPerformance(
    performance,
    details.performance.isPending,
  )
  const reportedCash = performance?.accountingEvidence?.cash.amount ?? null

  return (
    <div className="mt-4">
      {failures.length > 0 ? (
        <p className="mb-3 rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-[11px] leading-5 text-destructive">
          Some portfolio details could not be loaded. Try refreshing this page. If the problem
          continues, review Logs &amp; Diagnostics.
        </p>
      ) : null}

      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <TruthFact
          label="Current value"
          value={currentValue.value}
          detail={currentValue.detail}
        />
        <TruthFact
          label="Recent performance"
          value="Not available"
          detail={
            performance?.timeWeightedReturn === undefined
              ? "No comparable performance result was returned."
              : "A comparable recent period is not available for this account."
          }
        />
        <TruthFact
          label="Available cash"
          value="Not available"
          detail={
            reportedCash
              ? `The account reports ${formatMoney(reportedCash)}, but settlement availability and reservations are not included.`
              : "No cash balance is available for this account."
          }
        />
        <TruthFact
          label="Major risk alerts"
          value="Not available"
          detail={
            risk
              ? "The current risk read supplies historical measures, not alert severity, active limit breaches, or an all-clear state."
              : "No current risk-alert status is available."
          }
        />
      </div>

      <details className="mt-4 rounded-lg border border-border bg-background/35 px-4 py-3">
        <summary className="cursor-pointer text-xs font-medium">
          Additional account evidence
        </summary>
        <p className="mt-2 text-[10px] leading-4 text-muted-foreground">
          Additional values currently available for this account.
        </p>
        <dl className="mt-3 grid gap-3 text-[10px] sm:grid-cols-2 xl:grid-cols-4">
          <EvidenceFact
            label="Reported cash"
            value={reportedCash ? formatMoney(reportedCash) : "Not returned"}
          />
          <EvidenceFact
            label="Available-history return"
            value={formatAvailableHistoryReturn(performance)}
          />
          <EvidenceFact
            label="Net exposure"
            value={formatExposure(exposure)}
          />
          <EvidenceFact
            label="Historical value at risk"
            value={formatHistoricalValueAtRisk(risk)}
          />
          <EvidenceFact
            label="Reporting currency"
            value={account.currency.toUpperCase()}
          />
          <EvidenceFact
            label="Holdings"
            value={account.holdingCount.toLocaleString()}
          />
          <EvidenceFact
            label="Last updated"
            value={formatTimestamp(account.currentRevision.effectiveAtUnixNanos)}
          />
        </dl>
      </details>
    </div>
  )
}

function RecommendationSummary({
  analyses,
}: {
  analyses: OverviewQueries["analyses"]
}) {
  return (
    <section
      className="rounded-xl border border-border bg-card/45 p-5"
      aria-labelledby="recommendation-summary-title"
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Recommendations
          </p>
          <h2 id="recommendation-summary-title" className="mt-1 text-base font-semibold">
            What may need attention
          </h2>
        </div>
        <Sparkles className="size-5 text-primary" aria-hidden="true" />
      </div>

      {analyses.status === "loading" ? (
        <div className="mt-5 space-y-3">
          <Skeleton className="h-16 rounded-lg" />
          <Skeleton className="h-16 rounded-lg" />
        </div>
      ) : analyses.status === "unavailable" ? (
        <Alert className="mt-5">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Saved analyses are unavailable</AlertTitle>
          <AlertDescription>
            Try again or review Logs &amp; Diagnostics for details.
          </AlertDescription>
        </Alert>
      ) : analyses.data.availableCount === 0 ? (
        <div className="mt-5 rounded-lg border border-dashed border-border p-5">
          <p className="text-xs font-medium">No saved investment analysis yet</p>
          <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
            Run an analysis to see recommendations and supporting evidence here.
          </p>
        </div>
      ) : (
        <RecommendationFacts page={analyses.data} />
      )}

      <div className="mt-4 flex flex-wrap gap-2">
        <Button asChild size="sm" variant="outline">
          <Link to="/opportunities">Open Opportunities</Link>
        </Button>
      </div>
    </section>
  )
}

function RecommendationFacts({ page }: { page: InvestmentAnalysisPage }) {
  const generatedInPage = page.analyses.filter(
    (analysis) => analysis.outcome.kind === "generated",
  )
  const heldActionsInPage = generatedInPage.filter(
    (analysis) =>
      analysis.outcome.kind === "generated" &&
      analysis.outcome.action !== "buy",
  )

  return (
    <div className="mt-5 space-y-3">
      <QueueItem
        label="Retained analyses"
        value={page.availableCount.toLocaleString()}
        detail={`${generatedInPage.length.toLocaleString()} completed analyses and ${heldActionsInPage.length.toLocaleString()} non-Buy actions are ready to review.`}
      />
      <QueueItem
        label="Strongest current opportunities"
        value="Not available"
        detail="No ranked opportunity set is available yet."
      />
      <QueueItem
        label="Current held-position actions"
        value="Not available"
        detail="No current position guidance is available yet."
      />
      <QueueItem
        label="Changed, expired, or invalidated"
        value="Not available"
        detail="No current changes or expired recommendations are available yet."
      />
      {page.completeness === "truncated" ? (
        <p className="rounded-lg border border-border bg-background/35 p-3 text-[10px] leading-4 text-muted-foreground">
          More saved analyses are available in Opportunities.
        </p>
      ) : null}
    </div>
  )
}

function RunningAnalysisPanel({
  jobs,
  analysisJobs,
}: {
  jobs: OverviewQueries["jobs"]
  analysisJobs: OverviewJob[]
}) {
  return (
    <section
      className="rounded-xl border border-border bg-card/45 p-5"
      aria-labelledby="running-analysis-title"
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Work in progress
          </p>
          <h2 id="running-analysis-title" className="mt-1 text-base font-semibold">
            Running analyses
          </h2>
        </div>
        <Clock3 className="size-5 text-primary" aria-hidden="true" />
      </div>

      {jobs.status === "loading" ? (
        <Skeleton className="mt-5 h-32 rounded-lg" />
      ) : jobs.status === "unavailable" ? (
        <Alert className="mt-5">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Job status is unavailable</AlertTitle>
          <AlertDescription>
            Try again or review Logs &amp; Diagnostics for details.
          </AlertDescription>
        </Alert>
      ) : analysisJobs.length === 0 ? (
        <div className="mt-5 rounded-lg border border-dashed border-border p-5">
          <p className="text-xs font-medium">
            No analysis is running
          </p>
          <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
            Start an analysis when you are ready to research an investment.
          </p>
        </div>
      ) : (
        <ul className="mt-5 divide-y divide-border">
          {analysisJobs.slice(0, 4).map((job) => (
            <li key={job.jobId} className="py-3 first:pt-0 last:pb-0">
              <div className="flex items-baseline justify-between gap-3">
                <p className="truncate text-xs font-medium" title={job.kind}>
                  {humanize(job.kind)}
                </p>
                <span className="font-mono text-[9px] uppercase tracking-wider text-primary">
                  {humanize(job.state)}
                </span>
              </div>
              <p className="mt-1 text-[10px] leading-4 text-muted-foreground">
                {job.phase ? humanize(job.phase) : "No phase reported"}
                {job.completedUnits !== null && job.completedUnits !== undefined
                  ? ` · ${job.completedUnits.toLocaleString()} completed${job.totalUnits !== null && job.totalUnits !== undefined ? ` of ${job.totalUnits.toLocaleString()}` : ""}`
                  : ""}
              </p>
            </li>
          ))}
        </ul>
      )}

      <Button asChild className="mt-4" size="sm" variant="outline">
        <Link to="/system/operations-jobs">Open Operations &amp; Jobs</Link>
      </Button>
    </section>
  )
}

function SetupGuidance({
  accounts,
  markets,
  analyses,
}: {
  accounts: PortfolioAccountsRead
  markets: OverviewQueries["markets"]
  analyses: OverviewQueries["analyses"]
}) {
  const accountCount =
    accounts.query.data?.pages.reduce(
      (count, page) => count + page.value.length,
      0,
    ) ?? 0
  return (
    <section
      className="rounded-xl border border-border bg-card/45 p-5"
      aria-labelledby="setup-guidance-title"
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Readiness
          </p>
          <h2 id="setup-guidance-title" className="mt-1 text-base font-semibold">
            What to set up next
          </h2>
        </div>
        <ListChecks className="size-5 text-primary" aria-hidden="true" />
      </div>

      <div className="mt-5 space-y-3">
        <GuidanceItem
          icon={Sparkles}
          title="Automatic opportunity search is not available yet"
          detail="Opportunity search is still being completed. You can review saved analyses now."
          path="/opportunities"
          linkLabel="Review retained analyses"
        />
        {accounts.query.isPending ? (
          <GuidanceItem
            icon={BriefcaseBusiness}
            title="Checking portfolio accounts"
            detail="Checking your connected accounts."
            path="/portfolio"
            linkLabel="Open Portfolio"
          />
        ) : !accounts.available || accounts.query.isError || accountCount === 0 ? (
          <GuidanceItem
            icon={BriefcaseBusiness}
            title="Add a portfolio account"
            detail="Add an account and choose its currency before requesting portfolio-aware recommendations."
            path="/portfolio"
            linkLabel="Open Portfolio"
          />
        ) : (
          <GuidanceItem
            icon={BriefcaseBusiness}
            title="Review your recommendation account"
            detail="Confirm which account and investment preferences recommendations should use."
            path="/portfolio"
            linkLabel="Open Portfolio setup"
          />
        )}
        {markets.status === "unavailable" ||
        (markets.status === "ready" && (markets.data?.length ?? 0) === 0) ? (
          <GuidanceItem
            icon={ServerCog}
            title="Market data needs attention"
            detail={
              markets.status === "unavailable"
                ? "Current market information is unavailable. Review your connections to restore coverage."
                : "No current market information is available. Connect a data service to continue."
            }
            path="/connections/sources"
            linkLabel="Open Connections & Sources"
          />
        ) : null}
        {analyses.status === "unavailable" ? (
          <GuidanceItem
            icon={ShieldAlert}
            title="Investment-analysis history needs repair"
            detail="Saved analyses are unavailable. Try again or review Logs & Diagnostics."
            path="/opportunities"
            linkLabel="Open Opportunities"
          />
        ) : null}
      </div>
    </section>
  )
}

function GuidanceItem({
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
        <Icon className="mt-0.5 size-4 shrink-0 text-amber-300" aria-hidden="true" />
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

function PortfolioUnavailable({
  title,
  detail,
}: {
  title: string
  detail: string
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <div className="flex items-start gap-3">
        <BriefcaseBusiness
          className="mt-0.5 size-4 text-primary"
          aria-hidden="true"
        />
        <div>
          <h2 className="text-sm font-semibold">{title}</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
            {detail}
          </p>
          <Button asChild className="mt-3" size="sm">
            <Link to="/portfolio">Open Portfolio</Link>
          </Button>
        </div>
      </div>
    </section>
  )
}

function LiveMarketPanel({
  markets,
}: {
  markets: OverviewQueries["markets"]
}) {
  return (
    <EvidencePanel
      title="Live market truth"
      icon={Activity}
      state={markets.status}
    >
      {markets.status === "ready" && markets.data && markets.data.length > 0 ? (
        <ul className="divide-y divide-border">
          {markets.data.slice(0, 5).map((stream, index) => (
            <li
              key={`${stream.instrumentId}:${stream.venueId}:${index}`}
              className="flex items-center gap-3 py-3 first:pt-0 last:pb-0"
            >
              <span
                className={`size-2 rounded-full ${stream.freshAtReference ? "bg-[var(--success)]" : "bg-[var(--warning)]"}`}
                aria-hidden="true"
              />
              <span className="min-w-0 flex-1">
                <span className="block truncate text-xs font-medium">
                  {stream.instrumentId}
                </span>
                <span className="mt-0.5 block truncate font-mono text-[9px] text-muted-foreground">
                  {stream.venueId}
                </span>
              </span>
              <span className="text-right text-[10px] text-muted-foreground">
                <span className="block">
                  {humanize(stream.currentDisplayQuality)}
                </span>
                <span className="block">{humanize(stream.tradingStatus)}</span>
              </span>
            </li>
          ))}
        </ul>
      ) : null}
    </EvidencePanel>
  )
}

function EvidencePanel({
  title,
  icon: Icon,
  state,
  children,
}: {
  title: string
  icon: typeof Activity
  state: ReadState<unknown>["status"]
  children: React.ReactNode
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <div className="mb-4 flex items-center gap-2">
        <Icon className="size-4 text-primary" aria-hidden="true" />
        <h3 className="text-sm font-semibold">{title}</h3>
      </div>
      {state === "loading" ? <Skeleton className="h-28 rounded-lg" /> : null}
      {state === "unavailable" ? (
        <div className="rounded-lg border border-border bg-background/35 p-4">
          <p className="text-xs font-medium">Not available right now</p>
          <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
            Try again or review Logs &amp; Diagnostics for details.
          </p>
        </div>
      ) : null}
      {state === "ready" && !children ? (
        <p className="rounded-lg border border-dashed border-border p-5 text-center text-xs text-muted-foreground">
          No current records were returned.
        </p>
      ) : (
        children
      )}
    </section>
  )
}

function StatusCard({
  icon: Icon,
  label,
  value,
  detail,
  state,
}: {
  icon: typeof Database
  label: string
  value: string
  detail: string | null
  state: ReadState<unknown>["status"]
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-4">
      <div className="flex items-center justify-between">
        <Icon className="size-4 text-primary" aria-hidden="true" />
        <span
          className={`size-2 rounded-full ${state === "ready" ? "bg-[var(--success)]" : state === "loading" ? "animate-pulse bg-primary" : "bg-[var(--warning)]"}`}
          aria-label={state}
        />
      </div>
      <p className="mt-4 font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 text-lg font-semibold">
        {state === "loading" ? "Checking…" : value}
      </p>
      <p className="mt-1 min-h-8 text-[10px] leading-4 text-muted-foreground">
        {detail}
      </p>
    </section>
  )
}

function TruthFact({
  label,
  value,
  detail,
}: {
  label: string
  value: string
  detail: string
}) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-3">
      <p className="font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-2 text-base font-semibold">{value}</p>
      <p className="mt-1 text-[10px] leading-4 text-muted-foreground">{detail}</p>
    </div>
  )
}

function EvidenceFact({
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
      <dt className="uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd
        className={`mt-1 break-all text-foreground/80 ${mono ? "font-mono" : ""}`}
      >
        {value}
      </dd>
    </div>
  )
}

function QueueItem({
  label,
  value,
  detail,
}: {
  label: string
  value: string
  detail: string
}) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-3">
      <div className="flex items-baseline justify-between gap-3">
        <p className="text-xs font-medium">{label}</p>
        <p className="font-mono text-[10px] text-primary">{value}</p>
      </div>
      <p className="mt-1 text-[10px] leading-4 text-muted-foreground">{detail}</p>
    </div>
  )
}

function factFromPerformance(
  performance: PortfolioPerformance | null,
  loading: boolean,
): { value: string; detail: string } {
  if (performance) {
    return {
      value: formatMoney(performance.currentValue),
      detail: "Current value for the selected account.",
    }
  }
  if (loading) {
    return {
      value: "Loading…",
      detail: "Reading the selected account revision.",
    }
  }
  return {
    value: "Not available",
    detail: "No current-value result was returned for this account revision.",
  }
}

function formatAvailableHistoryReturn(
  performance: PortfolioPerformance | null,
): string {
  if (performance?.timeWeightedReturn === undefined) return "Not returned"
  const periods =
    performance.periods === undefined
      ? "period count not returned"
      : `${performance.periods.toLocaleString()} comparable periods`
  return `${formatPercent(performance.timeWeightedReturn)} · ${periods}`
}

function formatExposure(exposure: PortfolioExposure | null): string {
  return exposure?.net ? formatMoney(exposure.net) : "Not returned"
}

function formatHistoricalValueAtRisk(risk: PortfolioRisk | null): string {
  if (risk?.valueAtRisk === undefined) return "Not returned"
  return `${formatPercent(risk.valueAtRisk)} at ${formatPercent(risk.confidence)} confidence`
}

function countValue<T extends readonly unknown[] | null>(
  state: ReadState<T>,
  suffix: string,
) {
  return state.status === "ready"
    ? `${state.data?.length ?? 0} ${suffix}`
    : "Unavailable"
}

function analysisCount(state: ReadState<InvestmentAnalysisPage>) {
  return state.status === "ready"
    ? state.data.availableCount.toLocaleString()
    : "Unavailable"
}

function freshMarketDetail(markets: MarketSnapshot) {
  if (!markets || markets.length === 0) {
    return "No active qualified live streams."
  }
  const fresh = markets.filter((stream) => stream.freshAtReference).length
  return `${fresh} of ${markets.length} markets are current.`
}

function isAnalysisJob(kind: string) {
  return ["analysis.", "decision.", "model.", "research."].some((prefix) =>
    kind.startsWith(prefix),
  )
}

function isTerminal(state: string) {
  return ["completed", "failed", "cancelled", "interrupted"].includes(state)
}
