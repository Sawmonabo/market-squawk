import * as React from "react"
import {
  Activity,
  Bot,
  BriefcaseBusiness,
  CircleAlert,
  Database,
  Gauge,
  Search,
  ServerCog,
} from "lucide-react"
import { Link } from "react-router-dom"

import type { ProductScope } from "@/app/query-client"
import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { LookupSurface } from "@/features/lookup/lookup-surface"
import { compactMoney, formatPercent, formatTimestamp } from "@/features/portfolio/portfolio-format"
import { usePortfolioAccounts, usePortfolioDetails } from "@/features/portfolio/use-portfolio"
import type { MarketSnapshot, OverviewJob, PaperStatus } from "./schemas"
import { type ReadState, useOverviewQueries } from "./use-overview"

export function OverviewDashboard({
  transport,
  scope,
}: {
  transport: ProductTransport
  scope: ProductScope
}) {
  const product = useProduct()
  const queries = useOverviewQueries(transport, scope)
  const activeJobs =
    queries.jobs.status === "ready"
      ? queries.jobs.data.jobs.filter((job) => !isTerminal(job.state))
      : []

  return (
    <div className="space-y-4">
      <section aria-label="Current workspace status" className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <StatusCard
          icon={ServerCog}
          label="Data sources"
          value={countValue(queries.sources, "configured")}
          state={queries.sources.status}
          detail={
            queries.sources.status === "ready"
              ? "Current provider health, not just setup status."
              : queries.sources.message
          }
        />
        <StatusCard
          icon={Activity}
          label="Live markets"
          value={countValue(queries.markets, "streams")}
          state={queries.markets.status}
          detail={
            queries.markets.status === "ready"
              ? freshMarketDetail(queries.markets.data)
              : "No qualified live snapshot is available right now."
          }
        />
        <StatusCard
          icon={Gauge}
          label="Active work"
          value={`${activeJobs.length} job${activeJobs.length === 1 ? "" : "s"}`}
          state={queries.jobs.status}
          detail={queries.jobs.status === "ready" ? "Durable jobs survive page changes." : queries.jobs.message}
        />
        <StatusCard
          icon={Bot}
          label="Paper execution"
          value={queries.paper.status === "ready" ? friendlyState(queries.paper.data.state) : "Unavailable"}
          state={queries.paper.status}
          detail={paperDetail(queries.paper)}
        />
      </section>

      {product.status === "ready" ? (
        <PortfolioTruth
          transport={transport}
          bootstrap={product.bootstrap}
        />
      ) : null}

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.65fr)]">
        <div className="rounded-xl border border-border bg-card/45 p-5">
          <div className="mb-4 flex items-start gap-3">
            <span className="rounded-lg border border-border bg-background/70 p-2.5">
              <Search className="size-4 text-primary" aria-hidden="true" />
            </span>
            <div>
              <h2 className="text-base font-semibold">Look up anything in your workspace</h2>
              <p className="mt-1 text-xs leading-5 text-muted-foreground">
                Find real local sources, research datasets, screens, jobs, and safe application actions.
              </p>
            </div>
          </div>
          <LookupSurface transport={transport} scope={scope} />
        </div>

        <DecisionQueue overview={queries.overview} activeJobs={activeJobs} />
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <LiveMarketPanel markets={queries.markets} />
        <SourceHealthPanel sources={queries.sources} />
      </section>
    </div>
  )
}

function PortfolioTruth({
  transport,
  bootstrap,
}: {
  transport: ProductTransport
  bootstrap: DesktopBootstrap
}) {
  const accounts = usePortfolioAccounts(transport, bootstrap)
  const accountRows = accounts.query.data?.pages.flatMap((page) => page.value) ?? []
  const [selectedId, setSelectedId] = React.useState<string | null>(null)
  const account =
    accountRows.find((candidate) => candidate.accountId === selectedId) ??
    accountRows[0] ??
    null
  const details = usePortfolioDetails(
    transport,
    bootstrap.runtime,
    bootstrap,
    account?.accountId ?? null,
  )

  if (!accounts.available) {
    return (
      <PortfolioEmpty
        title="Portfolio truth is not available"
        detail="The installed service does not expose the account index. Restore or complete Portfolio setup before relying on account, holding, performance, exposure, or risk totals."
      />
    )
  }
  if (accounts.query.isPending) {
    return <Skeleton className="h-56 rounded-xl" />
  }
  if (accounts.query.isError) {
    return (
      <PortfolioEmpty
        title="Portfolio accounts could not be read"
        detail={accounts.query.error instanceof Error ? accounts.query.error.message : "The installed service rejected the account query."}
      />
    )
  }
  if (!account) {
    return (
      <PortfolioEmpty
        title="No portfolio account is loaded"
        detail="Import a supported portfolio source to see holdings, performance, exposure, and risk here. Market Squawk will not invent an account or assume a reporting currency."
      />
    )
  }

  const holdings = details.holdings.data?.value ?? null
  const performance = details.performance.data?.value ?? null
  const exposure = details.exposure.data?.value ?? null
  const risk = details.risk.data?.value ?? null
  const expectedAccountId = account.accountId
  const expectedRevisionId = account.currentRevision.revisionId
  const changedWhileReading =
    holdings?.some(
      (holding) =>
        holding.account_id !== expectedAccountId ||
        holding.revisionId !== expectedRevisionId,
    ) === true ||
    [performance, exposure, risk].some(
      (detail) =>
        detail !== null &&
        (detail.accountId !== expectedAccountId ||
          detail.revisionId !== expectedRevisionId),
    )
  const detailLoading = {
    performance:
      details.operationAvailable["Portfolio.GetPerformance"] &&
      details.performance.isPending,
    exposure:
      details.operationAvailable["Portfolio.GetExposure"] &&
      details.exposure.isPending,
    risk:
      details.operationAvailable["Portfolio.GetRisk"] && details.risk.isPending,
  }
  const unavailable = [
    !details.operationAvailable["Portfolio.GetHoldings"] ? "holdings" : null,
    !details.operationAvailable["Portfolio.GetPerformance"] ? "performance" : null,
    !details.operationAvailable["Portfolio.GetExposure"] ? "exposure" : null,
    !details.operationAvailable["Portfolio.GetRisk"] ? "risk" : null,
  ].filter((value): value is string => value !== null)
  const failures = [
    details.holdings.error,
    details.performance.error,
    details.exposure.error,
    details.risk.error,
  ].filter((value): value is Error => value instanceof Error)

  if (changedWhileReading) {
    return (
      <section className="rounded-xl border border-amber-400/30 bg-amber-400/5 p-5" aria-labelledby="portfolio-changed-title">
        <div className="flex items-start gap-3">
          <CircleAlert className="mt-0.5 size-4 shrink-0 text-amber-300" aria-hidden="true" />
          <div>
            <h2 id="portfolio-changed-title" className="text-sm font-semibold">
              Portfolio changed while details were being read
            </h2>
            <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
              At least one detail response belongs to a different account or revision. No mixed-revision portfolio values are shown. Refresh the account and its details together before relying on this summary.
            </p>
            <Button
              className="mt-3"
              size="sm"
              variant="outline"
              disabled={accounts.query.isFetching || details.isFetching}
              onClick={() => {
                void accounts.query.refetch().then(() => details.refresh())
              }}
            >
              Refresh portfolio truth
            </Button>
          </div>
        </div>
      </section>
    )
  }

  return (
    <section className="rounded-xl border border-border bg-card/45 p-5" aria-labelledby="portfolio-truth-title">
      <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
        <div className="flex items-start gap-3">
          <span className="rounded-lg border border-border bg-background/70 p-2.5">
            <BriefcaseBusiness className="size-4 text-primary" aria-hidden="true" />
          </span>
          <div>
            <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">Portfolio truth</p>
            <h2 id="portfolio-truth-title" className="mt-1 text-base font-semibold">Account, holdings, performance, exposure, and risk</h2>
            <p className="mt-1 text-xs leading-5 text-muted-foreground">
              Values below come from one selected immutable account revision. Accounts with different currencies are never silently combined.
            </p>
          </div>
        </div>
        <Button asChild variant="outline" size="sm">
          <Link to="/portfolios">Open Portfolios</Link>
        </Button>
      </div>

      {accountRows.length > 1 ? (
        <label className="mt-4 block max-w-xl text-xs font-medium">
          Account
          <select
            className="mt-2 h-9 w-full rounded-md border border-input bg-background px-3 font-mono text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring"
            value={account.accountId}
            onChange={(event) => setSelectedId(event.target.value)}
          >
            {accountRows.map((candidate) => (
              <option key={candidate.accountId} value={candidate.accountId}>
                {candidate.accountId} · {candidate.currency.toUpperCase()}
              </option>
            ))}
          </select>
        </label>
      ) : null}

      {unavailable.length ? (
        <p className="mt-4 rounded-lg border border-border bg-background/35 p-3 text-[11px] leading-5 text-muted-foreground">
          This service does not expose {unavailable.join(", ")} for the selected account. Open Portfolios to complete or repair the missing capability.
        </p>
      ) : null}
      {failures.length ? (
        <p className="mt-4 rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-[11px] leading-5 text-destructive">
          {failures.map((failure) => failure.message).join(" · ")}
        </p>
      ) : null}

      <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        <TruthFact
          label="Current value"
          value={
            performance
              ? compactMoney(performance.currentValue)
              : detailLoading.performance
                ? "Loading…"
                : "Unavailable"
          }
          detail={
            performance?.historyStatus ??
            (detailLoading.performance
              ? "Reading the selected account revision."
              : "No performance truth returned.")
          }
        />
        <TruthFact
          label="Holdings"
          value={account.holdingCount.toLocaleString()}
          detail={`${account.transactionCount.toLocaleString()} retained transactions.`}
        />
        <TruthFact
          label="Performance"
          value={
            performance
              ? formatPercent(performance.timeWeightedReturn)
              : detailLoading.performance
                ? "Loading…"
                : "Unavailable"
          }
          detail={
            detailLoading.performance
              ? "Reading the selected account revision."
              : performance?.periods === undefined
                ? "Comparable history unavailable."
                : `${performance.periods.toLocaleString()} comparable periods.`
          }
        />
        <TruthFact
          label="Net exposure"
          value={
            exposure?.net
              ? compactMoney(exposure.net)
              : detailLoading.exposure
                ? "Loading…"
                : "Unavailable"
          }
          detail={
            exposure?.calculationStatus ??
            (detailLoading.exposure
              ? "Reading the selected account revision."
              : "No exposure calculation returned.")
          }
        />
        <TruthFact
          label="Historical value at risk"
          value={
            risk
              ? formatPercent(risk.valueAtRisk)
              : detailLoading.risk
                ? "Loading…"
                : "Unavailable"
          }
          detail={
            risk
              ? `${formatPercent(risk.confidence)} confidence · ${risk.observations?.toLocaleString() ?? "unknown"} observations.`
              : detailLoading.risk
                ? "Reading the selected account revision."
                : "No historical VaR estimate returned."
          }
        />
      </div>

      <dl className="mt-4 grid gap-3 border-t border-border pt-4 text-[10px] sm:grid-cols-2 xl:grid-cols-4">
        <TruthEvidence label="Reporting currency" value={account.currency.toUpperCase()} />
        <TruthEvidence label="Portfolio source" value={account.currentRevision.sourceId} />
        <TruthEvidence label="Effective at" value={formatTimestamp(account.currentRevision.effectiveAtUnixNanos)} />
        <TruthEvidence label="Available to analysis" value={formatTimestamp(account.currentRevision.availableAtUnixNanos)} />
        <TruthEvidence label="Revision" value={account.currentRevision.revisionId} mono />
        <TruthEvidence label="Artifact" value={account.currentRevision.artifactSha256} mono />
        <TruthEvidence label="Source coverage" value={account.currentRevision.sourceCoverage.join(", ") || "Not reported"} />
        <TruthEvidence
          label="Reconciliation"
          value={account.reconciliationDiscrepancies === 0 ? "No retained discrepancy" : `${account.reconciliationDiscrepancies} retained discrepancies`}
        />
      </dl>
    </section>
  )
}

function PortfolioEmpty({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <div className="flex items-start gap-3">
        <BriefcaseBusiness className="mt-0.5 size-4 text-primary" aria-hidden="true" />
        <div>
          <h2 className="text-sm font-semibold">{title}</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">{detail}</p>
          <Button asChild className="mt-3" size="sm">
            <Link to="/portfolios">Open Portfolios</Link>
          </Button>
        </div>
      </div>
    </section>
  )
}

function TruthFact({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-3">
      <p className="font-mono text-[9px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-2 text-base font-semibold">{value}</p>
      <p className="mt-1 text-[10px] leading-4 text-muted-foreground">{detail}</p>
    </div>
  )
}

function TruthEvidence({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="min-w-0">
      <dt className="uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className={`mt-1 break-all text-foreground/80 ${mono ? "font-mono" : ""}`}>{value}</dd>
    </div>
  )
}

function DecisionQueue({
  overview,
  activeJobs,
}: {
  overview: ReturnType<typeof useOverviewQueries>["overview"]
  activeJobs: OverviewJob[]
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5" aria-labelledby="decision-queue-title">
      <div className="flex items-center justify-between">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">Decision queue</p>
          <h2 id="decision-queue-title" className="mt-1 text-base font-semibold">What deserves attention</h2>
        </div>
        <Gauge className="size-5 text-primary" aria-hidden="true" />
      </div>
      {overview.status === "loading" ? (
        <div className="mt-5 space-y-3">
          <Skeleton className="h-14 rounded-lg" />
          <Skeleton className="h-14 rounded-lg" />
          <Skeleton className="h-14 rounded-lg" />
        </div>
      ) : overview.status === "unavailable" ? (
        <Alert className="mt-5">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Decision summary unavailable</AlertTitle>
          <AlertDescription>{overview.message}</AlertDescription>
        </Alert>
      ) : (
        <div className="mt-5 space-y-3">
          <QueueItem
            label="Research foundation"
            value={`${overview.data.datasets.count} dataset${overview.data.datasets.count === 1 ? "" : "s"}`}
            detail={overview.data.datasets.hasMore ? "More datasets are available in Research." : "Complete bounded dataset index."}
          />
          <QueueItem
            label="Saved decision screens"
            value={`${overview.data.screens.count} screen${overview.data.screens.count === 1 ? "" : "s"}`}
            detail="Point-in-time screen definitions available for research workflows."
          />
          <QueueItem
            label="Work in progress"
            value={`${activeJobs.length} active`}
            detail={activeJobs[0] ? `${activeJobs[0].kind} · ${friendlyState(activeJobs[0].state)}` : "No durable job currently needs attention."}
          />
          {overview.data.unavailable.length > 0 ? (
            <details className="rounded-lg border border-border bg-background/35 px-3 py-2">
              <summary className="cursor-pointer text-[11px] font-medium text-muted-foreground">
                Evidence not available in this summary ({overview.data.unavailable.length})
              </summary>
              <ul className="mt-2 space-y-2 text-[11px] leading-4 text-muted-foreground">
                {overview.data.unavailable.map((item) => (
                  <li key={item.category}>
                    <strong className="capitalize text-foreground/75">{item.category}:</strong> {item.reason}
                  </li>
                ))}
              </ul>
            </details>
          ) : null}
        </div>
      )}
    </section>
  )
}

function LiveMarketPanel({
  markets,
}: {
  markets: ReturnType<typeof useOverviewQueries>["markets"]
}) {
  return (
    <EvidencePanel title="Live market truth" icon={Activity} state={markets.status} message={markets.message}>
      {markets.status === "ready" && markets.data && markets.data.length > 0 ? (
        <ul className="divide-y divide-border">
          {markets.data.slice(0, 5).map((stream) => (
            <li key={`${stream.sourceId}:${stream.instrumentId}`} className="flex items-center gap-3 py-3 first:pt-0 last:pb-0">
              <span className={`size-2 rounded-full ${stream.freshAtReference ? "bg-[var(--success)]" : "bg-[var(--warning)]"}`} aria-hidden="true" />
              <span className="min-w-0 flex-1">
                <span className="block truncate text-xs font-medium">{stream.instrumentId}</span>
                <span className="mt-0.5 block truncate font-mono text-[9px] text-muted-foreground">
                  {stream.sourceId} · {stream.venueId}
                </span>
              </span>
              <span className="text-right text-[10px] text-muted-foreground">
                <span className="block capitalize">{stream.currentDisplayQuality.replaceAll("_", " ")}</span>
                <span className="block capitalize">{stream.tradingStatus.replaceAll("_", " ")}</span>
              </span>
            </li>
          ))}
        </ul>
      ) : null}
    </EvidencePanel>
  )
}

function SourceHealthPanel({
  sources,
}: {
  sources: ReturnType<typeof useOverviewQueries>["sources"]
}) {
  return (
    <EvidencePanel title="Source health" icon={ServerCog} state={sources.status} message={sources.message}>
      {sources.status === "ready" && sources.data && sources.data.length > 0 ? (
        <ul className="divide-y divide-border">
          {sources.data.slice(0, 5).map((source) => {
            const runtimeState = firstText(source.runtimeHealth, ["state", "phase", "status"]) ?? "Not active"
            return (
              <li key={source.surfaceId} className="flex items-center gap-3 py-3 first:pt-0 last:pb-0">
                <span className="rounded-md border border-border bg-background/60 p-2">
                  <ServerCog className="size-3.5 text-primary" aria-hidden="true" />
                </span>
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-xs font-medium">{source.surfaceId}</span>
                  <span className="mt-0.5 block text-[10px] capitalize text-muted-foreground">
                    {source.onboardingState?.replaceAll("_", " ") ?? "Not configured"}
                  </span>
                </span>
                <span className="text-[10px] capitalize text-muted-foreground">{runtimeState.replaceAll("_", " ")}</span>
              </li>
            )
          })}
        </ul>
      ) : null}
    </EvidencePanel>
  )
}

function EvidencePanel({
  title,
  icon: Icon,
  state,
  message,
  children,
}: {
  title: string
  icon: typeof Activity
  state: ReadState<unknown>["status"]
  message: string | null
  children: React.ReactNode
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <div className="mb-4 flex items-center gap-2">
        <Icon className="size-4 text-primary" aria-hidden="true" />
        <h2 className="text-sm font-semibold">{title}</h2>
      </div>
      {state === "loading" ? <Skeleton className="h-28 rounded-lg" /> : null}
      {state === "unavailable" ? (
        <div className="rounded-lg border border-border bg-background/35 p-4">
          <p className="text-xs font-medium">Not available right now</p>
          <p className="mt-1 text-[11px] leading-5 text-muted-foreground">{message}</p>
        </div>
      ) : null}
      {state === "ready" && !children ? (
        <p className="rounded-lg border border-dashed border-border p-5 text-center text-xs text-muted-foreground">
          No current records were returned.
        </p>
      ) : children}
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
      <p className="mt-4 font-mono text-[9px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 text-lg font-semibold">{state === "loading" ? "Checking…" : value}</p>
      <p className="mt-1 min-h-8 text-[10px] leading-4 text-muted-foreground">{detail}</p>
    </section>
  )
}

function QueueItem({ label, value, detail }: { label: string; value: string; detail: string }) {
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

function countValue<T extends readonly unknown[] | null>(state: ReadState<T>, suffix: string) {
  return state.status === "ready" ? `${state.data?.length ?? 0} ${suffix}` : "Unavailable"
}

function freshMarketDetail(markets: MarketSnapshot) {
  if (!markets || markets.length === 0) return "No active qualified live streams."
  const fresh = markets.filter((stream) => stream.freshAtReference).length
  return `${fresh} of ${markets.length} streams are fresh at the service reference time.`
}

function paperDetail(paper: ReadState<PaperStatus>) {
  if (paper.status !== "ready") return paper.message ?? "Paper state is unavailable."
  if (paper.data.state !== "running") return "No orders can be submitted until the paper runtime is explicitly started."
  if (paper.data.reconciliationRequired) return "Reconciliation is required before normal operation can continue."
  return `${paper.data.orders ?? 0} orders · ${paper.data.fills ?? 0} fills · ${paper.data.positions ?? 0} positions`
}

function firstText(values: Record<string, unknown>, keys: string[]) {
  for (const key of keys) {
    const value = values[key]
    if (typeof value === "string" && value.length > 0) return value
  }
  return null
}

function friendlyState(value: string) {
  return value.replaceAll("_", " ").replace(/^./, (first) => first.toUpperCase())
}

function isTerminal(state: string) {
  return ["completed", "failed", "cancelled", "interrupted"].includes(state)
}
