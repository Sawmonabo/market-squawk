import * as React from "react"
import { CircleAlert, RefreshCw } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { formatUnixNanos } from "../opportunities/format"
import { CursorNavigation } from "../shared/cursor-navigation"
import { DemandPanel } from "../shared/demand-panel"

import { AccountTransactions } from "./account-transactions"
import type { PortfolioAccountSummary, PortfolioAttribution } from "./portfolio-contracts"
import { usePortfolioAttribution, usePortfolioRevisions } from "./use-portfolio"

type HistoryProps = {
  account: PortfolioAccountSummary
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}

// The parent demand panel mounts this only while history is expanded.
export function PortfolioHistory(props: HistoryProps) {
  const [generation, setGeneration] = React.useState(0)
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <header>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">What changed</p>
        <h2 className="mt-2 text-lg font-semibold">Saved portfolio comparison</h2>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Choose an earlier saved version to compare reported position values. Change is shown
          before cash-flow and corporate-action adjustments. It is not investment return or performance.
        </p>
      </header>
      <HistoryRead key={generation} {...props} refresh={() => setGeneration((value) => value + 1)} />
      <DemandPanel title="Transaction history" className="mt-5 rounded-lg border border-border bg-background/25 p-4">
        <AccountTransactions {...props} />
      </DemandPanel>
    </section>
  )
}

function HistoryRead({ account, bootstrap, transport, refresh }: HistoryProps & { refresh: () => void }) {
  const readSession = React.useId()
  const history = usePortfolioRevisions(transport, bootstrap, account.accountToken, readSession)
  const [comparison, setComparison] = React.useState<{
    selectedSnapshotToken: string
    baselineSnapshotToken: string
  } | null>(null)
  const page = history.query.data

  if (!history.available) {
    return <HistoryUnavailable title="Saved versions unavailable"
      detail="Saved portfolio versions cannot currently be opened for this portfolio." />
  }

  return (
    <div role="region" className="mt-5 space-y-4" aria-label={`Saved portfolio history for ${account.displayName}`}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="max-w-3xl text-xs leading-5 text-muted-foreground">
          Saved versions are listed newest first. The selected version stays fixed while you browse.
          Refresh history to use the latest saved version and clear the comparison.
        </p>
        <Button variant="outline" onClick={refresh}>
          <RefreshCw aria-hidden="true" /> Refresh history
        </Button>
      </div>
      {history.query.isPending ? (
        <Skeleton className="h-48 rounded-xl" aria-label="Loading saved portfolio versions" />
      ) : history.query.isError ? (
        <div>
          <HistoryUnavailable title="Saved versions could not be opened"
            detail="Try again, or refresh history to start with the latest saved portfolio." />
          <Button className="mt-4" onClick={() => void history.query.refetch()} disabled={history.query.isFetching}>
            Try again
          </Button>
        </div>
      ) : page ? (
        <div>
          <h3 className="text-sm font-semibold">Choose an earlier saved version</h3>
          {page.revisions.length === 0 ? (
            <p className="mt-3 text-xs text-muted-foreground">No saved versions are available on this page.</p>
          ) : (
            <ul className="mt-3 space-y-3">
              {page.revisions.map((revision) => {
                const current = revision.snapshotToken === page.selectedSnapshotToken
                const chosen = revision.snapshotToken === comparison?.baselineSnapshotToken
                return (
                  <li key={revision.snapshotToken} className="rounded-lg border border-border bg-background/25 p-4">
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div>
                        <p className="text-sm font-medium">{formatUnixNanos(revision.effectiveAtUnixNanos)}</p>
                        <p className="mt-1 text-xs text-muted-foreground">
                          Information available: {availableTime(revision.availableAtUnixNanos)}
                        </p>
                        <p className="mt-2 text-xs text-muted-foreground">
                          {revision.holdingCount.toLocaleString()} holdings · {revision.transactionCount.toLocaleString()} recorded transactions
                          {` · ${revision.dataIssueCount.toLocaleString()} data issues`}
                        </p>
                        <p className="mt-1 text-xs text-muted-foreground">
                          {revision.dataState === "needs_review" ? "Review needed" : "Ready"}
                        </p>
                      </div>
                      {current ? (
                        <span className="text-xs font-medium text-primary">Selected saved version</span>
                      ) : (
                        <Button variant={chosen ? "default" : "outline"} size="sm" aria-pressed={chosen}
                          disabled={chosen}
                          onClick={() => setComparison({
                            selectedSnapshotToken: page.selectedSnapshotToken,
                            baselineSnapshotToken: revision.snapshotToken,
                          })}>
                          {chosen ? "Selected for comparison" : "Compare with this version"}
                        </Button>
                      )}
                    </div>
                  </li>
                )
              })}
            </ul>
          )}
        </div>
      ) : null}
      <CursorNavigation navigation={history.navigation} current={page?.pageCursor} next={page?.nextCursor}
        busy={history.query.isFetching} error={history.query.isError} onRestart={refresh} />
      {comparison ? (
        <div className="space-y-3">
          <Button variant="outline" size="sm" onClick={() => setComparison(null)}>Clear comparison</Button>
          <ComparisonRead key={`${comparison.selectedSnapshotToken}:${comparison.baselineSnapshotToken}`}
            account={account} bootstrap={bootstrap} transport={transport} {...comparison} />
        </div>
      ) : (
        <p className="text-xs leading-5 text-muted-foreground">
          No comparison is selected. Choose an earlier saved version to show its exact value changes.
        </p>
      )}
    </div>
  )
}

function ComparisonRead({ account, bootstrap, transport, selectedSnapshotToken, baselineSnapshotToken }: HistoryProps & {
  selectedSnapshotToken: string
  baselineSnapshotToken: string
}) {
  const readSession = React.useId()
  const comparison = usePortfolioAttribution(transport, bootstrap, account.accountToken,
    selectedSnapshotToken, baselineSnapshotToken, readSession)
  const page = comparison.query.data
  if (!comparison.available) {
    return <HistoryUnavailable title="Saved comparison unavailable"
      detail="This comparison cannot currently be opened for this portfolio." />
  }
  return (
    <section className="rounded-lg border border-border bg-background/25 p-4" aria-label="Saved position value comparison">
      <h3 className="text-sm font-semibold">Reported position value changes</h3>
      {comparison.query.isPending ? (
        <Skeleton className="mt-4 h-64 rounded-xl" aria-label="Loading saved portfolio comparison" />
      ) : comparison.query.isError ? (
        <div className="mt-4">
          <HistoryUnavailable title="Comparison could not be opened"
            detail="Try again to read the selected saved versions, or refresh history to choose again." />
          <Button className="mt-4" onClick={() => void comparison.query.refetch()} disabled={comparison.query.isFetching}>
            Try again
          </Button>
        </div>
      ) : page ? <ComparisonDetails page={page} /> : null}
      <CursorNavigation navigation={comparison.navigation} current={page?.pageCursor} next={page?.nextCursor}
        busy={comparison.query.isFetching} error={comparison.query.isError} onRestart={comparison.restart} />
    </section>
  )
}

function ComparisonDetails({ page }: { page: PortfolioAttribution }) {
  return (
    <div className="mt-4 space-y-4">
      <dl className="grid gap-4 text-xs sm:grid-cols-2">
        <VersionDates label="Earlier saved version" effective={page.baselineEffectiveAtUnixNanos}
          available={page.baselineAvailableAtUnixNanos} />
        <VersionDates label="Selected saved version" effective={page.effectiveAtUnixNanos}
          available={page.availableAtUnixNanos} />
        <div className="sm:col-span-2">
          <dt className="text-muted-foreground">Whole comparison change in reported position value</dt>
          <dd className="mt-1 font-mono text-lg tabular-nums">{formatMoney(page.total)}</dd>
        </div>
      </dl>
      <p className="text-xs leading-5 text-muted-foreground">{page.explanation}</p>
      <p className="text-xs leading-5 text-muted-foreground">
        The total covers all positions in the comparison, regardless of this page. These changes
        are before cash-flow and corporate-action adjustments, and are not returns or performance.
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-xs">
          <caption className="sr-only">Reported position value changes on this page</caption>
          <thead className="border-b border-border text-muted-foreground">
            <tr>
              <th scope="col" className="px-3 py-3 font-medium">Investment</th>
              <th scope="col" className="px-3 py-3 font-medium">Opening value</th>
              <th scope="col" className="px-3 py-3 font-medium">Closing value</th>
              <th scope="col" className="px-3 py-3 font-medium">Change</th>
            </tr>
          </thead>
          <tbody>
            {page.contributions.map((contribution) => (
              <tr key={contribution.instrumentId} className="border-b border-border/60">
                <th scope="row" className="px-3 py-3 font-medium">
                  {contribution.investment.name ?? "Investment name unavailable"}
                  {contribution.investment.symbol ? ` (${contribution.investment.symbol})` : ""}
                </th>
                <td className="whitespace-nowrap px-3 py-3 font-mono tabular-nums">{formatMoney(contribution.opening)}</td>
                <td className="whitespace-nowrap px-3 py-3 font-mono tabular-nums">{formatMoney(contribution.closing)}</td>
                <td className="whitespace-nowrap px-3 py-3 font-mono tabular-nums">{formatMoney(contribution.amount)}</td>
              </tr>
            ))}
            {page.contributions.length === 0 ? (
              <tr><td colSpan={4} className="px-3 py-5 text-muted-foreground">No positions are available on this comparison page.</td></tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function VersionDates({ label, effective, available }: { label: string; effective: string; available: string | null }) {
  return (
    <div>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-1">{formatUnixNanos(effective)}</dd>
      <dd className="mt-1 text-muted-foreground">Information available: {availableTime(available)}</dd>
    </div>
  )
}

function availableTime(value: string | null) {
  return value === null ? "Not recorded" : formatUnixNanos(value)
}

function HistoryUnavailable({ title, detail }: { title: string; detail: string }) {
  return (
    <Alert className="mt-4">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{detail}</AlertDescription>
    </Alert>
  )
}
