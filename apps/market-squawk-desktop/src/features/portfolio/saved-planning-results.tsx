import * as React from "react"
import { RefreshCw } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { formatUnixNanos } from "../opportunities/format"
import { CursorNavigation } from "../shared/cursor-navigation"

import type { PortfolioAccountSummary } from "./portfolio-contracts"
import { PortfolioPositionReportView } from "./portfolio-position-impact"
import { PortfolioRebalanceReportView } from "./portfolio-rebalance"
import { PortfolioScenarioReportView } from "./portfolio-scenarios"
import type { SavedPlanningSummary } from "./saved-planning-contracts"
import { useSavedPlanningResult, useSavedPlanningResults } from "./use-saved-planning"

type SavedPlanningProps = { account: PortfolioAccountSummary; bootstrap: DesktopBootstrap; transport: ProductTransport }
const kindLabels: Record<SavedPlanningSummary["kind"], string> = {
  scenario: "Stress scenario", scenario_batch: "Stress scenario comparison",
  rebalance: "Rebalance plan", position_comparison: "Position comparison",
}

// The parent demand panel and account key own read lifetimes. Opening this view
// reads one page; selecting a summary reads only its original completed report.
export function SavedPlanningResults(props: SavedPlanningProps) {
  const [generation, setGeneration] = React.useState(0)
  return <section className="space-y-4" aria-label={`Saved planning for ${props.account.displayName}`}>
    <h2 className="text-lg font-semibold">Saved planning results</h2>
    <p className="text-xs leading-5 text-muted-foreground">
      Reopen calculations you explicitly saved for this portfolio. They keep their original
      assumptions, portfolio observation, and calculation evidence after restart or later imports.
      Opening a result does not recalculate it or check current prices.
    </p>
    <SavedPlanningRead key={`${props.account.accountToken}:${generation}`} {...props}
      refresh={() => setGeneration((value) => value + 1)} />
  </section>
}

function SavedPlanningRead({ account, bootstrap, transport, refresh }: SavedPlanningProps & { refresh: () => void }) {
  const readSession = React.useId()
  const results = useSavedPlanningResults(transport, bootstrap, account.accountToken, readSession)
  const [selected, setSelected] = React.useState<SavedPlanningSummary | null>(null)
  const page = results.query.data
  if (!results.available) return <SavedPlanningError title="Saved planning unavailable"
    detail="Saved planning results cannot currently be listed for this portfolio." />

  return <div className="space-y-4">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <p className="text-xs text-muted-foreground">Refresh to include newly saved calculations and clear the open result.</p>
      <Button type="button" variant="outline" onClick={refresh}><RefreshCw aria-hidden="true" /> Refresh saved planning results</Button>
    </div>
    {results.query.isPending ? <Skeleton className="h-32 rounded-xl" aria-label="Loading saved planning results" />
      : results.query.isError ? <div className="space-y-3">
        <SavedPlanningError title="Saved planning results could not be opened"
          detail="Try again for this page, or refresh saved planning results to start again." />
        <Button type="button" variant="outline" disabled={results.query.isFetching} onClick={() => void results.query.refetch()}>Try again</Button>
      </div> : page ? page.results.length ? <ul className="space-y-3" aria-label="Saved calculations on this page">
        {page.results.map((summary) => <li key={summary.savedResultToken} className="rounded-lg border border-border bg-background/25 p-4">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <p className="text-sm font-semibold">{kindLabels[summary.kind]}</p>
              <p className="mt-1 text-xs text-muted-foreground">Calculated {formatUnixNanos(summary.calculatedAtUnixNanos)}</p>
              <p className="mt-1 text-xs text-muted-foreground">Saved {formatUnixNanos(summary.savedAtUnixNanos)}</p>
              <p className="mt-1 text-xs text-muted-foreground">Portfolio observation: {formatUnixNanos(summary.portfolioEffectiveAtUnixNanos)}</p>
            </div>
            <Button type="button" size="sm" variant={selected?.savedResultToken === summary.savedResultToken ? "default" : "outline"}
              aria-pressed={selected?.savedResultToken === summary.savedResultToken}
              aria-label={`Open saved ${kindLabels[summary.kind].toLowerCase()} calculated ${formatUnixNanos(summary.calculatedAtUnixNanos)}`}
              onClick={() => setSelected(summary)}>Open saved result</Button>
          </div>
        </li>)}
      </ul> : <p className="text-xs text-muted-foreground">No saved planning results are available on this page.</p> : null}
    <CursorNavigation navigation={results.navigation} current={page?.pageCursor} next={page?.nextCursor}
      busy={results.query.isFetching} error={results.query.isError} onNavigate={() => setSelected(null)} onRestart={refresh} />
    {selected ? <section className="space-y-4" aria-label="Selected saved planning result">
      <Button type="button" size="sm" variant="outline" onClick={() => setSelected(null)}>Close saved result</Button>
      <SavedPlanningDetail key={selected.savedResultToken} bootstrap={bootstrap} transport={transport} selected={selected} />
    </section> : <p className="text-xs text-muted-foreground">Choose a saved result to show its original calculation.</p>}
  </div>
}

function SavedPlanningDetail({ bootstrap, transport, selected }: {
  bootstrap: DesktopBootstrap; transport: ProductTransport; selected: SavedPlanningSummary
}) {
  const readSession = React.useId()
  const detail = useSavedPlanningResult(transport, bootstrap, selected, readSession)
  if (!detail.available) return <SavedPlanningError title="Saved result unavailable"
    detail="The selected saved calculation cannot currently be opened." />
  if (detail.query.isPending) return <Skeleton className="h-48 rounded-xl" aria-label="Loading original saved calculation" />
  if (detail.query.isError) return <div className="space-y-3">
    <SavedPlanningError title="Saved result could not be opened" detail="Try again to reopen this exact saved calculation." />
    <Button type="button" variant="outline" disabled={detail.query.isFetching} onClick={() => void detail.query.refetch()}>Try again</Button>
  </div>
  if (!detail.query.data) return null
  const { summary, calculation } = detail.query.data
  return <div className="space-y-4">
    <div className="rounded-lg border border-border bg-background/25 p-4">
      <h3 className="text-sm font-semibold">Original saved {kindLabels[summary.kind].toLowerCase()}</h3>
      <p className="mt-2 text-xs leading-5 text-muted-foreground">
        This is a historical calculation. Later imports and prices do not update it. Any price
        freshness shown applies to the original calculation time and does not establish a current
        price or permission to trade.
      </p>
      <dl className="mt-3 grid gap-3 text-xs sm:grid-cols-2">
        <DateFact label="Original calculation time" value={summary.calculatedAtUnixNanos} />
        <DateFact label="Saved time" value={summary.savedAtUnixNanos} />
        <DateFact label="Original portfolio observation" value={summary.portfolioEffectiveAtUnixNanos} />
        <DateFact label="Original information available" value={summary.portfolioAvailableAtUnixNanos} />
      </dl>
    </div>
    {calculation.kind === "scenario" || calculation.kind === "scenario_batch"
      ? <PortfolioScenarioReportView report={calculation.report} />
      : calculation.kind === "rebalance" ? <PortfolioRebalanceReportView report={calculation.report} />
        : calculation.kind === "position_comparison" ? <PortfolioPositionReportView report={calculation.report} /> : null}
  </div>
}

function DateFact({ label, value }: { label: string; value: string | null }) {
  return <div><dt className="text-muted-foreground">{label}</dt><dd className="mt-1">{value === null ? "Not recorded" : formatUnixNanos(value)}</dd></div>
}

function SavedPlanningError({ title, detail }: { title: string; detail: string }) {
  return <Alert><AlertTitle>{title}</AlertTitle><AlertDescription>{detail}</AlertDescription></Alert>
}
