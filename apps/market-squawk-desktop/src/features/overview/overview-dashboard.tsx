import {
  Activity,
  Bot,
  CircleAlert,
  Database,
  Gauge,
  Search,
  ServerCog,
} from "lucide-react"

import type { ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Skeleton } from "@/components/ui/skeleton"
import type { ProductTransport } from "@/lib/transport"

import { LookupSurface } from "@/features/lookup/lookup-surface"
import type { MarketSnapshot, OverviewJob, PaperStatus } from "./schemas"
import { type ReadState, useOverviewQueries } from "./use-overview"

export function OverviewDashboard({
  transport,
  scope,
}: {
  transport: ProductTransport
  scope: ProductScope
}) {
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
