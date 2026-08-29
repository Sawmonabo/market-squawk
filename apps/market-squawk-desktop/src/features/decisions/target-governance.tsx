import * as React from "react"
import { useInfiniteQuery, useQuery } from "@tanstack/react-query"
import {
  AlertCircle,
  CalendarClock,
  CircleDollarSign,
  Scale,
  ShieldAlert,
} from "lucide-react"

import { messageFrom } from "@/app/product-context"
import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney, humanize } from "@/lib/formatters"
import { formatTimestamp, timestampFromUnixNanos } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import {
  parseDecisionTargetIndexPage,
  parseDecisionTargets,
  type DecisionDossierView,
  type TargetIndexView,
  type TargetStateView,
} from "./contracts"
import { StateLabel } from "./decision-boundaries"
import { TargetGovernanceWorkflow } from "./governance-workflow"
import { TargetBuilder } from "./target-builder"

const DISCOVERY_LIMIT = 100

export function TargetGovernanceWorkspace({
  transport,
  scope,
  dossier,
}: {
  transport: ProductTransport
  scope: ProductScope
  dossier: DecisionDossierView | null
}) {
  const [targetId, setTargetId] = React.useState("")
  const targetIndex = useInfiniteQuery({
    queryKey: productKeys.operation(scope, "decision", "target-index", {
      limit: DISCOVERY_LIMIT,
    }),
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam }) =>
      parseDecisionTargetIndexPage(
        await transport.query({
          query: "decisionTargetIndex",
          ...(pageParam ? { afterTargetId: pageParam } : {}),
          limit: DISCOVERY_LIMIT,
        }),
      ),
    getNextPageParam: (page) => page.nextAfter ?? undefined,
  })
  const targets = useQuery({
    queryKey: productKeys.operation(scope, "decision", "target-history", {
      targetId,
    }),
    queryFn: async () =>
      parseDecisionTargets(
        await transport.query({ query: "decisionTargets", targetId }),
      ),
    enabled: targetId.length > 0,
  })

  const revisions = [...(targets.data ?? [])].sort(
    (left, right) => right.target.revision - left.target.revision,
  )
  const targetEntries = targetIndex.data?.pages.flatMap((page) => page.items) ?? []

  return (
    <section aria-labelledby="target-governance-heading" className="mt-8">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Reviewable target history
          </p>
          <h2 id="target-governance-heading" className="mt-1 text-lg font-semibold">
            Target ranges, review, and invalidation
          </h2>
          <p className="mt-1 max-w-3xl text-sm leading-6 text-muted-foreground">
            Review each target's price ranges, supporting analysis, review dates, and invalidation
            state.
          </p>
        </div>
        <div className="max-w-sm rounded-xl border border-primary/25 bg-primary/5 p-3 text-xs leading-5 text-muted-foreground">
          Prices below are research judgment ranges. They are neither forecasts nor executable orders.
        </div>
      </div>

      <TargetBuilder
        key={dossier?.id ?? "no-dossier"}
        transport={transport}
        scope={scope}
        dossier={dossier}
        targetIndex={targetEntries}
        onCommitted={async (committedTargetId) => {
          setTargetId(committedTargetId)
          await targetIndex.refetch()
          if (committedTargetId === targetId) await targets.refetch()
        }}
      />

      {targetIndex.isPending ? (
        <TargetLoading />
      ) : targetIndex.isError ? (
        <Alert variant="destructive" className="mt-4">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Target discovery could not be loaded</AlertTitle>
          <AlertDescription>
            {messageFrom(targetIndex.error)}
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="mt-2"
              onClick={() => void targetIndex.refetch()}
            >
              Retry
            </Button>
          </AlertDescription>
        </Alert>
      ) : targetEntries.length === 0 ? (
        <div className="mt-4 rounded-xl border border-dashed border-border p-6 text-sm text-muted-foreground">
          No investment targets are saved in this workspace.
        </div>
      ) : (
        <div className="mt-4 grid gap-2 lg:grid-cols-2">
          {targetEntries.map((entry) => (
            <TargetIndexCard
              key={entry.id}
              entry={entry}
              selected={entry.id === targetId}
              onSelect={() => setTargetId(entry.id)}
            />
          ))}
          {targetIndex.hasNextPage && (
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={targetIndex.isFetchingNextPage}
              onClick={() => void targetIndex.fetchNextPage()}
            >
              Load more targets
            </Button>
          )}
        </div>
      )}

      {!targetId ? (
        <TargetPrompt />
      ) : targets.isPending ? (
        <TargetLoading />
      ) : targets.isError ? (
        <Alert variant="destructive" className="mt-4">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Target history could not be loaded</AlertTitle>
          <AlertDescription>
            {messageFrom(targets.error)}
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="mt-2"
              onClick={() => void targets.refetch()}
            >
              Retry
            </Button>
          </AlertDescription>
        </Alert>
      ) : revisions.length === 0 ? (
        <div className="mt-4 rounded-xl border border-dashed border-border p-6 text-sm text-muted-foreground">
          No revisions exist for this target.
        </div>
      ) : (
        <div className="mt-4 grid gap-4">
          {revisions.map((state) => (
            <TargetRevisionCard
              key={`${state.target.id}:${state.target.revision}`}
              state={state}
              transport={transport}
              scope={scope}
              onCommitted={() => {
                void targets.refetch()
                void targetIndex.refetch()
              }}
            />
          ))}
        </div>
      )}
    </section>
  )
}

function TargetRevisionCard({
  state,
  transport,
  scope,
  onCommitted,
}: {
  state: TargetStateView
  transport: ProductTransport
  scope: ProductScope
  onCommitted: () => void
}) {
  const { target } = state
  const expired = isPast(target.expiresAt)
  const reviewOverdue = isPast(target.reviewDueAt)
  const stale = state.status === "needs_review"

  return (
    <article className="overflow-hidden rounded-xl border border-border bg-card/45">
      <header className="flex flex-wrap items-start justify-between gap-3 border-b border-border p-4">
        <div className="min-w-0">
          <p className="font-mono text-[10px] uppercase tracking-[0.12em] text-primary">
            Revision {target.revision} · {humanize(target.method)}
          </p>
          <h3 className="mt-1 truncate text-base font-semibold" title={target.instrumentId}>
            {target.instrumentId}
          </h3>
          <p className="mt-1 text-xs text-muted-foreground">
            Author {target.author} · ruleset {target.rulesetVersion}
          </p>
        </div>
        <div className="flex flex-wrap justify-end gap-2">
          <StateLabel value={state.status} />
          {expired && <StateLabel value="expired by recorded timestamp" />}
          {reviewOverdue && <StateLabel value="review overdue" />}
          {stale && <StateLabel value="new review needed" />}
        </div>
      </header>

      <div className="grid gap-4 p-4 xl:grid-cols-[minmax(0,1.15fr)_minmax(300px,0.85fr)]">
        <div className="grid content-start gap-4">
          <ObservedReference state={state} />
          <JudgmentRanges state={state} />
          <ThesisEvidence state={state} />
        </div>
        <div className="grid content-start gap-4">
          <GovernanceTimeline state={state} />
          <RiskInvalidation state={state} />
          <ExecutionBoundary />
          <TargetGovernanceWorkflow
            transport={transport}
            scope={scope}
            targetId={target.id}
            targetRevision={target.revision}
            onCommitted={onCommitted}
          />
        </div>
      </div>
    </article>
  )
}

function TargetIndexCard({
  entry,
  selected,
  onSelect,
}: {
  entry: TargetIndexView
  selected: boolean
  onSelect: () => void
}) {
  return (
    <button
      type="button"
      className="rounded-xl border border-border bg-card/45 p-4 text-left transition-colors hover:border-primary/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      aria-pressed={selected}
      onClick={onSelect}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="truncate text-sm font-semibold" title={entry.instrumentId}>
            {entry.instrumentId}
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            Revision {entry.revision}
          </p>
        </div>
        <StateLabel value={entry.status} />
      </div>
    </button>
  )
}

function ObservedReference({ state }: { state: TargetStateView }) {
  const { target } = state
  return (
    <section className="rounded-xl border border-border bg-background/45 p-4">
      <div className="flex items-center gap-2">
        <CircleDollarSign className="size-4 text-primary" aria-hidden="true" />
        <h4 className="text-sm font-semibold">Observed reference mark</h4>
      </div>
      <div className="mt-3 flex flex-wrap items-end justify-between gap-3">
        <p className="text-xl font-semibold tabular-nums">{formatMoney(target.referencePrice)}</p>
        <p className="text-xs text-muted-foreground">
          Observed {formatTimestamp(target.referenceObservedAt)} · {humanize(target.markQuality)}
        </p>
      </div>
    </section>
  )
}

function JudgmentRanges({ state }: { state: TargetStateView }) {
  const { target } = state
  return (
    <section className="rounded-xl border border-primary/20 bg-primary/5 p-4">
      <div className="flex items-center gap-2">
        <Scale className="size-4 text-primary" aria-hidden="true" />
        <h4 className="text-sm font-semibold">Research judgment</h4>
      </div>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        Versioned cases and inclusive decision ranges; not a probability forecast or instruction to trade.
      </p>
      <dl className="mt-4 grid gap-2 sm:grid-cols-3">
        <PriceFact label="Downside case" value={formatMoney(target.downside)} />
        <PriceFact label="Base case" value={formatMoney(target.base)} />
        <PriceFact label="Upside case" value={formatMoney(target.upside)} />
        <PriceFact label="Entry range" value={formatRange(target.entryLower, target.entryUpper)} />
        <PriceFact label="Trim range" value={formatRange(target.trimLower, target.trimUpper)} />
        <PriceFact label="Exit range" value={formatRange(target.exitLower, target.exitUpper)} />
      </dl>
      <p className="mt-3 text-xs text-muted-foreground">
        Add case: <span className="font-medium text-foreground">{formatMoney(target.addCase)}</span>
      </p>
    </section>
  )
}

function ThesisEvidence({ state }: { state: TargetStateView }) {
  const { target } = state
  return (
    <section className="rounded-xl border border-border bg-background/45 p-4">
      <h4 className="text-sm font-semibold">Thesis and supporting analysis</h4>
      <p className="mt-2 text-sm leading-6">{target.thesis}</p>
      {target.assumptions.length > 0 && (
        <div className="mt-4">
          <h5 className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
            Assumptions
          </h5>
          <ul className="mt-2 grid gap-2">
            {target.assumptions.map((assumption, index) => (
              <li key={index} className="rounded-lg border border-border/60 p-3 text-xs">
                <p>{assumption.text}</p>
              </li>
            ))}
          </ul>
        </div>
      )}
    </section>
  )
}

function GovernanceTimeline({ state }: { state: TargetStateView }) {
  const { target, latestReview, latestInvalidation } = state
  return (
    <section className="rounded-xl border border-border bg-background/45 p-4">
      <div className="flex items-center gap-2">
        <CalendarClock className="size-4 text-primary" aria-hidden="true" />
        <h4 className="text-sm font-semibold">Version and review history</h4>
      </div>
      <dl className="mt-3 grid gap-3 text-xs sm:grid-cols-2 xl:grid-cols-1 2xl:grid-cols-2">
        <TimelineFact label="Created" value={formatTimestamp(target.createdAt)} />
        <TimelineFact label="Effective" value={formatTimestamp(target.effectiveAt)} />
        <TimelineFact label="Horizon" value={formatTimestamp(target.horizonAt)} />
        <TimelineFact label="Review due" value={formatTimestamp(target.reviewDueAt)} />
        <TimelineFact label="Expires" value={formatTimestamp(target.expiresAt)} />
        <TimelineFact
          label="Supersedes"
          value={
            target.supersedes
              ? `Revision ${target.supersedes.revision} at ${formatTimestamp(target.supersedes.supersededAt)}`
              : "No prior revision"
          }
        />
      </dl>

      <div className="mt-4 border-t border-border pt-3">
        <h5 className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
          Latest review
        </h5>
        {latestReview ? (
          <div className="mt-2 text-xs leading-5">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <span>{latestReview.reviewer} · {formatTimestamp(latestReview.reviewedAt)}</span>
              <StateLabel value={latestReview.disposition} />
            </div>
          </div>
        ) : (
          <p className="mt-2 text-xs text-muted-foreground">No review has been recorded.</p>
        )}
      </div>

      {latestInvalidation && (
        <div className="mt-4 border-t border-border pt-3">
          <h5 className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
            Latest invalidation
          </h5>
          <div className="mt-2 flex flex-wrap items-center justify-between gap-2 text-xs">
            <span>
              {latestInvalidation.actor ?? "Legacy record without principal"} · {formatTimestamp(latestInvalidation.observedAt)}
            </span>
            <StateLabel value={latestInvalidation.kind} />
          </div>
        </div>
      )}
    </section>
  )
}

function RiskInvalidation({ state }: { state: TargetStateView }) {
  const { target } = state
  return (
    <section className="rounded-xl border border-border bg-background/45 p-4">
      <div className="flex items-center gap-2">
        <ShieldAlert className="size-4 text-primary" aria-hidden="true" />
        <h4 className="text-sm font-semibold">Risks and invalidators</h4>
      </div>
      <TextList title="Risks" items={target.risks} empty="No risk statements are recorded." />
      <TextList
        title="Invalidation conditions"
        items={target.invalidationConditions}
        empty="No invalidation conditions are recorded."
      />
    </section>
  )
}

function ExecutionBoundary() {
  return (
    <Alert>
      <ShieldAlert aria-hidden="true" />
      <AlertTitle>Research only</AlertTitle>
      <AlertDescription>
        Review state does not confer execution authority. No order controls are available here.
        Every review or invalidation requires authorization and is recorded in the target history.
      </AlertDescription>
    </Alert>
  )
}

function TextList({
  title,
  items,
  empty,
}: {
  title: string
  items: string[]
  empty: string
}) {
  return (
    <div className="mt-4">
      <h5 className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">{title}</h5>
      {items.length === 0 ? (
        <p className="mt-2 text-xs text-muted-foreground">{empty}</p>
      ) : (
        <ul className="mt-2 list-disc space-y-2 pl-4 text-xs leading-5">
          {items.map((item, index) => <li key={`${item}:${index}`}>{item}</li>)}
        </ul>
      )}
    </div>
  )
}

function PriceFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-primary/15 bg-background/55 p-3">
      <dt className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">{label}</dt>
      <dd className="mt-1 text-sm font-semibold tabular-nums">{value}</dd>
    </div>
  )
}

function TimelineFact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">{label}</dt>
      <dd className="mt-1 font-medium">{value}</dd>
    </div>
  )
}

function TargetPrompt() {
  return (
    <div className="mt-4 rounded-xl border border-dashed border-border p-6 text-sm leading-6 text-muted-foreground">
      Select a target to load its price judgment, supporting analysis, review state,
      expiration timestamps, and latest invalidation record.
    </div>
  )
}

function TargetLoading() {
  return (
    <div className="mt-4 space-y-3" aria-label="Loading target history">
      <Skeleton className="h-28 w-full" />
      <Skeleton className="h-64 w-full" />
    </div>
  )
}

function formatRange(lower: TargetStateView["target"]["entryLower"], upper: TargetStateView["target"]["entryUpper"]): string {
  return `${formatMoney(lower)} – ${formatMoney(upper)}`
}

function isPast(timestamp: string): boolean {
  const value = timestampFromUnixNanos(timestamp)
  return value !== null && value.valueOf() <= Date.now()
}
