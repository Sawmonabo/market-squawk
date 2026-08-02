import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import {
  AlertCircle,
  CalendarClock,
  CircleDollarSign,
  RefreshCw,
  Scale,
  Search,
  ShieldAlert,
} from "lucide-react"

import { messageFrom } from "@/app/product-context"
import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney, humanize } from "@/lib/formatters"
import { formatTimestamp, timestampFromUnixNanos } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import {
  digestHex,
  parseDecisionTargets,
  type TargetStateView,
} from "./contracts"
import { EvidenceIdentity, StateLabel } from "./decision-boundaries"

export function TargetGovernanceWorkspace({
  transport,
  scope,
}: {
  transport: ProductTransport
  scope: ProductScope
}) {
  const [targetDraft, setTargetDraft] = React.useState("")
  const [targetId, setTargetId] = React.useState("")
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

  return (
    <section aria-labelledby="target-governance-heading" className="mt-8">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Append-only target authority
          </p>
          <h2 id="target-governance-heading" className="mt-1 text-lg font-semibold">
            Target ranges, review, and invalidation
          </h2>
          <p className="mt-1 max-w-3xl text-sm leading-6 text-muted-foreground">
            Inspect every known revision for one durable target-set ID. Target discovery is not
            exposed by the installed service, so the identity must come from an existing record.
          </p>
        </div>
        <div className="max-w-sm rounded-xl border border-primary/25 bg-primary/5 p-3 text-xs leading-5 text-muted-foreground">
          Prices below are research judgment ranges. They are neither forecasts nor executable orders.
        </div>
      </div>

      <form
        className="mt-4 flex max-w-2xl flex-col gap-2 sm:flex-row sm:items-end"
        onSubmit={(event) => {
          event.preventDefault()
          setTargetId(targetDraft.trim())
        }}
      >
        <label htmlFor="decision-target-id" className="flex-1 text-xs font-medium">
          Target-set ID
          <Input
            id="decision-target-id"
            value={targetDraft}
            onChange={(event) => setTargetDraft(event.target.value)}
            className="mt-2 font-mono text-xs"
            autoComplete="off"
          />
        </label>
        <Button
          type="submit"
          variant="outline"
          disabled={targets.isFetching || targetDraft.trim().length === 0}
        >
          {targets.isFetching ? (
            <RefreshCw className="animate-spin" aria-hidden="true" />
          ) : (
            <Search aria-hidden="true" />
          )}
          Load history
        </Button>
      </form>

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
          No target revisions exist for this identity.
        </div>
      ) : (
        <div className="mt-4 grid gap-4">
          {revisions.map((state) => (
            <TargetRevisionCard
              key={`${state.target.id}:${state.target.revision}`}
              state={state}
            />
          ))}
        </div>
      )}
    </section>
  )
}

function TargetRevisionCard({ state }: { state: TargetStateView }) {
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
          <EvidenceIdentity value={target.id} />
        </div>
        <div className="flex flex-wrap justify-end gap-2">
          <StateLabel value={state.status} />
          {expired && <StateLabel value="expired by recorded timestamp" />}
          {reviewOverdue && <StateLabel value="review overdue" />}
          {stale && <StateLabel value="stale evidence" />}
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
        </div>
      </div>
    </article>
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
      <div className="mt-3">
        <EvidenceIdentity value={digestHex(target.referenceIdentity)} />
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
      <dl className="mt-4 grid gap-3 sm:grid-cols-2">
        <EvidenceFact label="Target content" value={digestHex(target.targetIdentity)} />
        <EvidenceFact label="Dossier" value={target.dossierId} />
        <EvidenceFact label="Portfolio revision" value={digestHex(target.portfolioRevision)} />
        <EvidenceFact label="Forecast evidence" value={digestHex(target.forecast)} />
        <EvidenceFact label="Fair-value decision" value={target.fairValue ?? "Not bound"} />
      </dl>
      {target.assumptions.length > 0 && (
        <div className="mt-4">
          <h5 className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
            Evidence-bound assumptions
          </h5>
          <ul className="mt-2 grid gap-2">
            {target.assumptions.map((assumption, index) => (
              <li key={`${digestHex(assumption.evidenceIdentity)}:${index}`} className="rounded-lg border border-border/60 p-3 text-xs">
                <p>{assumption.text}</p>
                <EvidenceIdentity value={digestHex(assumption.evidenceIdentity)} />
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
        <h4 className="text-sm font-semibold">Version and review evidence</h4>
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
          Latest immutable review
        </h5>
        {latestReview ? (
          <div className="mt-2 text-xs leading-5">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <span>{latestReview.reviewer} · {formatTimestamp(latestReview.reviewedAt)}</span>
              <StateLabel value={latestReview.disposition} />
            </div>
            <EvidenceIdentity value={latestReview.id} />
            <EvidenceIdentity value={digestHex(latestReview.contentIdentity)} />
          </div>
        ) : (
          <p className="mt-2 text-xs text-muted-foreground">No review evidence has been appended.</p>
        )}
      </div>

      {latestInvalidation && (
        <div className="mt-4 border-t border-border pt-3">
          <h5 className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
            Latest immutable invalidation
          </h5>
          <div className="mt-2 flex flex-wrap items-center justify-between gap-2 text-xs">
            <span>{formatTimestamp(latestInvalidation.observedAt)}</span>
            <StateLabel value={latestInvalidation.kind} />
          </div>
          <EvidenceIdentity value={latestInvalidation.id} />
          <EvidenceIdentity value={digestHex(latestInvalidation.contentIdentity)} />
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
        Review mutations also require an authenticated actor and canonical content identity that
        this desktop contract does not currently supply.
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

function EvidenceFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">{label}</dt>
      <dd><EvidenceIdentity value={value} /></dd>
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
      Enter a known target-set identity to load its versioned judgment, evidence, review state,
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
