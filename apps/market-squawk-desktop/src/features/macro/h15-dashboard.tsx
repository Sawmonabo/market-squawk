import * as React from "react"
import {
  AlertCircle,
  CheckCircle2,
  CircleOff,
  Database,
  RefreshCw,
  ShieldCheck,
  type LucideIcon,
} from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { humanize } from "@/lib/formatters"

import type {
  MacroDashboard,
  MacroDashboardObservation,
  MacroDashboardSourceReadiness,
} from "./contracts"

export type H15DashboardState =
  | { status: "loading" }
  | { status: "error"; message: string; onRetry?: () => void }
  | {
      status: "ready"
      dashboard: MacroDashboard
      sourceReadiness?: MacroDashboardSourceReadiness | null
    }

export interface H15DashboardProps {
  state: H15DashboardState
}

/**
 * Presents a parsed, backend-authoritative H.15 projection.
 *
 * Data loading remains outside this component so page composition can use the same loader in
 * native and browser presentations without granting the WebView provider or dataset authority.
 */
export function H15Dashboard({ state }: H15DashboardProps) {
  if (state.status === "loading") return <H15DashboardLoading />
  if (state.status === "error") {
    return (
      <section aria-labelledby="h15-dashboard-title">
        <Alert variant="destructive">
          <AlertCircle aria-hidden="true" />
          <AlertTitle id="h15-dashboard-title">
            H.15 publication could not be read
          </AlertTitle>
          <AlertDescription>
            <p>{state.message}</p>
            {state.onRetry ? (
              <Button
                className="mt-2"
                size="sm"
                variant="outline"
                onClick={state.onRetry}
              >
                <RefreshCw aria-hidden="true" />
                Retry
              </Button>
            ) : null}
          </AlertDescription>
        </Alert>
      </section>
    )
  }

  const { dashboard, sourceReadiness = null } = state
  const { binding, release, selection } = dashboard

  return (
    <section
      className="rounded-xl border border-border bg-card/45"
      aria-labelledby="h15-dashboard-title"
    >
      <header className="border-b border-border p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
              Federal Reserve Board macro dashboard
            </p>
            <h2 id="h15-dashboard-title" className="mt-2 text-xl font-semibold">
              {release.title}
            </h2>
            <p className="mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">
              Latest available locally from the immutable analytical publication. Values are
              official delayed, business-daily observations and are not real-time quotes.
            </p>
          </div>
          <EvidenceBadge tone="neutral">Official delayed</EvidenceBadge>
        </div>

        <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <ReadinessCard
            icon={Database}
            title="Stored publication"
            label="Queryable"
            detail={`Complete ${selection.returnedSeries}-maturity H.15 representation`}
            tone="good"
          />
          <ReadinessCard
            icon={sourceReadiness?.state === "active" ? CheckCircle2 : CircleOff}
            title="Source readiness"
            label={sourceReadiness?.label ?? "Not reported"}
            detail={
              sourceReadiness?.detail ??
              "Provider runtime readiness was not supplied with this stored publication."
            }
            tone={sourceReadiness?.state === "active" ? "good" : "neutral"}
          />
          <ReadinessCard
            icon={ShieldCheck}
            title="Observation values"
            label={`${selection.availableSeries} observed`}
            detail={`${selection.missingSeries} explicit provider missing`}
            tone={selection.missingSeries === 0 ? "good" : "neutral"}
          />
          <ReadinessCard
            icon={RefreshCw}
            title="Server evaluated"
            label={selection.evaluatedAt}
            detail={selection.policy}
            tone="neutral"
          />
        </div>
      </header>

      <div className="p-5">
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {dashboard.observations.map((observation) => (
            <ObservationCard key={observation.slot} observation={observation} />
          ))}
        </div>

        <details className="mt-5 rounded-lg border border-border bg-background/30 p-4">
          <summary className="cursor-pointer text-xs font-semibold focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary">
            Publication, timing, and source evidence
          </summary>
          <p className="mt-3 max-w-4xl text-[11px] leading-5 text-muted-foreground">
            Stored publication readiness and source runtime readiness are separate. A durable
            manifest can remain queryable while acquisition is inactive. “Available” below means
            first observed locally; it does not claim a provider publication timestamp or provider
            vintage chronology.
          </p>
          <dl className="mt-4 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <EvidenceFact label="Surface" value={binding.surfaceId} mono />
            <EvidenceFact label="Source" value={binding.sourceId} mono />
            <EvidenceFact
              label="Provider dataset"
              value={binding.providerDatasetId}
              mono
            />
            <EvidenceFact
              label="Analytical dataset"
              value={binding.analyticalDatasetId}
              mono
            />
            <EvidenceFact
              label="Manifest"
              value={`v${binding.manifest.manifestVersion}`}
            />
            <EvidenceFact
              label="Manifest schema"
              value={`${binding.manifest.schema.name} v${binding.manifest.schema.version}`}
            />
            <EvidenceFact label="Release family" value={release.family} mono />
            <EvidenceFact label="Frequency" value={humanize(release.frequency)} />
            <EvidenceFact
              label="Source lifecycle observed"
              value={
                sourceReadiness?.lifecycleObservedAt
                  ? sourceReadiness.lifecycleObservedAt
                  : "Not reported"
              }
            />
            <EvidenceFact
              label="Source runtime observed"
              value={
                sourceReadiness?.runtimeObservedAt
                  ? sourceReadiness.runtimeObservedAt
                  : "Not reported"
              }
            />
            <EvidenceFact
              label="Manifest content SHA-256"
              value={binding.manifest.contentHash}
              mono
            />
            <EvidenceFact
              label="Schema fingerprint"
              value={binding.manifest.schema.fingerprint}
              mono
            />
            <EvidenceFact
              label="Object graph digest"
              value={binding.objectGraphDigest}
              mono
            />
            <EvidenceFact label="Query identity" value={binding.queryIdentity} mono />
            <EvidenceFact
              label="Pinned query result digest"
              value={binding.resultDigest}
              mono
            />
            <EvidenceFact
              label="Final typed selection digest"
              value={selection.selectionDigest}
              mono
            />
          </dl>
        </details>
      </div>
    </section>
  )
}

function ObservationCard({
  observation,
}: {
  observation: MacroDashboardObservation
}) {
  const value = observation.observation

  return (
    <article className="rounded-lg border border-border bg-background/35 p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
            {observation.slot}
          </p>
          <h3 className="mt-1 text-sm font-semibold">{observation.label}</h3>
        </div>
        <EvidenceBadge tone={value.state === "observed" ? "good" : "neutral"}>
          {value.state === "observed" ? "Observed" : "Missing"}
        </EvidenceBadge>
      </div>

      {value.state === "observed" ? (
        <p className="mt-5 break-words font-mono text-2xl font-semibold tabular-nums">
          {value.decimal}%
        </p>
      ) : (
        <div className="mt-5">
          <p className="font-mono text-lg font-semibold text-muted-foreground">
            {value.marker}
          </p>
          <p className="mt-1 text-[10px] leading-4 text-muted-foreground">
            {value.reason ?? "Provider reported no observation."}
          </p>
        </div>
      )}

      <dl className="mt-5 grid gap-3 border-t border-border/70 pt-4 sm:grid-cols-2">
        <EvidenceFact label="Effective date" value={observation.effectiveDate} />
        <EvidenceFact
          label="First observed locally"
          value={observation.availableAt}
        />
      </dl>

      <details className="mt-4 border-t border-border/70 pt-3">
        <summary className="cursor-pointer font-mono text-[9px] uppercase tracking-wider text-muted-foreground focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary">
          Observation evidence
        </summary>
        <dl className="mt-3 grid gap-3">
          <EvidenceFact label="Series" value={observation.seriesId} mono />
          <EvidenceFact label="Unit" value={observation.unitId} mono />
          <EvidenceFact
            label="Revision"
            value={String(observation.revision)}
            mono
          />
          <EvidenceFact
            label="Source identifier"
            value={observation.sourceIdentifier}
            mono
          />
          <EvidenceFact
            label="Source payload SHA-256"
            value={observation.sourcePayloadDigest}
            mono
          />
        </dl>
      </details>
    </article>
  )
}

function H15DashboardLoading() {
  return (
    <section
      className="rounded-xl border border-border bg-card/45 p-5"
      aria-labelledby="h15-dashboard-loading-title"
      aria-busy="true"
    >
      <p
        id="h15-dashboard-loading-title"
        className="text-sm font-semibold text-muted-foreground"
      >
        Loading H.15 publication evidence
      </p>
      <Skeleton className="mt-4 h-24 rounded-xl" />
      <div className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }, (_, index) => (
          <Skeleton key={index} className="h-36 rounded-lg" />
        ))}
      </div>
    </section>
  )
}

function ReadinessCard({
  icon: Icon,
  title,
  label,
  detail,
  tone,
}: {
  icon: LucideIcon
  title: string
  label: string
  detail: string
  tone: "good" | "neutral"
}) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-4">
      <div className="flex items-center gap-2">
        <Icon
          className={tone === "good" ? "text-[var(--success)]" : "text-muted-foreground"}
          aria-hidden="true"
        />
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
          {title}
        </p>
      </div>
      <p className="mt-3 text-sm font-semibold">{label}</p>
      <p className="mt-1 break-words text-[10px] leading-4 text-muted-foreground">
        {detail}
      </p>
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
      <dt className="text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </dt>
      <dd
        className={`mt-1 break-all text-[11px] leading-4 text-foreground/85 ${
          mono ? "font-mono" : ""
        }`}
      >
        {value}
      </dd>
    </div>
  )
}

function EvidenceBadge({
  children,
  tone,
}: {
  children: React.ReactNode
  tone: "good" | "neutral"
}) {
  return (
    <span
      className={`rounded border px-2 py-1 text-[9px] font-medium uppercase tracking-wider ${
        tone === "good"
          ? "border-[var(--success)]/35 text-[var(--success)]"
          : "border-border text-muted-foreground"
      }`}
    >
      {children}
    </span>
  )
}
