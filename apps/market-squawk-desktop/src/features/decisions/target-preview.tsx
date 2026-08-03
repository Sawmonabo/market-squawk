import type * as React from "react"
import { CheckCircle2, Clock3, FileCheck2, RefreshCw } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { formatMoney, humanize } from "@/lib/formatters"
import { formatTimestamp } from "@/lib/time"

import { EvidenceIdentity, StateLabel } from "./decision-boundaries"
import { evidenceKey, TARGET_PRICE_FIELDS } from "./target-builder-model"
import type {
  PreparedTargetView,
  TargetCommitOutcome,
} from "./target-preparation-contracts"

export function PreparedTargetPreview({
  preview,
  expired,
  committing,
  onCommit,
  onDiscard,
}: {
  preview: PreparedTargetView
  expired: boolean
  committing: boolean
  onCommit: () => void
  onDiscard: () => void
}) {
  return (
    <section className="rounded-xl border border-amber-400/35 bg-amber-400/5" aria-labelledby="target-preview-heading">
      <header className="flex flex-wrap items-start justify-between gap-3 border-b border-amber-400/20 p-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.14em] text-amber-300">
            Human confirmation required
          </p>
          <h3 id="target-preview-heading" className="mt-1 text-base font-semibold">
            Review the complete prepared target
          </h3>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            Nothing is committed until you confirm this exact server-owned preview.
          </p>
        </div>
        <StateLabel value={expired ? "receipt expired" : `revision ${preview.revision} prepared`} />
      </header>

      <div className="grid gap-5 p-4 xl:grid-cols-2">
        <PreviewSection title="Identity and authority">
          <PreviewGrid>
            <Fact label="Target series" value={preview.targetId} evidence />
            <Fact label="Revision" value={String(preview.revision)} />
            <Fact label="Instrument" value={preview.instrumentId} />
            <Fact label="Retained dossier" value={preview.dossierId} evidence />
            <Fact label="Author" value={preview.author} />
            <Fact label="Ruleset" value={`Version ${preview.rulesetVersion}`} />
            <Fact label="Intent" value={humanize(preview.intent)} />
            <Fact label="Method" value={humanize(preview.method)} />
          </PreviewGrid>
        </PreviewSection>

        <PreviewSection title="Server-owned timing">
          <PreviewGrid>
            <Fact label="Prepared" value={formatTimestamp(preview.createdAt)} />
            <Fact label="Review due" value={formatTimestamp(preview.reviewDueAt)} />
            <Fact label="Target horizon" value={formatTimestamp(preview.horizonAt)} />
            <Fact label="Target expires" value={formatTimestamp(preview.expiresAt)} />
            <Fact label="Confirmation expires" value={formatTimestamp(preview.receiptExpiresAt)} />
            <Fact label="Admission receipt" value={preview.receiptId} evidence />
          </PreviewGrid>
        </PreviewSection>

        <PreviewSection title="Observed reference mark">
          <p className="text-lg font-semibold tabular-nums">{formatMoney(preview.referenceMark)}</p>
          <p className="mt-1 text-xs text-muted-foreground">
            {preview.referenceMarkSource} · {humanize(preview.referenceMarkQuality)} · observed{" "}
            {formatTimestamp(preview.referenceMarkObservedAt)}
          </p>
        </PreviewSection>

        <PreviewSection title="Selected evidence">
          <PreviewGrid>
            <Fact label="Forecast" value={preview.forecastSelected ? "Selected" : "Not selected"} />
            <Fact label="Fair value" value={preview.fairValueSelected ? "Selected" : "Not selected"} />
            <Fact label="Portfolio" value={preview.portfolioSelected ? "Selected" : "Not selected"} />
          </PreviewGrid>
        </PreviewSection>

        <PreviewSection title="Complete price ladder" wide>
          <dl className="grid gap-2 sm:grid-cols-2 lg:grid-cols-5">
            {TARGET_PRICE_FIELDS.map(({ key, label }) => (
              <div key={key} className="rounded-lg border border-border bg-background/55 p-3">
                <dt className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground">{label}</dt>
                <dd className="mt-1 text-sm font-semibold tabular-nums">
                  {formatMoney(preview.prices[key])}
                </dd>
              </div>
            ))}
          </dl>
        </PreviewSection>

        <PreviewSection title="Investment thesis" wide>
          <p className="text-sm leading-6">{preview.thesis}</p>
        </PreviewSection>

        <PreviewSection title="Evidence-bound assumptions">
          <PreviewList
            values={preview.assumptions.map(
              (assumption) =>
                `${assumption.text} — ${assumptionEvidenceLabel(assumption.evidence)}`,
            )}
          />
        </PreviewSection>
        <PreviewSection title="Risks">
          <PreviewList values={preview.risks} />
        </PreviewSection>
        <PreviewSection title="Invalidation conditions" wide>
          <PreviewList values={preview.invalidationConditions} />
        </PreviewSection>
      </div>

      <footer className="flex flex-wrap items-center justify-between gap-3 border-t border-amber-400/20 p-4">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Clock3 className="size-4" aria-hidden="true" />
          {expired
            ? "This receipt can no longer be committed. Prepare a fresh preview."
            : "Confirmation consumes this one-use receipt and appends immutable history."}
        </div>
        <div className="flex gap-2">
          <Button type="button" variant="outline" onClick={onDiscard}>Edit draft</Button>
          <Button type="button" disabled={expired || committing} onClick={onCommit}>
            {committing ? <RefreshCw className="animate-spin" aria-hidden="true" /> : <FileCheck2 aria-hidden="true" />}
            Confirm and commit revision {preview.revision}
          </Button>
        </div>
      </footer>
    </section>
  )
}

export function TargetCommitReceipt({
  outcome,
  targetId,
  revision,
}: {
  outcome: TargetCommitOutcome
  targetId: string
  revision: number
}) {
  return (
    <Alert className="border-emerald-500/35 bg-emerald-500/5" role="status">
      <CheckCircle2 className="text-emerald-400" aria-hidden="true" />
      <AlertTitle>
        {outcome === "appended"
          ? "Target revision committed"
          : "This exact target revision was already committed"}
      </AlertTitle>
      <AlertDescription>
        <span className="block">Target {targetId} revision {revision} is retained in immutable history.</span>
        <EvidenceIdentity value={targetId} />
      </AlertDescription>
    </Alert>
  )
}

function PreviewSection({
  title,
  wide = false,
  children,
}: {
  title: string
  wide?: boolean
  children: React.ReactNode
}) {
  return (
    <section className={`rounded-xl border border-border bg-background/45 p-4 ${wide ? "xl:col-span-2" : ""}`}>
      <h4 className="text-xs font-semibold uppercase tracking-[0.1em] text-muted-foreground">{title}</h4>
      <div className="mt-3">{children}</div>
    </section>
  )
}

function PreviewGrid({ children }: { children: React.ReactNode }) {
  return <dl className="grid gap-3 sm:grid-cols-2">{children}</dl>
}

function Fact({ label, value, evidence = false }: { label: string; value: string; evidence?: boolean }) {
  return (
    <div className="min-w-0">
      <dt className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground">{label}</dt>
      <dd className="mt-1 text-xs font-medium">
        {evidence ? <EvidenceIdentity value={value} /> : value}
      </dd>
    </div>
  )
}

function PreviewList({ values }: { values: string[] }) {
  return (
    <ol className="list-decimal space-y-2 pl-4 text-xs leading-5">
      {values.map((value, index) => <li key={`${value}:${index}`}>{value}</li>)}
    </ol>
  )
}

function assumptionEvidenceLabel(
  value: PreparedTargetView["assumptions"][number]["evidence"],
): string {
  return value.kind === "dossier_reference"
    ? `Dossier reference ${value.index + 1}`
    : humanize(evidenceKey(value))
}
