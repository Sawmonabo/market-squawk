import { Box, Database, ShieldCheck } from "lucide-react"

import { humanize } from "@/lib/formatters"
import type { LosslessInteger } from "@/lib/lossless-integer"
import { formatTimestamp } from "@/lib/time"

import type { ModelBundle, ModelMetadata } from "./models-contracts"

export function BundleEvidence({
  bundle,
  metadata,
  metadataAvailable,
  loading,
  error,
}: {
  bundle: ModelBundle | null
  metadata: ModelMetadata | null
  metadataAvailable: boolean
  loading: boolean
  error: string | null
}) {
  if (!bundle) {
    return (
      <EvidencePanel
        title="No admitted bundle selected"
        detail="Select a model to review its intended use, validation, limitations, and readiness."
      />
    )
  }

  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-wider text-primary">
            Research model
          </p>
          <h2 className="mt-2 text-xl font-semibold">
            {bundle.bundleId} <span className="text-muted-foreground">v{bundle.bundleVersion}</span>
          </h2>
          <p className="mt-1 font-mono text-[11px] text-muted-foreground">
            Model {short(bundle.modelId)}
          </p>
        </div>
        <span className="inline-flex items-center gap-1.5 rounded-full border border-emerald-400/30 bg-emerald-400/10 px-2.5 py-1 text-[10px] font-medium uppercase tracking-wider text-emerald-300">
          <ShieldCheck className="size-3" aria-hidden="true" />
          Admitted
        </span>
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MiniFact icon={Box} label="Format" value={`${humanize(bundle.format)} v${bundle.formatVersion}`} />
        <MiniFact
          icon={Database}
          label="Training evidence"
          value="Available"
        />
        <MiniFact icon={ShieldCheck} label="Failure response" value="No action" />
      </div>

      <dl className="mt-5 grid gap-x-6 gap-y-3 border-t border-border pt-4 sm:grid-cols-2">
        <Fact label="Training period available" value="Yes" />
        <Fact label="Inference failure" value="No action" />
      </dl>

      {!metadataAvailable ? (
        <EvidenceNotice text="Validation, inputs, and intended-use information are unavailable." />
      ) : loading ? (
        <EvidenceNotice text="Loading complete admitted metadata…" />
      ) : error ? (
        <EvidenceNotice text="Model details could not be loaded right now." />
      ) : metadata ? (
        <MetadataEvidence metadata={metadata} />
      ) : (
        <EvidenceNotice text="No complete metadata was returned for this admitted model." />
      )}
      <p className="mt-3 text-[11px] leading-5 text-muted-foreground">
        If the model cannot produce a valid result, it suggests no action.
      </p>
    </section>
  )
}

function MetadataEvidence({ metadata }: { metadata: ModelMetadata }) {
  return (
    <div className="mt-5 space-y-4 border-t border-border pt-4">
      <div>
        <h3 className="text-sm font-semibold">Validation and intended use</h3>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          {metadata.intendedUse}
        </p>
      </div>
      <dl className="grid gap-x-6 gap-y-3 sm:grid-cols-2 xl:grid-cols-3">
        <Fact
          label="Training period"
          value={`${formatNanos(metadata.trainingPeriod.startUnixNanos)} → ${formatNanos(metadata.trainingPeriod.endUnixNanos)}`}
        />
        <Fact
          label="Dataset selection"
          value={`${metadata.trainingDataset.selectedComponentRows.toLocaleString()} rows · ${formatNanos(metadata.trainingDataset.selectionAsOfUnixNanos)}`}
        />
        <Fact
          label="Universe / label"
          value={`${metadata.universeId} · ${metadata.label.name} v${metadata.label.version} (${humanize(metadata.label.kind)})`}
        />
        <Fact label="Validation status" value="Available" />
      </dl>
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {metadata.validationMetrics.map((metric) => (
          <div key={metric.name} className="rounded-lg border border-border bg-background/35 p-3">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
              {humanize(metric.name)}
            </p>
            <p className="mt-1 font-mono text-lg font-semibold">
              {metric.value.toLocaleString(undefined, { maximumFractionDigits: 6 })}
            </p>
          </div>
        ))}
      </div>
      <div>
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Feature contract
        </p>
        <ul className="mt-2 grid gap-2 sm:grid-cols-2">
          {metadata.features.map((feature) => (
            <li key={`${feature.name}:${feature.version}`} className="rounded-lg border border-border bg-background/35 p-3">
              <p className="text-xs font-medium">
                {feature.name} <span className="text-muted-foreground">v{feature.version}</span>
              </p>
              <p className="mt-1 text-[11px] text-muted-foreground">
                {feature.normalizer.kind === "standard"
                  ? `Standardized · mean ${feature.normalizer.mean} · scale ${feature.normalizer.scale}`
                  : "Identity normalization"}
              </p>
            </li>
          ))}
        </ul>
      </div>
      <div className="rounded-lg border border-amber-400/25 bg-amber-400/5 p-3">
        <p className="text-xs font-medium text-amber-200">Declared limitations</p>
        {metadata.limitations.length === 0 ? (
          <p className="mt-1 text-xs text-muted-foreground">No limitation text was supplied.</p>
        ) : (
          <ul className="mt-2 list-disc space-y-1 pl-4 text-xs leading-5 text-muted-foreground">
            {metadata.limitations.map((limitation) => (
              <li key={limitation}>{limitation}</li>
            ))}
          </ul>
        )}
      </div>
      <ModelReadiness metadata={metadata} />
      <TrainingAndCohortEvidence metadata={metadata} />
    </div>
  )
}

function ModelReadiness({ metadata }: { metadata: ModelMetadata }) {
  return (
    <div className="grid gap-3 lg:grid-cols-2">
      <div className="rounded-lg border border-emerald-400/25 bg-emerald-400/5 p-3">
        <p className="text-xs font-medium text-emerald-200">Research readiness</p>
        <dl className="mt-3 grid gap-3 sm:grid-cols-2">
          <Fact label="Status" value={humanize(metadata.admissionEvidence.status)} />
          <Fact label="Use" value="Investment research only" />
        </dl>
        <p className="mt-3 text-xs leading-5 text-muted-foreground">
          {metadata.admissionEvidence.rejectionPolicy}
        </p>
      </div>
      <div className="rounded-lg border border-sky-400/25 bg-sky-400/5 p-3">
        <p className="text-xs font-medium text-sky-200">Current availability</p>
        <dl className="mt-3 grid gap-3 sm:grid-cols-2">
          <Fact label="Status" value={humanize(metadata.runtimeHealth.status)} />
        </dl>
        <p className="mt-3 text-xs leading-5 text-muted-foreground">
          If unavailable or unhealthy, the model suggests no action.
        </p>
      </div>
    </div>
  )
}

function TrainingAndCohortEvidence({ metadata }: { metadata: ModelMetadata }) {
  const evidence = metadata.trainingEvidence
  return (
    <div className="rounded-lg border border-violet-400/25 bg-violet-400/5 p-3">
      <p className="text-xs font-medium text-violet-200">Training and cohort evidence</p>
      <dl className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Fact label="Train / validation / test" value={`${evidence.splits.train.toLocaleString()} / ${evidence.splits.validation.toLocaleString()} / ${evidence.splits.test.toLocaleString()}`} mono />
        <Fact label="Seed / missing policy" value={`${evidence.seed.toLocaleString()} · ${humanize(evidence.missingPolicy)}`} />
      </dl>
      {evidence.forecastSchedule ? (
        <p className="mt-3 text-xs leading-5 text-muted-foreground">
          Forecast schedule: {humanize(evidence.forecastSchedule.strategy)} · horizons {evidence.forecastSchedule.horizons.join(", ")} · {evidence.forecastSchedule.rollingSplits} rolling splits.
        </p>
      ) : null}
      <ul className="mt-3 grid gap-2 sm:grid-cols-2">
        {evidence.cohortEvidence.map((cohort) => (
          <li key={cohort.dimension} className="rounded-md border border-border bg-background/25 p-2.5">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
              {humanize(cohort.dimension)} · {humanize(cohort.status)}
            </p>
            <p className="mt-1 text-xs leading-5 text-muted-foreground">{cohort.reason}</p>
          </li>
        ))}
      </ul>
    </div>
  )
}

function EvidenceNotice({ text }: { text: string }) {
  return (
    <div className="mt-4 rounded-lg border border-amber-400/25 bg-amber-400/5 p-3 text-xs leading-5 text-muted-foreground">
      {text}
    </div>
  )
}

function MiniFact({
  icon: Icon,
  label,
  value,
  mono = false,
}: {
  icon: typeof Box
  label: string
  value: string
  mono?: boolean
}) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-3">
      <Icon className="size-3.5 text-muted-foreground" aria-hidden="true" />
      <p className="mt-2 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className={`mt-1 text-xs font-medium ${mono ? "font-mono" : ""}`}>{value}</p>
    </div>
  )
}

function Fact({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className={`mt-1 text-xs ${mono ? "font-mono" : ""}`}>{value}</dd>
    </div>
  )
}

function EvidencePanel({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <h2 className="text-sm font-semibold">{title}</h2>
      <p className="mt-2 text-sm leading-6 text-muted-foreground">{detail}</p>
    </section>
  )
}

function short(value: string): string {
  return value.length <= 18 ? value : `${value.slice(0, 10)}…${value.slice(-6)}`
}

function formatNanos(value: LosslessInteger): string {
  return formatTimestamp(value)
}
