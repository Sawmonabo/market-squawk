import { Box, Database, ShieldCheck } from "lucide-react"

import { formatTimestamp } from "@/lib/time"

import type { ModelEvidence } from "./models-contracts"

export function BundleEvidence({
  model,
  available,
  loading,
  error,
}: {
  model: ModelEvidence | null
  available: boolean
  loading: boolean
  error: string | null
}) {
  if (!available) {
    return (
      <EvidencePanel
        title="Model evidence is unavailable"
        detail="Validation, purpose, limitations, and out-of-sample evidence are not available in this installation."
      />
    )
  }
  if (loading) {
    return <EvidencePanel title="Loading model evidence…" detail="" />
  }
  if (error) {
    return <EvidencePanel title="Model evidence is unavailable" detail={error} />
  }
  if (!model) {
    return (
      <EvidencePanel
        title="No reviewed model is selected"
        detail="Select a model to review its purpose, validation, limitations, and out-of-sample evidence."
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
          <h2 className="mt-2 text-xl font-semibold">{model.label}</h2>
        </div>
        <span className="inline-flex items-center gap-1.5 rounded-full border border-border px-2.5 py-1 text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
          <ShieldCheck className="size-3" aria-hidden="true" />
          {evidenceLabel(model.evidenceState)}
        </span>
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MiniFact icon={Box} label="Use" value="Investment research" />
        <MiniFact
          icon={Database}
          label="Out-of-sample observations"
          value={model.training.outOfSampleObservations.toLocaleString()}
        />
        <MiniFact
          icon={ShieldCheck}
          label="Rolling evaluation folds"
          value={model.training.rollingOutOfSampleFolds.toLocaleString()}
        />
        <MiniFact icon={ShieldCheck} label="If unavailable" value="No action" />
      </div>

      <div className="mt-5 space-y-4 border-t border-border pt-4">
        <div>
          <h3 className="text-sm font-semibold">Purpose</h3>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            {model.intendedUse}
          </p>
        </div>
        <dl className="grid gap-x-6 gap-y-3 sm:grid-cols-2 xl:grid-cols-3">
          <Fact
            label="Training period"
            value={`${formatTimestamp(model.training.observedFromUnixNanos)} through ${formatTimestamp(model.training.observedThroughUnixNanos)}`}
          />
          <Fact
            label="Information available by"
            value={formatTimestamp(model.training.availableAtUnixNanos)}
          />
          <Fact
            label="Train / validation / out-of-sample"
            value={`${model.training.trainingObservations.toLocaleString()} / ${model.training.validationObservations.toLocaleString()} / ${model.training.outOfSampleObservations.toLocaleString()}`}
          />
          <Fact
            label="Evaluated horizons"
            value={model.training.evaluatedHorizons.toLocaleString()}
          />
        </dl>

        {model.validation.length > 0 ? (
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            {model.validation.map((metric) => (
              <div
                key={metric.label}
                className="rounded-lg border border-border bg-background/35 p-3"
              >
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                  {metric.label}
                </p>
                <p className="mt-1 font-mono text-lg font-semibold">
                  {metric.value}
                </p>
                <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
                  {metric.interpretation}
                </p>
              </div>
            ))}
          </div>
        ) : (
          <EvidenceNotice text="No reviewable validation measures are available." />
        )}

        <div className="rounded-lg border border-violet-400/25 bg-violet-400/5 p-3">
          <p className="text-xs font-medium text-violet-200">
            Out-of-sample coverage
          </p>
          {model.coverage.length > 0 ? (
            <ul className="mt-3 grid gap-2 sm:grid-cols-2">
              {model.coverage.map((coverage) => (
                <li
                  key={coverage.label}
                  className="rounded-md border border-border bg-background/25 p-2.5"
                >
                  <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                    {coverage.label} · {evidenceLabel(coverage.state)}
                  </p>
                  <p className="mt-1 text-xs leading-5 text-muted-foreground">
                    {coverage.interpretation}
                  </p>
                </li>
              ))}
            </ul>
          ) : (
            <p className="mt-2 text-xs text-muted-foreground">
              Coverage evidence is unavailable.
            </p>
          )}
        </div>

        <div className="rounded-lg border border-amber-400/25 bg-amber-400/5 p-3">
          <p className="text-xs font-medium text-amber-200">Known limitations</p>
          {model.limitations.length > 0 ? (
            <ul className="mt-2 list-disc space-y-1 pl-4 text-xs leading-5 text-muted-foreground">
              {model.limitations.map((limitation) => (
                <li key={limitation}>{limitation}</li>
              ))}
            </ul>
          ) : (
            <p className="mt-1 text-xs text-muted-foreground">
              No additional limitations were supplied.
            </p>
          )}
        </div>
      </div>
      <p className="mt-3 text-[11px] leading-5 text-muted-foreground">
        A validation measure or model score alone is never treated as confidence.
        If usable evidence is unavailable, Market Squawk suggests no action.
      </p>
    </section>
  )
}

function EvidenceNotice({ text }: { text: string }) {
  return (
    <div className="rounded-lg border border-amber-400/25 bg-amber-400/5 p-3 text-xs leading-5 text-muted-foreground">
      {text}
    </div>
  )
}

function MiniFact({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Box
  label: string
  value: string
}) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-3">
      <Icon className="size-3.5 text-muted-foreground" aria-hidden="true" />
      <p className="mt-2 text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 text-xs font-medium">{value}</p>
    </div>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </dt>
      <dd className="mt-1 text-xs">{value}</dd>
    </div>
  )
}

function EvidencePanel({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <h2 className="text-sm font-semibold">{title}</h2>
      {detail ? (
        <p className="mt-2 text-sm leading-6 text-muted-foreground">{detail}</p>
      ) : null}
    </section>
  )
}

function evidenceLabel(value: string): string {
  return value === "sufficient"
    ? "Evidence available"
    : value === "evaluated"
      ? "Evaluated"
      : value === "limited"
        ? "Limited evidence"
        : "Unavailable"
}
