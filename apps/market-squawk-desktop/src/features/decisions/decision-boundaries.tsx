import { Ban, DatabaseZap, ShieldCheck } from "lucide-react"

import { humanize } from "@/lib/formatters"

export function DecisionBoundaries() {
  return (
    <section
      aria-labelledby="decision-boundaries-heading"
      className="grid gap-3 lg:grid-cols-3"
    >
      <BoundaryCard
        icon={DatabaseZap}
        title="Evidence stays identified"
        detail="Observed marks and source identities remain distinct from forecasts, fair-value analyses, and portfolio revisions."
      />
      <BoundaryCard
        icon={ShieldCheck}
        title="Targets are governed judgment"
        detail="Cases and entry, trim, and exit ranges are versioned research judgments with review and invalidation evidence."
      />
      <BoundaryCard
        icon={Ban}
        title="No execution authority"
        detail="A target never submits an order, bypasses portfolio risk, promotes a model output, or authorizes execution."
      />
      <h2 id="decision-boundaries-heading" className="sr-only">
        Decision evidence boundaries
      </h2>
    </section>
  )
}

export function EvidenceIdentity({ value }: { value: string }) {
  return (
    <span className="block truncate font-mono text-[10px] text-muted-foreground" title={value}>
      {value}
    </span>
  )
}

export function StateLabel({ value }: { value: string }) {
  return (
    <span className="inline-flex rounded-full border border-border bg-background/70 px-2 py-1 font-mono text-[9px] uppercase tracking-[0.12em] text-muted-foreground">
      {humanize(value)}
    </span>
  )
}

function BoundaryCard({
  icon: Icon,
  title,
  detail,
}: {
  icon: typeof Ban
  title: string
  detail: string
}) {
  return (
    <article className="rounded-xl border border-border bg-card/45 p-4">
      <Icon className="size-4 text-primary" aria-hidden="true" />
      <h3 className="mt-3 text-sm font-semibold">{title}</h3>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">{detail}</p>
    </article>
  )
}
