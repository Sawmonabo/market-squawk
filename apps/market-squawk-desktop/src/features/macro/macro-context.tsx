import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import { AlertCircle, CalendarClock, RefreshCw } from "lucide-react"

import { productKeys } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Skeleton } from "@/components/ui/skeleton"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  macroContextCutoffsSchema,
  parseMacroContext,
  type MacroContextData,
  type MacroContextObservation,
} from "./macro-context-contracts"
import { MacroAvailabilityNotice, MacroEvidenceBadge, MacroEvidenceFact } from "./presentation"

const operation = "Macro.GetContext"

type Cutoffs = {
  knowledgeCutoff: string
  effectiveDateCutoff: string
}

export interface MacroContextProps {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}

export function MacroContext({ bootstrap, transport }: MacroContextProps) {
  const operationAvailable = hasProductCapability(bootstrap, "macro_context")
  const [knowledgeCutoff, setKnowledgeCutoff] = React.useState("")
  const [effectiveDateCutoff, setEffectiveDateCutoff] = React.useState("")
  const [cutoffs, setCutoffs] = React.useState<Cutoffs | null>(null)
  const [requestVersion, setRequestVersion] = React.useState(0)
  const cutoffResult = macroContextCutoffsSchema.safeParse({
    knowledgeCutoff,
    effectiveDateCutoff,
  })
  const context = useQuery({
    queryKey: productKeys.operation(bootstrap.runtime, "research", operation, {
      knowledgeCutoff: cutoffs?.knowledgeCutoff ?? null,
      effectiveDateCutoff: cutoffs?.effectiveDateCutoff ?? null,
      requestVersion,
    }),
    enabled: operationAvailable,
    retry: false,
    queryFn: async () =>
      parseMacroContext(
        await transport.query({
          query: "macroContext",
          ...(cutoffs ?? {}),
        }),
        cutoffs ?? undefined,
      ),
  })

  if (!operationAvailable) {
    return (
      <MacroFrame>
        <MacroAvailabilityNotice
          title="Economic context is unavailable"
          detail="Complete setup to add economic and interest-rate context to your research."
          showSetup
        />
      </MacroFrame>
    )
  }

  if (context.isPending) return <MacroLoading />

  if (context.isError) {
    return (
      <MacroFrame>
        <Alert variant="destructive">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Economic context is unavailable</AlertTitle>
          <AlertDescription>
            Economic and interest-rate information cannot be shown right now.
          </AlertDescription>
        </Alert>
        <Button className="mt-4" variant="outline" onClick={() => void context.refetch()}>
          <RefreshCw aria-hidden="true" />
          Try again
        </Button>
      </MacroFrame>
    )
  }

  const data = context.data
  return (
    <MacroFrame>
      <header className="flex flex-wrap items-start justify-between gap-4 border-b border-border p-5">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Economic context
          </p>
          <h2 className="mt-2 text-xl font-semibold">Rates and labor conditions</h2>
          <p className="mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">
            Use the rate curve and unemployment backdrop to understand the environment around an
            investment. These indicators are research context, not trading prices or a standalone
            buy or sell signal.
          </p>
        </div>
        <MacroEvidenceBadge tone={data.confidence.level === "moderate" ? "good" : "neutral"}>
          {confidenceLabel(data.confidence.level)}
        </MacroEvidenceBadge>
      </header>

      <div className="space-y-5 p-5">
        <CutoffForm
          knowledgeCutoff={knowledgeCutoff}
          effectiveDateCutoff={effectiveDateCutoff}
          valid={cutoffResult.success}
          pending={context.isFetching}
          onKnowledgeCutoff={setKnowledgeCutoff}
          onEffectiveDateCutoff={setEffectiveDateCutoff}
          onApply={() => {
            if (!cutoffResult.success) return
            if (cutoffResult.data.knowledgeCutoff === "") {
              setCutoffs(null)
            } else {
              setCutoffs(cutoffResult.data)
            }
            setRequestVersion((version) => version + 1)
          }}
        />

        <AvailabilitySummary data={data} />
        <IndicatorSection
          title="Interest-rate curve"
          detail="Government yields across short and long maturities show how borrowing conditions change over time."
          observations={data.observations.filter(
            (observation) => observation.category === "interest_rates",
          )}
        />
        <IndicatorSection
          title="Labor market"
          detail="Unemployment helps frame household demand, business conditions, and recession risk."
          observations={data.observations.filter(
            (observation) => observation.category === "labor_market",
          )}
        />
      </div>
    </MacroFrame>
  )
}

function CutoffForm({
  knowledgeCutoff,
  effectiveDateCutoff,
  valid,
  pending,
  onKnowledgeCutoff,
  onEffectiveDateCutoff,
  onApply,
}: {
  knowledgeCutoff: string
  effectiveDateCutoff: string
  valid: boolean
  pending: boolean
  onKnowledgeCutoff: (value: string) => void
  onEffectiveDateCutoff: (value: string) => void
  onApply: () => void
}) {
  const entered = knowledgeCutoff !== "" || effectiveDateCutoff !== ""
  return (
    <form
      className="rounded-lg border border-border bg-background/30 p-4"
      onSubmit={(event) => {
        event.preventDefault()
        if (valid) onApply()
      }}
    >
      <div className="grid gap-4 sm:grid-cols-2">
        <div>
          <Label htmlFor="macro-knowledge-cutoff">What was known by</Label>
          <Input
            id="macro-knowledge-cutoff"
            className="mt-2 font-mono text-xs"
            value={knowledgeCutoff}
            placeholder="2026-08-28T14:30:00Z"
            autoComplete="off"
            spellCheck={false}
            onChange={(event) => onKnowledgeCutoff(event.target.value)}
          />
        </div>
        <div>
          <Label htmlFor="macro-effective-cutoff">Use data through</Label>
          <Input
            id="macro-effective-cutoff"
            type="date"
            className="mt-2 font-mono text-xs"
            value={effectiveDateCutoff}
            onChange={(event) => onEffectiveDateCutoff(event.target.value)}
          />
        </div>
      </div>
      {entered && !valid ? (
        <p className="mt-3 text-xs text-destructive" role="alert">
          Enter both dates and include a time zone. “Use data through” cannot be later than “What
          was known by.”
        </p>
      ) : null}
      <Button className="mt-4" type="submit" disabled={!valid || pending}>
        <CalendarClock aria-hidden="true" />
        {pending ? "Updating…" : entered ? "Apply dates" : "Use current context"}
      </Button>
    </form>
  )
}

function AvailabilitySummary({ data }: { data: MacroContextData }) {
  return (
    <section className="rounded-lg border border-border bg-background/30 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold">
            {data.availability === "available"
              ? "Economic context available"
              : data.availability === "partial"
                ? "Some economic context is unavailable"
                : "Economic context unavailable"}
          </h3>
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            {data.confidence.summary}
          </p>
        </div>
        <MacroEvidenceBadge>
          {data.coverage.observed} of {data.coverage.requested} available
        </MacroEvidenceBadge>
      </div>
      <dl className="mt-4 grid gap-3 border-t border-border pt-4 sm:grid-cols-3">
        <MacroEvidenceFact
          label="What was known by"
          value={data.selection.knowledgeCutoff}
        />
        <MacroEvidenceFact
          label="Use data through"
          value={data.selection.effectiveDateCutoff}
        />
        <MacroEvidenceFact label="Checked" value={data.selection.evaluatedAt} />
      </dl>
    </section>
  )
}

function IndicatorSection({
  title,
  detail,
  observations,
}: {
  title: string
  detail: string
  observations: MacroContextObservation[]
}) {
  return (
    <section>
      <h3 className="text-base font-semibold">{title}</h3>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">{detail}</p>
      <div className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
        {observations.map((observation) => (
          <IndicatorCard key={observation.indicatorId} observation={observation} />
        ))}
      </div>
    </section>
  )
}

function IndicatorCard({ observation }: { observation: MacroContextObservation }) {
  return (
    <article className="rounded-lg border border-border bg-background/30 p-4">
      <div className="flex items-start justify-between gap-3">
        <h4 className="text-sm font-semibold">{observation.label}</h4>
        <MacroEvidenceBadge tone={observation.availability === "available" ? "good" : "neutral"}>
          {availabilityLabel(observation.availability)}
        </MacroEvidenceBadge>
      </div>
      {observation.value.state === "observed" ? (
        <div className="mt-5">
          <p className="font-mono text-2xl font-semibold tabular-nums">
            {observation.value.decimal}
            {observation.unit.symbol ?? ""}
          </p>
          <p className="mt-1 text-[10px] text-muted-foreground">{observation.unit.label}</p>
        </div>
      ) : (
        <p className="mt-5 text-xs leading-5 text-muted-foreground">
          {observation.value.explanation}
        </p>
      )}
      <dl className="mt-4 grid gap-3 border-t border-border pt-4">
        <MacroEvidenceFact
          label="Effective date"
          value={observation.effectiveDate ?? "Not available"}
        />
        <MacroEvidenceFact
          label="Recorded date"
          value={
            observation.recorded.state === "known"
              ? observation.recorded.date
              : "Not supplied"
          }
        />
        <MacroEvidenceFact
          label="Available from"
          value={observation.availableAt ?? "Not available"}
        />
      </dl>
      <p className="mt-3 text-[10px] leading-4 text-muted-foreground">
        {observation.confidence.summary}
      </p>
    </article>
  )
}

function MacroFrame({ children }: { children: React.ReactNode }) {
  return (
    <section className="rounded-xl border border-border bg-card/45" aria-label="Economic context">
      {children}
    </section>
  )
}

function MacroLoading() {
  return (
    <MacroFrame>
      <div className="p-5" aria-busy="true" aria-label="Loading economic context">
        <Skeleton className="h-28 rounded-lg" />
        <Skeleton className="mt-3 h-72 rounded-lg" />
      </div>
    </MacroFrame>
  )
}

function availabilityLabel(value: MacroContextObservation["availability"]): string {
  return value === "available"
    ? "Available"
    : value === "missing"
      ? "Not reported"
      : "Unavailable"
}

function confidenceLabel(value: MacroContextData["confidence"]["level"]): string {
  return value === "moderate"
    ? "Moderate data confidence"
    : value === "limited"
      ? "Limited data confidence"
      : "Data confidence unavailable"
}
