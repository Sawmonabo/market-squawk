import * as React from "react"
import { useMutation, useQuery } from "@tanstack/react-query"
import {
  AlertCircle,
  FileSearch2,
  RefreshCw,
  ShieldCheck,
  SlidersHorizontal,
} from "lucide-react"

import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney, humanize } from "@/lib/formatters"
import { formatTimestamp, timestampFromUnixNanos } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import type { DecisionDossierView, TargetIndexView } from "./contracts"
import { StateLabel } from "./decision-boundaries"
import {
  AssumptionsEditor,
  Field,
  NarrativeList,
  SELECT_CLASS,
  TargetPriceLadder,
  ThesisField,
} from "./target-builder-fields"
import {
  assumptionEvidence,
  eligibleTargets,
  emptyAssumption,
  emptyPrices,
  TARGET_METHODS,
  validateTargetDraft,
  type TargetAssumptionDraft,
  type TargetHorizon,
  type TargetIntent,
  type TargetMethod,
  type TargetOperation,
  type TargetPriceDraft,
  type TargetPriceKey,
} from "./target-builder-model"
import {
  parsePreparedTarget,
  parseTargetCommit,
  parseTargetPreparation,
  type PreparedTargetView,
  type TargetCommitOutcome,
} from "./target-preparation-contracts"
import { PreparedTargetPreview, TargetCommitReceipt } from "./target-preview"

interface PreparedAdmission {
  preview: PreparedTargetView
  commitAction: "createTargetSet" | "reevaluateTargetSet"
}

interface DurableCommit {
  outcome: TargetCommitOutcome
  targetId: string
  revision: number
}

export function TargetBuilder({
  transport,
  scope,
  dossier,
  targetIndex,
  onCommitted,
}: {
  transport: ProductTransport
  scope: ProductScope
  dossier: DecisionDossierView | null
  targetIndex: TargetIndexView[]
  onCommitted: (targetId: string) => Promise<void>
}) {
  const dossierId = dossier?.id ?? ""
  const inventory = useQuery({
    queryKey: productKeys.operation(scope, "decision", "target-preparation", { dossierId }),
    queryFn: async () => {
      const result = parseTargetPreparation(
        await transport.query({ query: "decisionTargetPreparation", dossierId }),
      )
      if (
        !dossier ||
        result.dossierId !== dossier.id ||
        result.instrumentId !== dossier.instrumentId
      ) {
        throw new Error("The target preparation details did not match the selected dossier.")
      }
      return result
    },
    enabled: dossier !== null,
  })
  const targets = dossier ? eligibleTargets(dossier, targetIndex) : []
  const [operation, setOperation] = React.useState<TargetOperation>("create")
  const [referenceMark, setReferenceMark] = React.useState("")
  const [forecastIndex, setForecastIndex] = React.useState("none")
  const [useFairValue, setUseFairValue] = React.useState(false)
  const [usePortfolio, setUsePortfolio] = React.useState(false)
  const [intent, setIntent] = React.useState<TargetIntent>("hold")
  const [horizon, setHorizon] = React.useState<TargetHorizon>("year")
  const [prices, setPrices] = React.useState<TargetPriceDraft>(emptyPrices)
  const [method, setMethod] = React.useState<TargetMethod>("comparable_evidence")
  const [assumptions, setAssumptions] = React.useState<TargetAssumptionDraft[]>([
    emptyAssumption(),
  ])
  const [thesis, setThesis] = React.useState("")
  const [risks, setRisks] = React.useState([""])
  const [invalidations, setInvalidations] = React.useState([""])
  const [prepared, setPrepared] = React.useState<PreparedAdmission | null>(null)
  const [commitReceipt, setCommitReceipt] = React.useState<DurableCommit | null>(null)
  const [evidenceInitialized, setEvidenceInitialized] = React.useState(false)
  const [now, setNow] = React.useState(Date.now())

  React.useEffect(() => {
    setOperation("create")
    setReferenceMark("")
    setForecastIndex("none")
    setUseFairValue(false)
    setUsePortfolio(false)
    setIntent("hold")
    setHorizon("year")
    setPrices(emptyPrices())
    setMethod("comparable_evidence")
    setAssumptions([emptyAssumption()])
    setThesis("")
    setRisks([""])
    setInvalidations([""])
    setPrepared(null)
    setCommitReceipt(null)
    setEvidenceInitialized(false)
  }, [dossierId])

  React.useEffect(() => {
    const evidence = inventory.data
    if (!evidence || evidenceInitialized) return
    const mark = evidence.referenceMarks[0]
    setReferenceMark(mark?.selector ?? "")
    setForecastIndex(
      evidence.forecastOptions[0] ? String(evidence.forecastOptions[0].index) : "none",
    )
    setUseFairValue(evidence.fairValueAvailable)
    setUsePortfolio(evidence.portfolioAvailable)
    setEvidenceInitialized(true)
  }, [evidenceInitialized, inventory.data])

  React.useEffect(() => {
    if (!prepared) return
    setNow(Date.now())
    const interval = window.setInterval(() => setNow(Date.now()), 1_000)
    return () => window.clearInterval(interval)
  }, [prepared])

  const selectedMark = inventory.data?.referenceMarks.find(
    (mark) => mark.selector === referenceMark,
  )
  const validation =
    dossier && inventory.data
      ? validateTargetDraft({
          operation,
          targets,
          mark: selectedMark,
          intent,
          prices,
          method,
          assumptions,
          risks,
          invalidations,
          thesis,
          dossier,
          preparation: inventory.data,
          forecastIndex,
          useFairValue,
          usePortfolio,
        })
      : { valid: false as const, reason: "Select a retained dossier first." }

  const prepare = useMutation({
    mutationFn: async (draft: Record<string, unknown>) =>
      parsePreparedTarget(
        await transport.decisionControl({ action: "prepareTargetSet", draft }),
      ),
    onSuccess: (preview) => {
      setPrepared({
        preview,
        commitAction: operation === "create" ? "createTargetSet" : "reevaluateTargetSet",
      })
      setCommitReceipt(null)
    },
  })
  const commit = useMutation({
    mutationFn: async (admission: PreparedAdmission) => ({
      outcome: parseTargetCommit(
        await transport.decisionControl(
          { action: admission.commitAction, receiptId: admission.preview.receiptId },
          true,
        ),
      ),
      preview: admission.preview,
    }),
    onSuccess: async ({ outcome, preview }) => {
      setCommitReceipt({
        outcome,
        targetId: preview.targetId,
        revision: preview.revision,
      })
      setOperation(preview.targetId)
      setPrepared(null)
      await onCommitted(preview.targetId)
    },
  })
  const receiptExpired = prepared ? isPast(prepared.preview.receiptExpiresAt, now) : false

  function submit(event: React.FormEvent) {
    event.preventDefault()
    if (!dossier || !inventory.data || !selectedMark || !validation.valid) return
    const normalizedAssumptions = assumptions.map((assumption) => {
      const evidence = assumptionEvidence(assumption.evidenceKey)
      if (!evidence) throw new Error("An assumption evidence selection is unavailable.")
      return { text: assumption.text, evidence }
    })
    const money = (amount: string) => ({ amount, currency: selectedMark.price.currency })
    prepare.mutate({
      operation:
        operation === "create"
          ? { kind: "create" }
          : { kind: "reevaluate", targetId: operation },
      dossierId: dossier.id,
      intent,
      horizon,
      prices: Object.fromEntries(
        Object.entries(prices).map(([key, amount]) => [key, money(amount)]),
      ),
      method,
      assumptions: normalizedAssumptions,
      thesis,
      risks,
      invalidationConditions: invalidations,
      evidence: {
        referenceMark: selectedMark.selector,
        forecastReference: forecastIndex === "none" ? null : Number(forecastIndex),
        useFairValue,
        usePortfolio,
      },
    })
  }

  return (
    <section className="mt-8" aria-labelledby="target-builder-heading">
      <div className="overflow-hidden rounded-xl border border-primary/25 bg-card/55">
        <header className="flex flex-wrap items-start justify-between gap-4 border-b border-border p-5">
          <div>
            <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
              Guided investment target
            </p>
            <h2 id="target-builder-heading" className="mt-1 text-lg font-semibold">
              Turn retained research into a reviewable price judgment
            </h2>
            <p className="mt-1 max-w-3xl text-sm leading-6 text-muted-foreground">
              Choose the supporting analysis, enter your assumptions and ordered ranges, review the
              complete result, then explicitly confirm the revision.
            </p>
          </div>
          <StateLabel value={dossier ? "dossier selected" : "waiting for dossier"} />
        </header>

        {!dossier ? (
          <div
            className="m-5 rounded-xl border border-dashed border-border p-6 text-sm leading-6 text-muted-foreground"
          >
            Select <strong className="text-foreground">Use for investment target</strong> on a
            saved dossier above to begin a reviewable price judgment.
          </div>
        ) : inventory.isPending ? (
          <div className="grid gap-3 p-5" aria-label="Loading target evidence inventory">
            <Skeleton className="h-24 w-full" />
            <Skeleton className="h-64 w-full" />
          </div>
        ) : inventory.isError ? (
          <Alert variant="destructive" className="m-5">
            <AlertCircle aria-hidden="true" />
            <AlertTitle>Target inputs could not be loaded</AlertTitle>
            <AlertDescription>
              Market Squawk could not retrieve the information needed to prepare this target.
              Retry, and check Logs if the problem continues.
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="mt-2"
                onClick={() => void inventory.refetch()}
              >
                Retry
              </Button>
            </AlertDescription>
          </Alert>
        ) : inventory.data.referenceMarks.length === 0 ? (
          <Alert className="m-5">
            <AlertCircle aria-hidden="true" />
            <AlertTitle>No current reference mark is available</AlertTitle>
            <AlertDescription>
              No current, eligible price is available for {dossier.instrumentId}. Refresh market
              or portfolio information before preparing a target.
            </AlertDescription>
          </Alert>
        ) : inventory.data.forecastOptions.length === 0 && !inventory.data.fairValueAvailable ? (
          <Alert className="m-5">
            <AlertCircle aria-hidden="true" />
            <AlertTitle>No supported analytical method is available</AlertTitle>
            <AlertDescription>
              This dossier contains neither a forecast nor a fair-value analysis.
              Assemble a complete dossier before preparing an investment target.
            </AlertDescription>
          </Alert>
        ) : prepared ? (
          <div className="grid gap-4 p-5">
            {commit.isError && (
              <Alert variant="destructive">
                <AlertCircle aria-hidden="true" />
                <AlertTitle>The target revision was not committed</AlertTitle>
                <AlertDescription>
                  Market Squawk could not save this target revision. Try again, and check Logs if
                  the problem continues.
                </AlertDescription>
              </Alert>
            )}
            <PreparedTargetPreview
              preview={prepared.preview}
              expired={receiptExpired}
              committing={commit.isPending}
              onDiscard={() => {
                setPrepared(null)
                commit.reset()
              }}
              onCommit={() => commit.mutate(prepared)}
            />
          </div>
        ) : (
          <form className="grid gap-6 p-5" onSubmit={submit}>
            <DossierSummary dossier={dossier} />
            <div className="grid gap-4 lg:grid-cols-3">
              <Field
                label="Target history action"
                htmlFor="target-operation"
                help="Create a new target or revise a previous target for this investment."
              >
                <select
                  id="target-operation"
                  className={SELECT_CLASS}
                  value={operation}
                  onChange={(event) => setOperation(event.target.value)}
                >
                  <option value="create">Create a new target series</option>
                  {targets.map((target) => (
                    <option key={target.id} value={target.id}>
                      Reevaluate revision {target.revision} · {humanize(target.status)}
                    </option>
                  ))}
                </select>
              </Field>
              <Field
                label="Decision posture"
                htmlFor="target-intent"
                help="This choice must agree with the observed price and your ranges."
              >
                <select
                  id="target-intent"
                  className={SELECT_CLASS}
                  value={intent}
                  onChange={(event) => setIntent(event.target.value as TargetIntent)}
                >
                  <option value="buy">Buy — mark is inside or below entry range</option>
                  <option value="hold">Hold — mark is between entry and trim ranges</option>
                  <option value="sell">Sell — mark is inside or above trim range</option>
                </select>
              </Field>
              <Field
                label="Research horizon"
                htmlFor="target-horizon"
                help="This choice sets the review period and target expiration."
              >
                <select
                  id="target-horizon"
                  className={SELECT_CLASS}
                  value={horizon}
                  onChange={(event) => setHorizon(event.target.value as TargetHorizon)}
                >
                  <option value="quarter">Quarter · 90-day horizon</option>
                  <option value="year">Year · 365-day horizon</option>
                  <option value="three_years">Three years · 1,095-day horizon</option>
                </select>
              </Field>
            </div>

            <EvidenceSelector
              preparation={inventory.data}
              referenceMark={referenceMark}
              forecastIndex={forecastIndex}
              useFairValue={useFairValue}
              usePortfolio={usePortfolio}
              onReferenceMark={(selector) => {
                setReferenceMark(selector)
                setPrices(emptyPrices())
              }}
              onForecastIndex={setForecastIndex}
              onUseFairValue={setUseFairValue}
              onUsePortfolio={setUsePortfolio}
            />

            <div className="grid gap-4 lg:grid-cols-2">
              <Field
                label="Analytical method"
                htmlFor="target-method"
                help="Only methods supported by the selected forecast or fair-value analysis are available."
              >
                <select
                  id="target-method"
                  className={SELECT_CLASS}
                  value={method}
                  onChange={(event) => setMethod(event.target.value as TargetMethod)}
                >
                  {TARGET_METHODS.map((option) => (
                    <option key={option.value} value={option.value}>{option.label}</option>
                  ))}
                </select>
              </Field>
              {selectedMark && (
                <div className="rounded-xl border border-border bg-background/45 p-4">
                  <p className="text-xs font-semibold">Selected reference mark</p>
                  <p className="mt-2 text-lg font-semibold tabular-nums">{formatMoney(selectedMark.price)}</p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    {humanize(selectedMark.quality)} · observed {formatTimestamp(selectedMark.observedAt)}
                  </p>
                </div>
              )}
            </div>

            <TargetPriceLadder
              prices={prices}
              currency={selectedMark?.price.currency ?? "—"}
              onChange={(key: TargetPriceKey, value: string) =>
                setPrices((current) => ({ ...current, [key]: value }))
              }
            />
            <AssumptionsEditor
              assumptions={assumptions}
              dossier={dossier}
              preparation={inventory.data}
              forecastSelected={forecastIndex !== "none"}
              fairValueSelected={useFairValue}
              portfolioSelected={usePortfolio}
              onChange={setAssumptions}
            />
            <ThesisField value={thesis} onChange={setThesis} />
            <div className="grid gap-6 lg:grid-cols-2">
              <NarrativeList
                title="Risks"
                description="Record what could impair the judgment or expected outcome."
                singular="Risk"
                values={risks}
                onChange={setRisks}
              />
              <NarrativeList
                title="Invalidation conditions"
                description="State observable conditions that require the target to be reviewed."
                singular="Condition"
                values={invalidations}
                onChange={setInvalidations}
              />
            </div>

            {!validation.valid && (
              <Alert>
                <AlertCircle aria-hidden="true" />
                <AlertTitle>Complete the judgment before preparing it</AlertTitle>
                <AlertDescription>{validation.reason}</AlertDescription>
              </Alert>
            )}
            {prepare.isError && (
              <Alert variant="destructive">
                <AlertCircle aria-hidden="true" />
                <AlertTitle>The target preview was not prepared</AlertTitle>
                <AlertDescription>
                  Review the target inputs and try again. Detailed errors are available in Logs.
                </AlertDescription>
              </Alert>
            )}
            {commitReceipt && <TargetCommitReceipt {...commitReceipt} />}
            <div className="flex flex-wrap items-center justify-between gap-3 border-t border-border pt-4">
              <div className="flex max-w-2xl items-start gap-2 text-xs leading-5 text-muted-foreground">
                <ShieldCheck className="mt-0.5 size-4 shrink-0 text-primary" aria-hidden="true" />
                Market Squawk checks the complete judgment and returns a short-lived preview. This
                step does not save a target or create an order.
              </div>
              <Button type="submit" disabled={!validation.valid || prepare.isPending}>
                {prepare.isPending ? (
                  <RefreshCw className="animate-spin" aria-hidden="true" />
                ) : (
                  <FileSearch2 aria-hidden="true" />
                )}
                Prepare complete preview
              </Button>
            </div>
          </form>
        )}
      </div>
    </section>
  )
}

function DossierSummary({ dossier }: { dossier: DecisionDossierView }) {
  return (
    <div
      className="flex flex-wrap items-start justify-between gap-4 rounded-xl border border-primary/20 bg-primary/5 p-4"
    >
      <div>
        <div className="flex items-center gap-2">
          <SlidersHorizontal className="size-4 text-primary" aria-hidden="true" />
          <p className="text-sm font-semibold">Retained dossier selected</p>
        </div>
        <p className="mt-2 text-xs text-muted-foreground">
          {dossier.instrumentId} · assembled {formatTimestamp(dossier.assembledAt)} ·{" "}
          {dossier.references.length} evidence references
        </p>
      </div>
      <StateLabel value="selected" />
    </div>
  )
}

function EvidenceSelector({
  preparation,
  referenceMark,
  forecastIndex,
  useFairValue,
  usePortfolio,
  onReferenceMark,
  onForecastIndex,
  onUseFairValue,
  onUsePortfolio,
}: {
  preparation: NonNullable<ReturnType<typeof parseTargetPreparation>>
  referenceMark: string
  forecastIndex: string
  useFairValue: boolean
  usePortfolio: boolean
  onReferenceMark: (value: string) => void
  onForecastIndex: (value: string) => void
  onUseFairValue: (value: boolean) => void
  onUsePortfolio: (value: boolean) => void
}) {
  return (
    <fieldset className="rounded-xl border border-border bg-background/45 p-4">
      <legend className="px-1 text-sm font-semibold">Supporting analysis</legend>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        Choose from the observations and analyses already available for this dossier.
      </p>
      <div className="mt-3 grid gap-4 lg:grid-cols-2">
        <Field label="Observed reference mark" htmlFor="target-reference-mark">
          <select
            id="target-reference-mark"
            className={SELECT_CLASS}
            value={referenceMark}
            onChange={(event) => onReferenceMark(event.target.value)}
          >
            {preparation.referenceMarks.map((mark) => (
              <option key={mark.selector} value={mark.selector}>
                {formatMoney(mark.price)} · {humanize(mark.quality)}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Forecast analysis" htmlFor="target-forecast">
          <select
            id="target-forecast"
            className={SELECT_CLASS}
            value={forecastIndex}
            onChange={(event) => onForecastIndex(event.target.value)}
          >
            <option value="none">Do not include a forecast</option>
            {preparation.forecastOptions.map((option) => (
              <option key={option.index} value={String(option.index)}>
                Forecast {option.index + 1}
              </option>
            ))}
          </select>
        </Field>
        <EvidenceToggle
          label="Include fair-value analysis"
          available={preparation.fairValueAvailable}
          checked={useFairValue}
          onChange={onUseFairValue}
        />
        <EvidenceToggle
          label="Include portfolio analysis"
          available={preparation.portfolioAvailable}
          checked={usePortfolio}
          onChange={onUsePortfolio}
        />
      </div>
    </fieldset>
  )
}

function EvidenceToggle({
  label,
  available,
  checked,
  onChange,
}: {
  label: string
  available: boolean
  checked: boolean
  onChange: (value: boolean) => void
}) {
  return (
    <label className="flex items-start gap-3 rounded-lg border border-border bg-card/45 p-3 text-xs">
      <input
        type="checkbox"
        className="mt-0.5 size-4 accent-primary"
        checked={checked}
        disabled={!available}
        onChange={(event) => onChange(event.target.checked)}
      />
      <span>
        <span className="font-medium">{label}</span>
        <span className="mt-1 block text-muted-foreground">
          {available ? "Available in the retained dossier." : "Not available in this dossier."}
        </span>
      </span>
    </label>
  )
}

function isPast(timestamp: string, now: number): boolean {
  const value = timestampFromUnixNanos(timestamp)
  return value === null || value.valueOf() <= now
}
