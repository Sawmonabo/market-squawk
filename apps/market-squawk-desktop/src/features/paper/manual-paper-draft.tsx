import * as React from "react"
import { Link } from "react-router-dom"
import { useMutation, useQuery } from "@tanstack/react-query"
import { CircleAlert, FileCheck2, Send, ShieldCheck } from "lucide-react"

import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { formatMoney } from "@/lib/formatters"
import type { ProductTransport } from "@/lib/transport"

import type { PaperStatus } from "./contracts"
import {
  asManualPaperTransport,
  isPositiveLotQuantity,
  parseAcceptedManualPaperDraft,
  parseManualPaperTargets,
  parseManualPaperPreview,
  type ManualPaperTarget,
  type ManualPaperOrderType,
  type ManualPaperSide,
  type ManualPaperPrepare,
  type ManualPaperPreview,
  type ManualPaperTimeInForce,
  type TargetLevel,
} from "./manual-paper-contracts"

const CONTROL_CLASS =
  "h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"

type Draft = {
  targetToken: string
  side: ManualPaperSide | ""
  orderType: ManualPaperOrderType | ""
  quantityLots: string
  limitTargetLevel: TargetLevel | ""
  stopTargetLevel: TargetLevel | ""
  timeInForce: ManualPaperTimeInForce | ""
}

export function ManualPaperDraftPanel({
  transport,
  scope,
  enabled,
  busy,
  onAccepted,
  analysisActionToken,
  sessionStatus,
  statusLoading,
  statusFailed,
}: {
  analysisActionToken?: string
  transport: ProductTransport
  scope: ProductScope
  enabled: boolean
  busy: boolean
  onAccepted: () => Promise<unknown>
  sessionStatus: PaperStatus | undefined
  statusLoading: boolean
  statusFailed: boolean
}) {
  const manualPaper = asManualPaperTransport(transport)
  const [draft, setDraft] = React.useState<Draft>(emptyDraft)
  const [pending, setPending] = React.useState<ManualPaperPreview | null>(null)
  const [accepted, setAccepted] = React.useState<string | null>(null)
  const targets = useQuery({
    queryKey: productKeys.operation(
      scope, "execution", "Execution.GetManualPaperTargets", { analysisActionToken },
    ),
    enabled: enabled && manualPaper !== null,
    staleTime: 15_000,
    queryFn: async () => {
      if (!manualPaper) throw new Error("Paper drafting is unavailable.")
      return parseManualPaperTargets(
        await manualPaper.manualPaper({ action: "targets", analysisActionToken }),
        analysisActionToken,
      )
    },
  })
  const prepare = useMutation({
    mutationFn: async ({ request, provenance }: {
      request: ManualPaperPrepare
      provenance: ManualPaperTarget["provenance"]
    }) => {
      if (!manualPaper) throw new Error("Paper drafting is unavailable.")
      return parseManualPaperPreview(await manualPaper.manualPaper(request), provenance)
    },
    onSuccess: (preview) => setPending(preview),
  })
  const submit = useMutation({
    mutationFn: async (confirmationToken: string) => {
      if (!manualPaper) throw new Error("Paper drafting is unavailable.")
      return parseAcceptedManualPaperDraft(
        await manualPaper.manualPaper({ action: "submitManual", confirmationToken }, true),
      )
    },
    onSuccess: async (message) => {
      setPending(null)
      setAccepted(message)
      setDraft(emptyDraft())
      await onAccepted()
    },
    onError: () => setPending(null),
  })

  const selected = targets.data?.find((target) => target.targetToken === draft.targetToken) ?? null
  const orderChoice = selected?.orderChoices.find((choice) => choice.value === draft.orderType)
  const normalized = normalizeDraft(draft, selected)
  const ready =
    enabled && normalized !== null && pending === null && !busy && !prepare.isPending && !submit.isPending

  return (
    <section
      className="mt-4 rounded-xl border border-primary/25 bg-primary/5 p-5"
      aria-labelledby="manual-paper-heading"
    >
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Paper practice
          </p>
          <h2 id="manual-paper-heading" className="mt-1 text-lg font-semibold">
            Practice an investment idea without real money
          </h2>
          <p className="mt-1 max-w-3xl text-sm leading-6 text-muted-foreground">
            Select a plan or saved recommendation and make each choice yourself. Market Squawk supplies only the
            prepared choices, then rechecks current conditions and safeguards before
            a virtual order can proceed.
          </p>
        </div>
        <ShieldCheck className="size-5 text-primary" aria-hidden="true" />
      </div>

      {statusFailed ? (
        <Status text="Paper session status could not be loaded. Use Refresh above before preparing a virtual trade." tone="error" />
      ) : statusLoading ? (
        <Status text="Checking the current paper session…" />
      ) : sessionStatus?.sessionAvailability === "ready" ? (
        <Unavailable
          title="Start a paper session first"
          detail="Use the session controls below to start paper practice. They show any current account or market requirements before you confirm a session."
        />
      ) : sessionStatus?.sessionAvailability === "unavailable" ? (
        <Unavailable detail={sessionStatus.safeguards === "action_needed"
          ? "Paper practice needs attention. Review the session controls below and Logs & Diagnostics."
          : "Paper practice is temporarily unavailable. Review the session controls below, then try again shortly."} />
      ) : !enabled || manualPaper === null ? (
        <Unavailable />
      ) : targets.isPending ? (
        <Status text="Loading prepared paper choices…" />
      ) : targets.isError ? (
        <div>
          <Status text="Prepared paper choices are not available right now." tone="error" />
          <Button className="mt-3" variant="outline" onClick={() => void targets.refetch()} disabled={targets.isFetching}>
            Try again
          </Button>
        </div>
      ) : targets.data?.length === 0 ? (
        <Unavailable detail={analysisActionToken ? "This saved recommendation is not available for paper practice under current conditions. Return to Opportunities to review it." : "Create an investment plan before preparing a paper trade."} />
      ) : (
        <div className="mt-5 space-y-4">
          <fieldset disabled={busy || prepare.isPending || submit.isPending || pending !== null} className="grid gap-4 lg:grid-cols-2">
            <Field label={analysisActionToken ? "Saved recommendation" : "Investment plan"} htmlFor="manual-paper-target">
              <select
                id="manual-paper-target"
                className={CONTROL_CLASS}
                value={draft.targetToken}
                onChange={(event) => {
                  setDraft({ ...emptyDraft(), targetToken: event.target.value })
                  setAccepted(null)
                }}
              >
                <option value="">{analysisActionToken ? "Select the saved recommendation" : "Select an investment plan"}</option>
                {targets.data.map((target) => (
                  <option key={target.targetToken} value={target.targetToken}>
                    {investmentLabel(target.investment)} · {target.provenance.kind === "generated_proposal" ? "Saved recommendation" : "Investment plan"}
                  </option>
                ))}
              </select>
            </Field>

            {selected ? (
              <>
                <Field label="Direction" htmlFor="manual-paper-side">
                  <select
                    id="manual-paper-side"
                    className={CONTROL_CLASS}
                    value={draft.side}
                    onChange={(event) => {
                      setDraft((current) => ({
                        ...current,
                        side: event.target.value as ManualPaperSide | "",
                      }))
                      setAccepted(null)
                    }}
                  >
                    <option value="">Select a direction</option>
                    {selected.sideChoices.map((choice) => (
                      <option key={choice.value} value={choice.value}>
                        {choice.label} — {choice.explanation}
                      </option>
                    ))}
                  </select>
                </Field>

                <Field label="Order approach" htmlFor="manual-paper-order-type">
                  <select
                    id="manual-paper-order-type"
                    className={CONTROL_CLASS}
                    value={draft.orderType}
                    onChange={(event) => {
                      setDraft((current) => ({
                        ...current,
                        orderType: event.target.value as ManualPaperOrderType | "",
                        limitTargetLevel: "",
                        stopTargetLevel: "",
                        timeInForce: "",
                      }))
                      setAccepted(null)
                    }}
                  >
                    <option value="">Select an order approach</option>
                    {selected.orderChoices.map((choice) => (
                      <option key={choice.value} value={choice.value}>
                        {choice.label} — {choice.explanation}
                      </option>
                    ))}
                  </select>
                </Field>

                <Field label="Whole-lot quantity" htmlFor="manual-paper-quantity">
                  <Input
                    id="manual-paper-quantity"
                    value={draft.quantityLots}
                    inputMode="numeric"
                    autoComplete="off"
                    placeholder="Enter quantity"
                    aria-invalid={
                      draft.quantityLots.length > 0 &&
                      !isPositiveLotQuantity(draft.quantityLots)
                    }
                    onChange={(event) => {
                      setDraft((current) => ({ ...current, quantityLots: event.target.value }))
                      setAccepted(null)
                    }}
                  />
                  <FieldMessage>Enter a positive whole number of lots.</FieldMessage>
                </Field>

                {orderChoice?.requiresLimitLevel ? (
                  <LevelField
                    label="Limit condition"
                    fieldId="manual-paper-limit-level"
                    value={draft.limitTargetLevel}
                    ladder={selected.ladder}
                    onChange={(limitTargetLevel) => {
                      setDraft((current) => ({ ...current, limitTargetLevel }))
                      setAccepted(null)
                    }}
                  />
                ) : null}

                {orderChoice?.requiresStopLevel ? (
                  <LevelField
                    label="Stop condition"
                    fieldId="manual-paper-stop-level"
                    value={draft.stopTargetLevel}
                    ladder={selected.ladder}
                    onChange={(stopTargetLevel) => {
                      setDraft((current) => ({ ...current, stopTargetLevel }))
                      setAccepted(null)
                    }}
                  />
                ) : null}

                {orderChoice ? (
                  <Field label="How long the order remains active" htmlFor="manual-paper-duration">
                    <select
                      id="manual-paper-duration"
                      className={CONTROL_CLASS}
                      value={draft.timeInForce}
                      onChange={(event) => {
                        setDraft((current) => ({
                          ...current,
                          timeInForce: event.target.value as ManualPaperTimeInForce | "",
                        }))
                        setAccepted(null)
                      }}
                    >
                      <option value="">Select a duration</option>
                      {orderChoice.timeInForceChoices.map((choice) => (
                        <option key={choice.value} value={choice.value}>
                          {choice.label} — {choice.explanation}
                        </option>
                      ))}
                    </select>
                  </Field>
                ) : null}
              </>
            ) : null}
          </fieldset>

          {selected ? <TargetEvidence target={selected} /> : null}
          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-background/25 p-3">
            <p className="max-w-3xl text-[11px] leading-5 text-muted-foreground">
              Reviewing does not place a real or virtual order. A second confirmation and current
              safety check are required.
            </p>
            <Button
              disabled={!ready}
              onClick={() => {
                if (normalized && selected) {
                  prepare.reset()
                  submit.reset()
                  prepare.mutate({ request: normalized, provenance: selected.provenance })
                }
              }}
            >
              <FileCheck2 aria-hidden="true" />
              {prepare.isPending ? "Preparing…" : "Review paper trade"}
            </Button>
          </div>
          {prepare.isError ? (
            <Status text="The virtual trade preview is unavailable. Review the choices and try again." tone="error" />
          ) : null}
          {accepted ? <Status text={accepted} tone="success" /> : null}
          {submit.isError && pending === null ? (
            <Status text="The prepared confirmation is no longer usable. Review the trade again before retrying." tone="error" />
          ) : null}
        </div>
      )}

      <Dialog
        open={pending !== null}
        onOpenChange={(open) => {
          if (!open && !submit.isPending) setPending(null)
        }}
      >
        <DialogContent className="max-h-[88vh] overflow-y-auto sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Submit this virtual trade?</DialogTitle>
            <DialogDescription>
              Confirm the plan, direction, quantity, price conditions, and duration. Current
              conditions and safeguards are checked again before virtual execution.
            </DialogDescription>
          </DialogHeader>
          {pending ? <Confirmation preview={pending} /> : null}
          <DialogFooter>
            <Button variant="outline" disabled={submit.isPending} onClick={() => setPending(null)}>
              Keep editing
            </Button>
            <Button
              disabled={!pending || submit.isPending}
              onClick={() => {
                if (pending) submit.mutate(pending.confirmationToken)
              }}
            >
              <Send aria-hidden="true" />
              {submit.isPending ? "Submitting…" : "Confirm virtual trade"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

function TargetEvidence({ target }: { target: ManualPaperTarget }) {
  return (
    <div className="rounded-xl border border-border bg-background/35 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="font-semibold">{investmentLabel(target.investment)}</p>
          <Provenance provenance={target.provenance} />
        </div>
        <p className="text-right text-[10px] text-muted-foreground">
          Expires{" "}
          {formatProductTimestamp(target.expiresAt)}
        </p>
      </div>
      <dl className="mt-4 grid gap-2 sm:grid-cols-2 xl:grid-cols-5">
        {target.ladder.map((level) => (
          <div key={level.level} className="rounded-lg border border-border/70 bg-card/40 p-3">
            <dt className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground">
              {level.label}
            </dt>
            <dd className="mt-1 text-xs font-semibold tabular-nums">{formatMoney(level.value)}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}

function Confirmation({
  preview,
}: {
  preview: ManualPaperPreview
}) {
  return (
    <dl className="grid gap-3 rounded-xl border border-border bg-card/35 p-4 text-xs sm:grid-cols-2">
      <Fact label="Investment" value={investmentLabel(preview.investment)} />
      <div className="sm:col-span-2">
        <dt className="text-muted-foreground">Recommendation source</dt>
        <dd><Provenance provenance={preview.provenance} /></dd>
      </div>
      <Fact label="Direction" value={preview.direction} />
      <Fact label="Order approach" value={preview.orderApproach} />
      <Fact label="Quantity" value={preview.quantity} />
      <Fact label="Duration" value={preview.duration} />
      {preview.limitCondition ? <Fact label="Limit condition" value={`${preview.limitCondition.label} · ${formatMoney(preview.limitCondition.value)}`} /> : null}
      {preview.stopCondition ? <Fact label="Stop condition" value={`${preview.stopCondition.label} · ${formatMoney(preview.stopCondition.value)}`} /> : null}
      <Fact label="Maximum order value" value={formatMoney(preview.safeguards.maximumOrderValue)} />
      <Fact label="Maximum slippage" value={preview.safeguards.maximumSlippage} />
      <Fact label="Preview expires" value={formatProductTimestamp(preview.expiresAt)} />
      <div className="rounded-lg border border-amber-400/20 bg-amber-400/5 p-3 text-[11px] leading-5 text-amber-100 sm:col-span-2">
        {preview.simulationWarning}
      </div>
    </dl>
  )
}

function LevelField({
  label,
  fieldId,
  value,
  ladder,
  onChange,
}: {
  label: string
  fieldId: string
  value: TargetLevel | ""
  ladder: ManualPaperTarget["ladder"]
  onChange: (value: TargetLevel | "") => void
}) {
  return (
    <Field label={label} htmlFor={fieldId}>
      <select
        id={fieldId}
        className={CONTROL_CLASS}
        value={value}
        onChange={(event) => onChange(event.target.value as TargetLevel | "")}
      >
        <option value="">Select a plan price</option>
        {ladder.map((level) => (
          <option key={level.level} value={level.level}>
            {level.label} · {formatMoney(level.value)}
          </option>
        ))}
      </select>
    </Field>
  )
}

function Field({
  label,
  htmlFor,
  children,
}: {
  label: string
  htmlFor: string
  children: React.ReactNode
}) {
  return (
    <div className="grid gap-2">
      <Label htmlFor={htmlFor}>{label}</Label>
      {children}
    </div>
  )
}

function FieldMessage({ children }: { children: React.ReactNode }) {
  return <p className="text-[11px] leading-5 text-muted-foreground">{children}</p>
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground">{label}</dt>
      <dd className="mt-1 font-medium">{value}</dd>
    </div>
  )
}

function Unavailable({
  title = "Paper practice is unavailable",
  detail = "Paper trade drafting is unavailable in the current setup. Review the session controls below.",
}: {
  title?: string
  detail?: string
}) {
  return (
    <Alert className="mt-4">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{detail}</AlertDescription>
    </Alert>
  )
}

function Status({
  text,
  tone = "neutral",
}: {
  text: string
  tone?: "neutral" | "error" | "success"
}) {
  const classes =
    tone === "error"
      ? "border-rose-400/25 bg-rose-400/5 text-rose-100"
      : tone === "success"
        ? "border-emerald-400/25 bg-emerald-400/5 text-emerald-100"
        : "border-border bg-background/30 text-muted-foreground"
  return <p className={`mt-4 rounded-lg border p-3 text-xs leading-5 ${classes}`}>{text}</p>
}

function normalizeDraft(
  draft: Draft,
  selected: ManualPaperTarget | null,
): ManualPaperPrepare | null {
  if (!selected || !draft.side || !draft.orderType || !draft.timeInForce) return null
  if (!isPositiveLotQuantity(draft.quantityLots)) return null
  if (!selected.sideChoices.some((choice) => choice.value === draft.side)) return null

  const order = selected.orderChoices.find((choice) => choice.value === draft.orderType)
  if (!order) return null
  if (!order.timeInForceChoices.some((choice) => choice.value === draft.timeInForce)) return null
  if (order.requiresLimitLevel && !draft.limitTargetLevel) return null
  if (order.requiresStopLevel && !draft.stopTargetLevel) return null

  return {
    action: "prepareManual",
    targetToken: selected.targetToken,
    side: draft.side,
    orderType: draft.orderType,
    quantityLots: draft.quantityLots,
    ...(order.requiresLimitLevel ? { limitTargetLevel: draft.limitTargetLevel || undefined } : {}),
    ...(order.requiresStopLevel ? { stopTargetLevel: draft.stopTargetLevel || undefined } : {}),
    timeInForce: draft.timeInForce,
  }
}

function emptyDraft(): Draft {
  return {
    targetToken: "",
    side: "",
    orderType: "",
    quantityLots: "",
    limitTargetLevel: "",
    stopTargetLevel: "",
    timeInForce: "",
  }
}

function investmentLabel(investment: ManualPaperTarget["investment"]): string {
  return investment.symbol ? `${investment.name} (${investment.symbol})` : investment.name
}

function formatProductTimestamp(value: string): string {
  const date = new Date(value)
  return Number.isNaN(date.valueOf()) ? "Unavailable" : date.toLocaleString()
}

function Provenance({ provenance }: { provenance: ManualPaperTarget["provenance"] }) {
  return provenance.kind === "generated_proposal" ? (
    <div className="mt-2 text-xs leading-5 text-muted-foreground">
      <p>Saved recommendation: {provenance.recommendation}</p>
      <p>Published {formatProductTimestamp(provenance.publishedAt)}</p>
      <Link className="text-primary underline" to={`/opportunities?analysis=${encodeURIComponent(provenance.analysisActionToken)}`}>
        Review original saved analysis
      </Link>
    </div>
  ) : (
    <div className="mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">
      <p>{provenance.thesis}</p>
      <p>Review by {formatProductTimestamp(provenance.reviewDueAt)}</p>
    </div>
  )
}
