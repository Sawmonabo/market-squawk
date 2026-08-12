import * as React from "react"
import { useMutation, useQuery } from "@tanstack/react-query"
import { CircleAlert, FileCheck2, Send, ShieldCheck } from "lucide-react"

import { messageFrom } from "@/app/product-context"
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
import { formatMoney, humanize } from "@/lib/formatters"
import { formatTimestamp } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import {
  asManualPaperTransport,
  isPositiveLotQuantity,
  parseAcceptedManualPaperDraft,
  parseGovernedPaperTargets,
  requiresLimitLevel,
  requiresStopLevel,
  validTimeInForce,
  type GovernedPaperTarget,
  type ManualPaperOrderType,
  type ManualPaperSide,
  type ManualPaperSubmit,
  type ManualPaperTimeInForce,
  type TargetLevel,
} from "./manual-paper-contracts"

const CONTROL_CLASS =
  "h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"

type Draft = {
  targetKey: string
  side: ManualPaperSide
  orderType: ManualPaperOrderType
  quantityLots: string
  limitTargetLevel: TargetLevel | ""
  stopTargetLevel: TargetLevel | ""
  timeInForce: ManualPaperTimeInForce
}

export function ManualPaperDraftPanel({
  transport,
  scope,
  enabled,
  busy,
  onAccepted,
}: {
  transport: ProductTransport
  scope: ProductScope
  enabled: boolean
  busy: boolean
  onAccepted: () => Promise<unknown>
}) {
  const manualPaper = asManualPaperTransport(transport)
  const [draft, setDraft] = React.useState<Draft>(emptyDraft)
  const [pending, setPending] = React.useState<ManualPaperSubmit | null>(null)
  const [accepted, setAccepted] = React.useState<string | null>(null)
  const targets = useQuery({
    queryKey: productKeys.operation(scope, "execution", "Execution.GetManualPaperTargets", {}),
    enabled: enabled && manualPaper !== null,
    staleTime: 15_000,
    queryFn: async () => {
      if (!manualPaper) throw new Error("Controlled manual paper operation is unavailable.")
      return parseGovernedPaperTargets(await manualPaper.manualPaper({ action: "targets" }))
    },
  })
  const submit = useMutation({
    mutationFn: async (request: ManualPaperSubmit) => {
      if (!manualPaper) throw new Error("Controlled manual paper operation is unavailable.")
      const result = await manualPaper.manualPaper(request, true)
      parseAcceptedManualPaperDraft(result, request)
    },
    onSuccess: async (_value, request) => {
      setPending(null)
      setAccepted(
        `Paper draft for ${request.targetId} revision ${request.targetRevision} is waiting for the next qualified market event.`,
      )
      setDraft((current) => ({ ...current, quantityLots: "" }))
      await onAccepted()
    },
  })

  React.useEffect(() => {
    const targetCatalog = targets.data
    const first = targetCatalog?.[0]
    if (!targetCatalog || !first) return
    setDraft((current) =>
      current.targetKey &&
      targetCatalog.some((target) => targetKey(target) === current.targetKey)
        ? current
        : { ...current, targetKey: targetKey(first) },
    )
  }, [targets.data])

  const selected = targets.data?.find((target) => targetKey(target) === draft.targetKey) ?? null
  const normalized = normalizeDraft(draft, selected)
  const invalidTarget = draft.targetKey.length > 0 && selected === null
  const ready = enabled && normalized !== null && !busy && !submit.isPending

  return (
    <section className="mt-4 rounded-xl border border-primary/25 bg-primary/5 p-5" aria-labelledby="manual-paper-heading">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Controlled manual paper operation
          </p>
          <h2 id="manual-paper-heading" className="mt-1 text-lg font-semibold">
            Express a governed target as a paper draft
          </h2>
          <p className="mt-1 max-w-3xl text-sm leading-6 text-muted-foreground">
            Choose an active target, your side, order constraint, and whole-lot quantity. The next
            qualified committed event from the running live-market source supplies execution terms.
            The target is not automatic order authority: central pre-trade risk still evaluates
            every resulting intent before virtual paper dispatch.
          </p>
        </div>
        <ShieldCheck className="size-5 text-primary" aria-hidden="true" />
      </div>

      {!enabled || manualPaper === null ? (
        <Unavailable />
      ) : targets.isPending ? (
        <Status text="Loading currently governed targets and their approved price ladders…" />
      ) : targets.isError ? (
        <Status text={messageFrom(targets.error)} tone="error" />
      ) : targets.data?.length === 0 ? (
        <Unavailable detail="Create and activate a governed investment target before preparing a paper draft." />
      ) : selected ? (
        <div className="mt-5 space-y-4">
          <TargetEvidence target={selected} />
          <div className="grid gap-4 lg:grid-cols-2">
            <Field label="Governed target" htmlFor="manual-paper-target">
              <select
                id="manual-paper-target"
                className={CONTROL_CLASS}
                value={draft.targetKey}
                onChange={(event) => {
                  const nextTarget = targets.data?.find(
                    (target) => targetKey(target) === event.target.value,
                  )
                  setDraft((current) => ({
                    ...current,
                    targetKey: event.target.value,
                    limitTargetLevel: "",
                    stopTargetLevel: "",
                    timeInForce: nextTarget
                      ? defaultTimeInForce(current.orderType)
                      : current.timeInForce,
                  }))
                  setAccepted(null)
                }}
              >
                {targets.data.map((target) => (
                  <option key={targetKey(target)} value={targetKey(target)}>
                    {target.instrumentId} · revision {target.targetRevision}
                  </option>
                ))}
              </select>
              <FieldMessage>
                Only current active target revisions returned by the installed service appear here.
              </FieldMessage>
            </Field>
            <Field label="Direction" htmlFor="manual-paper-side">
              <select
                id="manual-paper-side"
                className={CONTROL_CLASS}
                value={draft.side}
                onChange={(event) => {
                  setDraft((current) => ({
                    ...current,
                    side: event.target.value as ManualPaperSide,
                  }))
                  setAccepted(null)
                }}
              >
                <option value="buy">Buy</option>
                <option value="sell">Sell</option>
              </select>
            </Field>
            <Field label="Order constraint" htmlFor="manual-paper-order-type">
              <select
                id="manual-paper-order-type"
                className={CONTROL_CLASS}
                value={draft.orderType}
                onChange={(event) => {
                  const orderType = event.target.value as ManualPaperOrderType
                  setDraft((current) => ({
                    ...current,
                    orderType,
                    limitTargetLevel: requiresLimitLevel(orderType)
                      ? current.limitTargetLevel
                      : "",
                    stopTargetLevel: requiresStopLevel(orderType)
                      ? current.stopTargetLevel
                      : "",
                    timeInForce: defaultTimeInForce(orderType),
                  }))
                  setAccepted(null)
                }}
              >
                <option value="market">Market</option>
                <option value="limit">Limit at governed level</option>
                <option value="stop">Stop at governed level</option>
                <option value="stop_limit">Stop-limit at governed levels</option>
              </select>
              <FieldMessage>
                This does not supply current market price or market-data quality from the dashboard.
              </FieldMessage>
            </Field>
            <Field label="Whole-lot quantity" htmlFor="manual-paper-quantity">
              <Input
                id="manual-paper-quantity"
                value={draft.quantityLots}
                inputMode="numeric"
                autoComplete="off"
                placeholder="100"
                aria-invalid={draft.quantityLots.length > 0 && !isPositiveLotQuantity(draft.quantityLots)}
                onChange={(event) => {
                  setDraft((current) => ({ ...current, quantityLots: event.target.value }))
                  setAccepted(null)
                }}
              />
              <FieldMessage>
                Enter a positive whole number of lots; fractions, separators, and scientific notation are not accepted.
              </FieldMessage>
            </Field>
            {requiresLimitLevel(draft.orderType) ? (
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
            {requiresStopLevel(draft.orderType) ? (
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
            <Field label="Time in force" htmlFor="manual-paper-time-in-force">
              <select
                id="manual-paper-time-in-force"
                className={CONTROL_CLASS}
                value={draft.timeInForce}
                onChange={(event) => {
                  setDraft((current) => ({
                    ...current,
                    timeInForce: event.target.value as ManualPaperTimeInForce,
                  }))
                  setAccepted(null)
                }}
              >
                {validTimeInForce(draft.orderType).map((value) => (
                  <option key={value} value={value}>{humanize(value)}</option>
                ))}
              </select>
            </Field>
          </div>

          {invalidTarget ? <Status text="The selected target is no longer an active governed target. Refresh before submitting." tone="error" /> : null}
          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-background/25 p-3">
            <p className="max-w-3xl text-[11px] leading-5 text-muted-foreground">
              The service owns the route, account, order identity, target content digest, reason,
              timing, maximum slippage, and risk/dispatch authority. A confirmed draft does not
              approve or guarantee an order.
            </p>
            <Button
              disabled={!ready}
              onClick={() => {
                if (normalized) {
                  submit.reset()
                  setPending(normalized)
                }
              }}
            >
              <FileCheck2 aria-hidden="true" />
              Review paper draft
            </Button>
          </div>
          {accepted ? <Status text={accepted} tone="success" /> : null}
        </div>
      ) : null}

      <Dialog
        open={pending !== null}
        onOpenChange={(open) => {
          if (!open && !submit.isPending) setPending(null)
        }}
      >
        <DialogContent className="max-h-[88vh] overflow-y-auto sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Submit this controlled paper draft?</DialogTitle>
            <DialogDescription>
              Review the exact target-backed request. Submission occupies one bounded manual draft
              slot and waits for the next qualified committed event from the running live-market
              source; it still must pass central pre-trade risk before virtual paper dispatch.
            </DialogDescription>
          </DialogHeader>
          {pending && selected ? <ConfirmationEvidence request={pending} target={selected} /> : null}
          {submit.isError ? <Status text={messageFrom(submit.error)} tone="error" /> : null}
          <DialogFooter>
            <Button variant="outline" disabled={submit.isPending} onClick={() => setPending(null)}>
              Keep editing
            </Button>
            <Button
              disabled={!pending || submit.isPending}
              onClick={() => {
                if (pending) submit.mutate(pending)
              }}
            >
              <Send aria-hidden="true" />
              {submit.isPending ? "Submitting draft…" : "Confirm paper draft"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

function TargetEvidence({ target }: { target: GovernedPaperTarget }) {
  return (
    <div className="rounded-xl border border-border bg-background/35 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
            Active governed target · revision {target.targetRevision}
          </p>
          <p className="mt-1 font-mono text-sm">{target.targetId} · {target.instrumentId}</p>
          <p className="mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">{target.thesis}</p>
        </div>
        <p className="text-right text-[10px] text-muted-foreground">
          {target.route.venueId}<br />
          Review due {formatTimestamp(target.reviewDueAt)}
        </p>
      </div>
      <dl className="mt-4 grid gap-2 sm:grid-cols-2 xl:grid-cols-5">
        {target.ladder.map((level) => (
          <div key={level.level} className="rounded-lg border border-border/70 bg-card/40 p-3">
            <dt className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground">{level.label}</dt>
            <dd className="mt-1 text-xs font-semibold tabular-nums">{formatMoney(level.value)}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}

function ConfirmationEvidence({
  request,
  target,
}: {
  request: ManualPaperSubmit
  target: GovernedPaperTarget
}) {
  const limit = request.limitTargetLevel
    ? target.ladder.find((level) => level.level === request.limitTargetLevel) ?? null
    : null
  const stop = request.stopTargetLevel
    ? target.ladder.find((level) => level.level === request.stopTargetLevel) ?? null
    : null
  return (
    <dl className="grid gap-3 rounded-xl border border-border bg-card/35 p-4 text-xs sm:grid-cols-2">
      <Fact label="Governed target" value={`${request.targetId} · revision ${request.targetRevision}`} />
      <Fact label="Instrument / route" value={`${target.instrumentId} · ${target.route.venueId}`} />
      <Fact label="Direction" value={humanize(request.side)} />
      <Fact label="Order constraint" value={humanize(request.orderType)} />
      <Fact label="Whole-lot quantity" value={`${request.quantityLots} lots`} />
      <Fact label="Time in force" value={humanize(request.timeInForce)} />
      {limit ? <Fact label="Limit target level" value={`${limit.label} · ${formatMoney(limit.value)}`} /> : null}
      {stop ? <Fact label="Stop target level" value={`${stop.label} · ${formatMoney(stop.value)}`} /> : null}
      <div className="sm:col-span-2 rounded-lg border border-amber-400/20 bg-amber-400/5 p-3 text-[11px] leading-5 text-amber-100">
        No live market price, market-data quality, event time, account, order identity, target digest,
        risk approval, or dispatch authority is supplied by this screen. The service resolves and
        records those facts only when a qualified event arrives.
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
  ladder: GovernedPaperTarget["ladder"]
  onChange: (value: TargetLevel | "") => void
}) {
  return (
    <Field label={label} htmlFor={fieldId}>
      <select
        id={fieldId}
        className={CONTROL_CLASS}
        value={value}
        aria-invalid={value.length === 0}
        onChange={(event) => onChange(event.target.value as TargetLevel | "")}
      >
        <option value="">Select a governed target level</option>
        {ladder.map((level) => (
          <option key={level.level} value={level.level}>
            {level.label} · {formatMoney(level.value)}
          </option>
        ))}
      </select>
      <FieldMessage>Only a named level from this exact target revision can be submitted.</FieldMessage>
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
  detail = "This service generation does not expose the closed manual-paper target and submission operations together.",
}: {
  detail?: string
}) {
  return (
    <Alert className="mt-4">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>Controlled paper drafting is unavailable</AlertTitle>
      <AlertDescription>{detail}</AlertDescription>
    </Alert>
  )
}

function Status({ text, tone = "neutral" }: { text: string; tone?: "neutral" | "error" | "success" }) {
  const classes =
    tone === "error"
      ? "border-rose-400/25 bg-rose-400/5 text-rose-100"
      : tone === "success"
        ? "border-emerald-400/25 bg-emerald-400/5 text-emerald-100"
        : "border-border bg-background/30 text-muted-foreground"
  return <p className={`mt-4 rounded-lg border p-3 text-xs leading-5 ${classes}`}>{text}</p>
}

function normalizeDraft(draft: Draft, selected: GovernedPaperTarget | null): ManualPaperSubmit | null {
  if (
    selected === null ||
    !isPositiveLotQuantity(draft.quantityLots) ||
    !validTimeInForce(draft.orderType).includes(draft.timeInForce)
  ) {
    return null
  }
  const limitTargetLevel = requiresLimitLevel(draft.orderType) ? draft.limitTargetLevel : undefined
  const stopTargetLevel = requiresStopLevel(draft.orderType) ? draft.stopTargetLevel : undefined
  if (
    (requiresLimitLevel(draft.orderType) && !limitTargetLevel) ||
    (requiresStopLevel(draft.orderType) && !stopTargetLevel)
  ) {
    return null
  }
  return {
    action: "submit",
    targetId: selected.targetId,
    targetRevision: selected.targetRevision,
    side: draft.side,
    orderType: draft.orderType,
    quantityLots: draft.quantityLots,
    ...(limitTargetLevel ? { limitTargetLevel } : {}),
    ...(stopTargetLevel ? { stopTargetLevel } : {}),
    timeInForce: draft.timeInForce,
  }
}

function emptyDraft(): Draft {
  return {
    targetKey: "",
    side: "buy",
    orderType: "market",
    quantityLots: "",
    limitTargetLevel: "",
    stopTargetLevel: "",
    timeInForce: "immediate_or_cancel",
  }
}

function defaultTimeInForce(orderType: ManualPaperOrderType): ManualPaperTimeInForce {
  switch (orderType) {
    case "market":
      return "immediate_or_cancel"
    case "limit":
    case "stop":
    case "stop_limit":
      return "day"
  }
}

function targetKey(target: GovernedPaperTarget): string {
  return `${target.targetId}:${target.targetRevision}`
}
