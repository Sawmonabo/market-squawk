import * as React from "react"
import { CircleAlert, OctagonX, Play, RefreshCw, Square } from "lucide-react"

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
import type { PaperControlRequest } from "@/lib/transport"

import type { PaperStatus } from "./contracts"

const MAXIMUM_REASON_LENGTH = 200

export interface PaperControlAvailability {
  start: boolean
  stop: boolean
  cancel: boolean
  reconcile: boolean
  killSwitch: boolean
}

export function PaperControlPanel({
  status,
  availability,
  busy,
  onRequest,
}: {
  status: PaperStatus | undefined
  availability: PaperControlAvailability
  busy: boolean
  onRequest: (request: PaperControlRequest) => void
}) {
  if (!status) {
    return (
      <ControlFrame title="Paper controls">
        <p className="text-sm text-muted-foreground">
          Paper controls will be available after the current session finishes loading.
        </p>
      </ControlFrame>
    )
  }
  if (status.state === "stopped") {
    if (!availability.start) {
      return (
        <ControlFrame title="Paper controls">
          <ControlUnavailable />
        </ControlFrame>
      )
    }
    return <StartPaperForm busy={busy} onRequest={onRequest} />
  }
  if (status.state === "stopping") {
    return (
      <ControlFrame title="Paper session is stopping">
        <p className="text-sm text-muted-foreground">
          Market Squawk is completing shutdown and reconciliation.
        </p>
      </ControlFrame>
    )
  }
  return (
    <RunningPaperControls
      status={status}
      availability={availability}
      busy={busy}
      onRequest={onRequest}
    />
  )
}

function StartPaperForm({
  busy,
  onRequest,
}: {
  busy: boolean
  onRequest: (request: PaperControlRequest) => void
}) {
  const [strategyMode, setStrategyMode] = React.useState<"manual" | "book_imbalance">("manual")
  const [initialCash, setInitialCash] = React.useState("100000")
  const [feeBasisPoints, setFeeBasisPoints] = React.useState("5")
  const cashValid = isExactPositiveDecimal(initialCash)
  const fee = parseBasisPoints(feeBasisPoints)
  const ready = cashValid && fee !== null && !busy

  const submit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    if (!ready || fee === null) return
    onRequest({
      action: "start",
      strategyMode,
      initialCash,
      feeBasisPoints: fee,
    })
  }

  return (
    <ControlFrame title="Start a paper session">
      <form className="grid gap-4 lg:grid-cols-2" onSubmit={submit}>
        <div className="rounded-lg border border-border bg-background/35 p-3 text-xs leading-5 text-muted-foreground lg:col-span-2">
          Market Squawk will use the strongest eligible live market data currently configured in
          Connections. If none is ready, the session remains unavailable without starting a
          simulation.
        </div>
        <Field label="Paper mode" htmlFor="paper-strategy-mode">
          <select
            id="paper-strategy-mode"
            value={strategyMode}
            onChange={(event) =>
              setStrategyMode(event.target.value as "manual" | "book_imbalance")
            }
            className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <option value="manual">Controlled manual drafts (recommended)</option>
            <option value="book_imbalance">Automated book-imbalance strategy</option>
          </select>
          <FieldMessage>
            Automated mode is an explicit paper-only market-signal strategy. It does not convert an
            investment recommendation into an order, and every draft still passes the safety checks.
          </FieldMessage>
        </Field>
        <Field label="Virtual starting cash" htmlFor="paper-initial-cash">
          <Input
            id="paper-initial-cash"
            value={initialCash}
            onChange={(event) => setInitialCash(event.target.value)}
            inputMode="decimal"
            autoComplete="off"
            aria-invalid={!cashValid}
            placeholder="100000"
          />
          {!cashValid ? (
            <FieldMessage>Enter a positive decimal without commas or scientific notation.</FieldMessage>
          ) : null}
        </Field>
        <Field label="Fee assumption (basis points)" htmlFor="paper-fee-bps">
          <Input
            id="paper-fee-bps"
            value={feeBasisPoints}
            onChange={(event) => setFeeBasisPoints(event.target.value)}
            inputMode="numeric"
            autoComplete="off"
            aria-invalid={fee === null}
            placeholder="5"
          />
          {fee === null ? (
            <FieldMessage>Enter a whole number from 0 through 65,535.</FieldMessage>
          ) : null}
        </Field>
        <div className="lg:col-span-2">
          <Button type="submit" disabled={!ready}>
            <Play aria-hidden="true" />
            Review and start
          </Button>
        </div>
      </form>
    </ControlFrame>
  )
}

function RunningPaperControls({
  status,
  availability,
  busy,
  onRequest,
}: {
  status: Exclude<PaperStatus, { state: "stopped" | "stopping" }>
  availability: PaperControlAvailability
  busy: boolean
  onRequest: (request: PaperControlRequest) => void
}) {
  const [reason, setReason] = React.useState("")
  const normalizedReason = reason.trim()
  const reasonValid =
    normalizedReason.length >= 3 && normalizedReason.length <= MAXIMUM_REASON_LENGTH
  const running = status.state === "running"
  const actionAvailable =
    availability.stop ||
    (running && (availability.reconcile || availability.killSwitch))

  return (
    <ControlFrame title="Paper session controls">
      {!actionAvailable ? <ControlUnavailable /> : null}
      <div className="grid gap-4 lg:grid-cols-[1fr_auto] lg:items-end">
        <Field label="Reason for stop action" htmlFor="paper-stop-reason">
          <Input
            id="paper-stop-reason"
            value={reason}
            onChange={(event) => setReason(event.target.value)}
            maxLength={MAXIMUM_REASON_LENGTH}
            aria-invalid={reason.length > 0 && !reasonValid}
            placeholder="Explain why this paper session should stop"
          />
          <FieldMessage>
            Required for ordinary and emergency stop; 3–{MAXIMUM_REASON_LENGTH} characters.
          </FieldMessage>
        </Field>
        <div className="flex flex-wrap gap-2">
          {running &&
          availability.reconcile &&
          (status.reconciliationRequired || !status.financialReconciliationCurrent) ? (
            <Button
              type="button"
              variant="outline"
              disabled={busy}
              onClick={() => onRequest({ action: "reconcile" })}
            >
              <RefreshCw aria-hidden="true" />
              Reconcile
            </Button>
          ) : null}
          {availability.stop ? (
            <Button
              type="button"
              variant="outline"
              disabled={busy || !reasonValid}
              onClick={() => onRequest({ action: "stop", reason: normalizedReason })}
            >
              <Square aria-hidden="true" />
              Stop
            </Button>
          ) : null}
          {running && availability.killSwitch ? (
            <Button
              type="button"
              variant="destructive"
              disabled={busy || !reasonValid}
              onClick={() =>
                onRequest({ action: "triggerKillSwitch", reason: normalizedReason })
              }
            >
              <OctagonX aria-hidden="true" />
              Emergency stop
            </Button>
          ) : null}
        </div>
      </div>
    </ControlFrame>
  )
}

export function PaperConfirmationDialog({
  request,
  busy,
  error,
  onClose,
  onConfirm,
}: {
  request: PaperControlRequest | null
  busy: boolean
  error: string | null
  onClose: () => void
  onConfirm: () => void
}) {
  const destructive =
    request?.action === "stop" || request?.action === "triggerKillSwitch"
  return (
    <Dialog open={request !== null} onOpenChange={(open) => !open && !busy && onClose()}>
      <DialogContent showCloseButton={!busy}>
        <DialogHeader>
          <DialogTitle>{request ? confirmationTitle(request) : "Confirm paper action"}</DialogTitle>
          <DialogDescription>
            {request ? confirmationDescription(request) : "Review this action before continuing."}
          </DialogDescription>
        </DialogHeader>
        {request ? <ConfirmationFacts request={request} /> : null}
        {error ? (
          <div className="flex gap-2 rounded-lg border border-rose-400/20 bg-rose-400/5 p-3 text-xs text-rose-100">
            <CircleAlert className="size-4 shrink-0" aria-hidden="true" />
            {error}
          </div>
        ) : null}
        <DialogFooter>
          <Button type="button" variant="ghost" disabled={busy} onClick={onClose}>
            Keep current state
          </Button>
          <Button
            type="button"
            variant={destructive ? "destructive" : "default"}
            disabled={busy || request === null}
            onClick={onConfirm}
          >
            {busy ? "Applying…" : request ? confirmationButton(request) : "Confirm"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function ConfirmationFacts({ request }: { request: PaperControlRequest }) {
  if (request.action === "start") {
    return (
      <dl className="grid gap-3 rounded-lg border border-border bg-card/40 p-4 sm:grid-cols-2">
        <Fact label="Paper mode" value={request.strategyMode} />
        <Fact label="Virtual starting cash" value={request.initialCash} />
        <Fact label="Fee basis points" value={request.feeBasisPoints.toLocaleString()} />
      </dl>
    )
  }
  if (request.action === "cancel") {
    return <Fact label="Virtual order" value={request.orderToken} />
  }
  if (request.action === "stop" || request.action === "triggerKillSwitch") {
    return <Fact label="Reason" value={request.reason} />
  }
  return null
}

function ControlFrame({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="mt-4 rounded-xl border border-border bg-card/45 p-5">
      <h2 className="text-base font-semibold">{title}</h2>
      <div className="mt-4">{children}</div>
    </section>
  )
}

function ControlUnavailable() {
  return (
    <p className="rounded-lg border border-dashed border-border p-3 text-xs leading-5 text-muted-foreground">
      This control is not available in the current setup. Review Connections or Updates &amp; Repair,
      then try again.
    </p>
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
    <div className="space-y-2">
      <Label htmlFor={htmlFor}>{label}</Label>
      {children}
    </div>
  )
}

function FieldMessage({ children }: { children: React.ReactNode }) {
  return <p className="text-[11px] leading-4 text-muted-foreground">{children}</p>
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-words font-mono text-xs">{value}</dd>
    </div>
  )
}

function isExactPositiveDecimal(value: string) {
  if (!/^(?:0|[1-9]\d{0,27})(?:\.\d{1,28})?$/.test(value)) return false
  const digits = value.replace(".", "").replace(/^0+/, "")
  return digits.length > 0 && digits.length <= 28
}

function parseBasisPoints(value: string) {
  if (!/^\d{1,5}$/.test(value)) return null
  const parsed = Number(value)
  return Number.isSafeInteger(parsed) && parsed <= 65_535 ? parsed : null
}

function confirmationTitle(request: PaperControlRequest) {
  switch (request.action) {
    case "start":
      return "Start this paper session?"
    case "stop":
      return "Stop this paper session?"
    case "cancel":
      return "Request cancellation?"
    case "reconcile":
      return "Reconcile paper execution?"
    case "triggerKillSwitch":
      return "Trigger the paper kill switch?"
  }
}

function confirmationDescription(request: PaperControlRequest) {
  switch (request.action) {
    case "start":
      return "Market Squawk will start a virtual session using the best eligible live market data. No brokerage order can be placed, and every virtual order must pass the safety checks."
    case "stop":
      return "Market Squawk will finish shutdown and reconciliation before closing the paper session."
    case "cancel":
      return "Market Squawk will request cancellation for this virtual order. Existing fills remain recorded."
    case "reconcile":
      return "Market Squawk will check virtual orders, fills, balances, positions, and market data."
    case "triggerKillSwitch":
      return "This immediately stops only the current virtual paper session. It cannot instruct a brokerage account."
  }
}

function confirmationButton(request: PaperControlRequest) {
  switch (request.action) {
    case "start":
      return "Start paper session"
    case "stop":
      return "Stop paper session"
    case "cancel":
      return "Request cancellation"
    case "reconcile":
      return "Run reconciliation"
    case "triggerKillSwitch":
      return "Trigger emergency stop"
  }
}

export function paperActionCompleted(request: PaperControlRequest) {
  switch (request.action) {
    case "start":
      return "The paper session started."
    case "stop":
      return "The paper session stopped successfully."
    case "cancel":
      return "The cancellation request was completed."
    case "reconcile":
      return "Paper balances, positions, orders, and fills were reconciled."
    case "triggerKillSwitch":
      return "The paper kill switch stopped the current session."
  }
}
