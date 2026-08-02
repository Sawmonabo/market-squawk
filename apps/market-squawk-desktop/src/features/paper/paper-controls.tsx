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
import type { ProviderSession } from "@/lib/schemas"
import type { PaperControlRequest } from "@/lib/transport"

import type { PaperStatus } from "./contracts"

const COINBASE_DIRECT_SURFACE = "coinbase.exchange-direct-market-data"
const MAXIMUM_REASON_LENGTH = 200

export function PaperControlPanel({
  status,
  sessions,
  busy,
  onRequest,
}: {
  status: PaperStatus | undefined
  sessions: ProviderSession[]
  busy: boolean
  onRequest: (request: PaperControlRequest) => void
}) {
  if (!status) {
    return (
      <ControlFrame title="Paper controls">
        <p className="text-sm text-muted-foreground">
          Controls remain unavailable until the installed service returns the current paper lifecycle.
        </p>
      </ControlFrame>
    )
  }
  if (status.state === "stopped") {
    return <StartPaperForm sessions={sessions} busy={busy} onRequest={onRequest} />
  }
  if (status.state === "stopping") {
    return (
      <ControlFrame title="Paper operation is stopping">
        <p className="text-sm text-muted-foreground">
          Market Squawk is retaining authority until shutdown and reconciliation finish.
        </p>
      </ControlFrame>
    )
  }
  return (
    <RunningPaperControls status={status} busy={busy} onRequest={onRequest} />
  )
}

function StartPaperForm({
  sessions,
  busy,
  onRequest,
}: {
  sessions: ProviderSession[]
  busy: boolean
  onRequest: (request: PaperControlRequest) => void
}) {
  const [provider, setProvider] = React.useState<
    "coinbase" | "coinbase-direct" | "kraken"
  >("coinbase")
  const [sessionId, setSessionId] = React.useState("")
  const [initialCash, setInitialCash] = React.useState("100000")
  const [feeBasisPoints, setFeeBasisPoints] = React.useState("5")
  const directSessions = sessions.filter(
    (session) =>
      session.surface_id === COINBASE_DIRECT_SURFACE &&
      session.state === "active_scoped" &&
      session.credential_stored,
  )
  const cashValid = isExactPositiveDecimal(initialCash)
  const fee = parseBasisPoints(feeBasisPoints)
  const directSessionValid = provider !== "coinbase-direct" || Boolean(sessionId)
  const ready = cashValid && fee !== null && directSessionValid && !busy

  const submit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    if (!ready || fee === null) return
    onRequest({
      action: "start",
      provider,
      ...(provider === "coinbase-direct" ? { providerSessionId: sessionId } : {}),
      initialCash,
      feeBasisPoints: fee,
    })
  }

  return (
    <ControlFrame title="Start a paper operation">
      <form className="grid gap-4 lg:grid-cols-2" onSubmit={submit}>
        <Field label="Market-data source" htmlFor="paper-provider">
          <select
            id="paper-provider"
            value={provider}
            onChange={(event) =>
              setProvider(event.target.value as "coinbase" | "coinbase-direct" | "kraken")
            }
            className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <option value="coinbase">Coinbase public market data</option>
            <option value="kraken">Kraken public market data</option>
            <option value="coinbase-direct">Coinbase authorized direct data</option>
          </select>
        </Field>
        {provider === "coinbase-direct" ? (
          <Field label="Authorized Coinbase session" htmlFor="paper-provider-session">
            <select
              id="paper-provider-session"
              value={sessionId}
              onChange={(event) => setSessionId(event.target.value)}
              aria-invalid={!directSessionValid}
              className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <option value="">Select an active session</option>
              {directSessions.map((session) => (
                <option key={session.session_id} value={session.session_id}>
                  {session.session_id}
                </option>
              ))}
            </select>
            {directSessions.length === 0 ? (
              <FieldMessage>
                Complete and activate Coinbase direct setup in Sources before starting this mode.
              </FieldMessage>
            ) : null}
          </Field>
        ) : null}
        <Field label="Starting cash" htmlFor="paper-initial-cash">
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
  busy,
  onRequest,
}: {
  status: Exclude<PaperStatus, { state: "stopped" | "stopping" }>
  busy: boolean
  onRequest: (request: PaperControlRequest) => void
}) {
  const [reason, setReason] = React.useState("")
  const normalizedReason = reason.trim()
  const reasonValid =
    normalizedReason.length >= 3 && normalizedReason.length <= MAXIMUM_REASON_LENGTH
  const running = status.state === "running"

  return (
    <ControlFrame title="Paper operation controls">
      <div className="grid gap-4 lg:grid-cols-[1fr_auto] lg:items-end">
        <Field label="Reason for stop action" htmlFor="paper-stop-reason">
          <Input
            id="paper-stop-reason"
            value={reason}
            onChange={(event) => setReason(event.target.value)}
            maxLength={MAXIMUM_REASON_LENGTH}
            aria-invalid={reason.length > 0 && !reasonValid}
            placeholder="Explain why this paper operation should stop"
          />
          <FieldMessage>
            Required for ordinary and emergency stop; 3–{MAXIMUM_REASON_LENGTH} characters.
          </FieldMessage>
        </Field>
        <div className="flex flex-wrap gap-2">
          {running &&
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
          <Button
            type="button"
            variant="outline"
            disabled={busy || !reasonValid}
            onClick={() => onRequest({ action: "stop", reason: normalizedReason })}
          >
            <Square aria-hidden="true" />
            Stop
          </Button>
          {running ? (
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
            {request ? confirmationDescription(request) : "Review the exact action before continuing."}
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
        <Fact label="Source" value={request.provider} />
        <Fact label="Starting cash" value={request.initialCash} />
        <Fact label="Fee basis points" value={request.feeBasisPoints.toLocaleString()} />
        {request.providerSessionId ? (
          <Fact label="Provider session" value={request.providerSessionId} />
        ) : null}
      </dl>
    )
  }
  if (request.action === "cancel") {
    return <Fact label="Exact order" value={request.orderId} />
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
      return "Start this paper operation?"
    case "stop":
      return "Stop this paper operation?"
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
      return "Market Squawk will start the selected simulated operation. Every resulting intent still requires central risk approval."
    case "stop":
      return "Market Squawk will stop the current paper runtime and retain authority until shutdown reconciliation completes."
    case "cancel":
      return "The dispatcher will evaluate cancellation for this exact tracked order. Existing fills remain recorded."
    case "reconcile":
      return "The dispatcher will reconcile orders, fills, balances, positions, and its source binding."
    case "triggerKillSwitch":
      return "This stops only the current paper operation with the exact reason shown below."
  }
}

function confirmationButton(request: PaperControlRequest) {
  switch (request.action) {
    case "start":
      return "Start paper operation"
    case "stop":
      return "Stop paper operation"
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
      return "The paper operation started."
    case "stop":
      return "The paper operation stopped and returned a shutdown receipt."
    case "cancel":
      return "The cancellation request returned an execution receipt."
    case "reconcile":
      return "Paper execution returned a reconciliation receipt."
    case "triggerKillSwitch":
      return "The paper kill switch stopped the current operation."
  }
}
