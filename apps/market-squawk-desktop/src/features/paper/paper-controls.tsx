import * as React from "react"
import { useMutation, useQuery } from "@tanstack/react-query"
import { CircleAlert, OctagonX, Square } from "lucide-react"

import { productKeys, type ProductScope } from "@/app/query-client"
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

import {
  parsePaperStartPreparation,
  parsePaperStartPreview,
  parsePaperStartResult,
  type PaperControlIntent,
  type PaperStartPreview,
  type PaperStatus,
} from "./contracts"

const MAXIMUM_REASON_LENGTH = 200

export interface PaperControlAvailability {
  stop: boolean
  cancel: boolean
  killSwitch: boolean
}

export function PaperControlPanel({
  status,
  availability,
  busy,
  onRequest,
  transport,
  scope,
  startAvailable,
  onStarted,
}: {
  status: PaperStatus | undefined
  availability: PaperControlAvailability
  busy: boolean
  onRequest: (request: PaperControlIntent) => void
  transport: ProductTransport
  scope: ProductScope
  startAvailable: boolean
  onStarted: (message: string) => Promise<unknown>
}) {
  if (!status) {
    return (
      <ControlFrame title="Paper session controls">
        <p className="text-sm text-muted-foreground">
          Controls will appear after the current paper status loads.
        </p>
      </ControlFrame>
    )
  }
  if (status.sessionAvailability === "ready") {
    return (
      <StartPaperControls
        transport={transport}
        scope={scope}
        enabled={startAvailable}
        onStarted={onStarted}
      />
    )
  }
  if (status.sessionAvailability === "unavailable") {
    return (
      <ControlFrame title="Paper session controls">
        <p className="text-sm text-muted-foreground">
          {status.safeguards === "action_needed"
            ? "Paper practice needs attention. Review Logs & Diagnostics."
            : "Paper practice is temporarily unavailable. Try again shortly."}
        </p>
      </ControlFrame>
    )
  }
  return (
    <RunningPaperControls
      availability={availability}
      busy={busy}
      onRequest={onRequest}
    />
  )
}

function StartPaperControls({
  transport,
  scope,
  enabled,
  onStarted,
}: {
  transport: ProductTransport
  scope: ProductScope
  enabled: boolean
  onStarted: (message: string) => Promise<unknown>
}) {
  const [cashChoice, setCashChoice] = React.useState("")
  const [costChoice, setCostChoice] = React.useState("")
  const [modeChoice, setModeChoice] = React.useState("")
  const [preview, setPreview] = React.useState<PaperStartPreview | null>(null)
  const [startError, setStartError] = React.useState(false)
  const options = useQuery({
    queryKey: productKeys.operation(scope, "bot", "Bot.GetStartPreparation", {}),
    enabled,
    queryFn: async () =>
      parsePaperStartPreparation(await transport.paperControl({ action: "startPreparation" })),
  })
  const prepare = useMutation({
    mutationFn: async () =>
      parsePaperStartPreview(
        await transport.paperControl({
          action: "prepareStart",
          cashChoice,
          costChoice,
          modeChoice,
        }),
      ),
    onSuccess: (value) => {
      setStartError(false)
      setPreview(value)
    },
  })
  const start = useMutation({
    mutationFn: async (confirmationToken: string) =>
      parsePaperStartResult(
        await transport.paperControl({ action: "start", confirmationToken }, true),
      ),
    onSuccess: async (message) => {
      setPreview(null)
      setStartError(false)
      await onStarted(message)
    },
    onError: () => {
      setPreview(null)
      setStartError(true)
    },
  })
  const choicesReady = cashChoice !== "" && costChoice !== "" && modeChoice !== ""

  return (
    <ControlFrame title="Start paper practice">
      {!enabled ? (
        <ControlUnavailable />
      ) : options.isLoading ? (
        <p className="text-sm text-muted-foreground">Loading paper-session choices…</p>
      ) : options.isError || !options.data ? (
        <p className="text-sm text-muted-foreground">
          Paper-session choices are unavailable. Try again shortly.
        </p>
      ) : (
        <>
          <p className="mb-4 text-sm leading-6 text-muted-foreground">
            Choose the virtual cash, estimated trading cost, and practice mode. Nothing is selected
            automatically, and starting a session does not place a virtual or brokerage order.
          </p>
          <div className="grid gap-4 lg:grid-cols-3">
            <PreparedSelect
              id="paper-cash-choice"
              label="Virtual cash"
              value={cashChoice}
              onChange={setCashChoice}
              choices={options.data.virtualCashChoices.map((choice) => ({
                token: choice.choiceToken,
                label: `${choice.label} · ${formatMoney(choice.amount)}`,
              }))}
            />
            <PreparedSelect
              id="paper-cost-choice"
              label="Estimated trading cost"
              value={costChoice}
              onChange={setCostChoice}
              choices={options.data.costChoices.map((choice) => ({
                token: choice.choiceToken,
                label: `${choice.label} · ${choice.estimatedTradingCost}`,
              }))}
            />
            <PreparedSelect
              id="paper-mode-choice"
              label="Practice mode"
              value={modeChoice}
              onChange={setModeChoice}
              choices={options.data.modeChoices.map((choice) => ({
                token: choice.choiceToken,
                label: choice.label,
              }))}
            />
          </div>
          <div className="mt-4 flex justify-end">
            <Button
              disabled={!choicesReady || prepare.isPending}
              onClick={() => prepare.mutate()}
            >
              {prepare.isPending ? "Preparing…" : "Review session"}
            </Button>
          </div>
          {prepare.isError ? (
            <p className="mt-3 text-xs text-rose-200">
              The session preview is unavailable. Review the choices and try again.
            </p>
          ) : null}
          {startError ? (
            <p className="mt-3 text-xs text-rose-200">
              That prepared confirmation is no longer usable. Review the session again before retrying.
            </p>
          ) : null}
        </>
      )}
      <Dialog open={preview !== null} onOpenChange={(open) => !open && !start.isPending && setPreview(null)}>
        <DialogContent showCloseButton={!start.isPending}>
          <DialogHeader>
            <DialogTitle>Start this paper session?</DialogTitle>
            <DialogDescription>
              Confirm the prepared virtual cash, trading-cost estimate, mode, and safeguards.
            </DialogDescription>
          </DialogHeader>
          {preview ? (
            <dl className="grid gap-3 sm:grid-cols-2">
              <Fact label="Virtual cash" value={formatMoney(preview.virtualCash)} />
              <Fact label="Estimated trading cost" value={preview.estimatedTradingCost} />
              <Fact label="Practice mode" value={preview.modeLabel} />
              <Fact label="Preview expires" value={new Date(preview.expiresAt).toLocaleString()} />
              <div className="sm:col-span-2">
                {preview.safeguards.map((safeguard) => (
                  <p key={safeguard} className="mt-1 text-xs text-muted-foreground">{safeguard}</p>
                ))}
              </div>
            </dl>
          ) : null}
          {start.isError ? <p className="text-xs text-rose-200">The session could not be started. Prepare it again and retry.</p> : null}
          <DialogFooter>
            <Button variant="ghost" disabled={start.isPending} onClick={() => setPreview(null)}>
              Keep current state
            </Button>
            <Button
              disabled={!preview || start.isPending}
              onClick={() => preview && start.mutate(preview.confirmationToken)}
            >
              {start.isPending ? "Starting…" : "Start paper practice"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </ControlFrame>
  )
}

function PreparedSelect({
  id,
  label,
  value,
  onChange,
  choices,
}: {
  id: string
  label: string
  value: string
  onChange: (value: string) => void
  choices: { token: string; label: string }[]
}) {
  return (
    <Field label={label} htmlFor={id}>
      <select
        id={id}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="h-10 w-full rounded-md border border-input bg-background px-3 text-sm"
      >
        <option value="">Select {label.toLowerCase()}</option>
        {choices.map((choice) => (
          <option key={choice.token} value={choice.token}>{choice.label}</option>
        ))}
      </select>
    </Field>
  )
}

function RunningPaperControls({
  availability,
  busy,
  onRequest,
}: {
  availability: PaperControlAvailability
  busy: boolean
  onRequest: (request: PaperControlIntent) => void
}) {
  const [reason, setReason] = React.useState("")
  const normalizedReason = reason.trim()
  const reasonValid =
    normalizedReason.length >= 3 && normalizedReason.length <= MAXIMUM_REASON_LENGTH

  return (
    <ControlFrame title="Paper session controls">
      {!availability.stop && !availability.killSwitch ? (
        <ControlUnavailable />
      ) : null}
      <div className="grid gap-4 lg:grid-cols-[1fr_auto] lg:items-end">
        <Field label="Reason for stopping" htmlFor="paper-stop-reason">
          <Input
            id="paper-stop-reason"
            value={reason}
            onChange={(event) => setReason(event.target.value)}
            maxLength={MAXIMUM_REASON_LENGTH}
            aria-invalid={reason.length > 0 && !reasonValid}
            placeholder="Why should this paper session stop?"
          />
          <FieldMessage>
            Required for an ordinary or emergency stop; 3–{MAXIMUM_REASON_LENGTH} characters.
          </FieldMessage>
        </Field>
        <div className="flex flex-wrap gap-2">
          {availability.stop ? (
            <Button
              type="button"
              variant="outline"
              disabled={busy || !reasonValid}
              onClick={() => onRequest({ action: "stop", reason: normalizedReason })}
            >
              <Square aria-hidden="true" />
              Stop session
            </Button>
          ) : null}
          {availability.killSwitch ? (
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
  request: PaperControlIntent | null
  busy: boolean
  error: string | null
  onClose: () => void
  onConfirm: () => void
}) {
  const destructive = request?.action === "stop" || request?.action === "triggerKillSwitch"
  return (
    <Dialog open={request !== null} onOpenChange={(open) => !open && !busy && onClose()}>
      <DialogContent showCloseButton={!busy}>
        <DialogHeader>
          <DialogTitle>{request ? confirmationTitle(request) : "Confirm paper action"}</DialogTitle>
          <DialogDescription>
            {request ? confirmationDescription(request) : "Review this action before continuing."}
          </DialogDescription>
        </DialogHeader>
        {request?.action === "stop" || request?.action === "triggerKillSwitch" ? (
          <Fact label="Reason" value={request.reason} />
        ) : null}
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
    <p className="mb-4 rounded-lg border border-dashed border-border p-3 text-xs leading-5 text-muted-foreground">
      Session controls are unavailable. Review Connections or Updates &amp; Repair, then try again.
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
    <dl className="rounded-lg border border-border bg-card/40 p-4">
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 text-xs">{value}</dd>
    </dl>
  )
}

function confirmationTitle(request: PaperControlIntent) {
  switch (request.action) {
    case "stop":
      return "Stop this paper session?"
    case "cancel":
      return "Cancel this virtual order?"
    case "triggerKillSwitch":
      return "Use the emergency stop?"
  }
}

function confirmationDescription(request: PaperControlIntent) {
  switch (request.action) {
    case "stop":
      return "Market Squawk will stop the session after checking its virtual balances and orders."
    case "cancel":
      return "Market Squawk will request cancellation. Any virtual fills already completed remain recorded."
    case "triggerKillSwitch":
      return "This immediately stops only the current virtual paper session. It cannot instruct a brokerage account."
  }
}

function confirmationButton(request: PaperControlIntent) {
  switch (request.action) {
    case "stop":
      return "Stop paper session"
    case "cancel":
      return "Request cancellation"
    case "triggerKillSwitch":
      return "Use emergency stop"
  }
}

export function paperActionCompleted(request: PaperControlIntent) {
  switch (request.action) {
    case "stop":
      return "The paper session stopped."
    case "cancel":
      return "The cancellation request was completed."
    case "triggerKillSwitch":
      return "The emergency stop ended the current paper session."
  }
}
